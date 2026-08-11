//! Executor-facing provider contexts for bound planned predicates and scan I/O.

use core::marker::PhantomData;

use pgrx::pg_sys;

use crate::batch::ScanBatchDriver;
use crate::customscan::error::CustomScanError;
use crate::customscan::plan_data::tuple_layout::{
    NeededColumns, ScanTupleDescriptor, ScanTupleLayout,
};
use crate::expr::pushdown::BoundFilterSet;
use crate::handles::{RelationHandle, ScanDirection, SnapshotHandle};
use crate::tuple::{Row, RowDatumCodec, SlotColumns, TupleSlotWriter};

use super::contract::LakebaseCustomScanProvider;
use super::planning::ScanPurpose;

/// Context for [`LakebaseCustomScanProvider::create_state`].
pub struct CreateStateContext<P: LakebaseCustomScanProvider + ?Sized> {
    _marker: PhantomData<fn() -> P>,
}

impl<P: LakebaseCustomScanProvider + ?Sized> CreateStateContext<P> {
    /// Construct a context with every field explicitly initialized.
    pub(crate) fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

/// Context for [`LakebaseCustomScanProvider::begin`].
pub struct BeginContext<'a, P: LakebaseCustomScanProvider + ?Sized> {
    /// Provider's per-scan runtime state.
    pub state: &'a mut P::State,
    /// Decoded provider `PrivateData` for this scan.
    pub private_data: &'a P::PrivateData,
    /// Query or modification-target purpose selected by the planner.
    pub purpose: ScanPurpose,
    /// Provider predicates bound for the current executor values.
    pub filters: BoundFilterSet<'a, P::BoundPredicate>,
    scan_tuple_desc: pg_sys::TupleDesc,
    tuple_layout: &'a ScanTupleLayout,
    /// Scan relation handle.
    pub relation: RelationHandle<'a>,
    /// Executor snapshot handle.
    pub snapshot: SnapshotHandle<'a>,
    _marker: PhantomData<&'a ()>,
}

impl<'a, P: LakebaseCustomScanProvider> BeginContext<'a, P> {
    pub(crate) fn new(
        state: &'a mut P::State,
        private_data: &'a P::PrivateData,
        purpose: ScanPurpose,
        filters: BoundFilterSet<'a, P::BoundPredicate>,
        scan_tuple_desc: pg_sys::TupleDesc,
        tuple_layout: &'a ScanTupleLayout,
        relation: RelationHandle<'a>,
        snapshot: SnapshotHandle<'a>,
    ) -> Self {
        Self {
            state,
            private_data,
            purpose,
            filters,
            scan_tuple_desc,
            tuple_layout,
            relation,
            snapshot,
            _marker: PhantomData,
        }
    }

    /// Actual executor descriptor for the provider-filled raw scan slot.
    #[inline]
    pub fn scan_tuple(&self) -> ScanTupleDescriptor<'_> {
        unsafe { ScanTupleDescriptor::new(self.scan_tuple_desc, self.tuple_layout) }
    }

    #[inline]
    pub fn required_columns(&self) -> NeededColumns<'_> {
        self.tuple_layout.required_columns()
    }

    /// Bind the semantic row conversion plan for this relation.
    pub fn row_datum_codec(&self) -> Result<RowDatumCodec, CustomScanError> {
        unsafe { RowDatumCodec::from_relation(self.relation.as_raw()) }
            .map_err(CustomScanError::provider)
    }
}

/// Context for [`LakebaseCustomScanProvider::next_slot`].
pub struct NextSlotContext<'a, P: LakebaseCustomScanProvider + ?Sized> {
    /// Provider's per-scan runtime state.
    pub state: &'a mut P::State,
    /// Scan relation handle.
    pub relation: RelationHandle<'a>,
    slot: *mut pg_sys::TupleTableSlot,
    scan_direction: ScanDirection,
    /// Memory context reset by ExecScan for each tuple cycle.
    per_tuple_memory_context: pg_sys::MemoryContext,
    _marker: PhantomData<&'a ()>,
}

impl<'a, P: LakebaseCustomScanProvider> NextSlotContext<'a, P> {
    pub(crate) fn new(
        state: &'a mut P::State,
        relation: RelationHandle<'a>,
        slot: *mut pg_sys::TupleTableSlot,
        scan_direction: ScanDirection,
        per_tuple_memory_context: pg_sys::MemoryContext,
    ) -> Self {
        Self {
            state,
            relation,
            slot,
            scan_direction,
            per_tuple_memory_context,
            _marker: PhantomData,
        }
    }

    /// PostgreSQL's current executor scan direction.
    #[inline]
    pub fn scan_direction(&self) -> ScanDirection {
        self.scan_direction
    }

    /// Write a row into the scan slot.
    ///
    /// # Safety
    ///
    /// `codec` must be bound to the same relation tuple descriptor as this
    /// scan context's slot.
    pub unsafe fn emit_row(
        &mut self,
        row: &mut Row,
        codec: &RowDatumCodec,
    ) -> Result<(), CustomScanError> {
        let writer = unsafe {
            TupleSlotWriter::new(self.slot, self.per_tuple_memory_context, codec)
        };
        unsafe { writer.write_row(row) }.map_err(CustomScanError::from)
    }

    /// Drive a slot-first scan driver into the scan slot.
    pub fn emit_columns<D: ScanBatchDriver>(
        &mut self,
        driver: &mut D,
    ) -> Result<bool, CustomScanError> {
        let direction = self.scan_direction;
        self.emit_with(|columns| driver.next_into_slot(direction, columns))
    }

    /// Fill the raw scan slot through one cohesive slot-first operation.
    pub fn emit_with<F>(&mut self, advance: F) -> Result<bool, CustomScanError>
    where
        F: FnOnce(&mut SlotColumns<'_>) -> crate::api::AmResult<bool>,
    {
        let slot = self.slot;
        let datum_context = self.per_tuple_memory_context;
        emit_into_slot(
            || {
                let mut columns = unsafe { SlotColumns::new(slot, datum_context) };
                advance(&mut columns)
            },
            || unsafe {
                pg_sys::ExecStoreVirtualTuple(slot);
            },
        )
    }
}

/// Shared produced-row/end-of-scan protocol for slot-first providers.
pub(crate) fn emit_into_slot<A, S>(
    advance: A,
    store: S,
) -> Result<bool, CustomScanError>
where
    A: FnOnce() -> crate::api::AmResult<bool>,
    S: FnOnce(),
{
    let found = advance().map_err(CustomScanError::from)?;
    if found {
        store();
    }
    Ok(found)
}

/// Context for [`LakebaseCustomScanProvider::rescan`].
pub struct ReScanContext<'a, P: LakebaseCustomScanProvider + ?Sized> {
    /// Provider's per-scan runtime state.
    pub state: &'a mut P::State,
    /// Whether parameter-dependent filter predicates were rebound.
    pub filters_changed: bool,
    /// Query or modification-target purpose selected by the planner.
    pub purpose: ScanPurpose,
    /// Complete provider predicate set rebound for the current values.
    pub filters: BoundFilterSet<'a, P::BoundPredicate>,
    /// Scan relation handle.
    pub relation: RelationHandle<'a>,
    /// Executor snapshot handle.
    pub snapshot: SnapshotHandle<'a>,
    _marker: PhantomData<&'a ()>,
}

impl<'a, P: LakebaseCustomScanProvider> ReScanContext<'a, P> {
    pub(crate) fn new(
        state: &'a mut P::State,
        filters_changed: bool,
        purpose: ScanPurpose,
        filters: BoundFilterSet<'a, P::BoundPredicate>,
        relation: RelationHandle<'a>,
        snapshot: SnapshotHandle<'a>,
    ) -> Self {
        Self {
            state,
            filters_changed,
            purpose,
            filters,
            relation,
            snapshot,
            _marker: PhantomData,
        }
    }
}

/// Context for [`LakebaseCustomScanProvider::end`].
pub struct EndContext<'a, P: LakebaseCustomScanProvider + ?Sized> {
    pub state: &'a mut P::State,
    pub relation: RelationHandle<'a>,
    _marker: PhantomData<&'a ()>,
}

impl<'a, P: LakebaseCustomScanProvider> EndContext<'a, P> {
    pub(crate) fn new(state: &'a mut P::State, relation: RelationHandle<'a>) -> Self {
        Self {
            state,
            relation,
            _marker: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn emit_into_slot_stores_only_produced_rows() {
        let mut stores = 0;
        assert!(
            emit_into_slot(
                || Ok::<_, crate::diag::PgReportError>(true),
                || {
                    stores += 1;
                }
            )
            .unwrap()
        );
        assert_eq!(stores, 1);

        assert!(
            !emit_into_slot(
                || Ok::<_, crate::diag::PgReportError>(false),
                || {
                    stores += 1;
                }
            )
            .unwrap()
        );
        assert_eq!(stores, 1);
    }
}
