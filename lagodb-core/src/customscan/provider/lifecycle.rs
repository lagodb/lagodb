//! Provider lifecycle contexts, separate from row production.

use core::marker::PhantomData;

use pgrx::pg_sys;

use crate::customscan::error::CustomScanError;
use crate::customscan::plan_data::tuple_layout::{
    NeededColumns, ScanTupleDescriptor, ScanTupleLayout,
};
use crate::expr::pushdown::BoundFilterSet;
use crate::handles::{RelationHandle, SnapshotHandle};
use crate::tuple::RowDatumCodec;

use super::contract::LagodbCustomScanProvider;
use super::planning::ScanPurpose;

/// Context for [`LagodbCustomScanProvider::create_state`].
pub struct CreateStateContext<P: LagodbCustomScanProvider + ?Sized> {
    _marker: PhantomData<fn() -> P>,
}

impl<P: LagodbCustomScanProvider + ?Sized> CreateStateContext<P> {
    /// Construct a context with every field explicitly initialized.
    pub(crate) fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

/// Context for [`LagodbCustomScanProvider::begin`].
pub struct BeginContext<'a, P: LagodbCustomScanProvider + ?Sized> {
    /// Provider's per-scan runtime state.
    pub state: &'a mut P::State,
    /// Decoded provider `PrivateData` for this scan.
    pub private_data: &'a P::PrivateData,
    /// Query or modification-target purpose selected by the planner.
    pub purpose: ScanPurpose,
    /// Statement-stable predicates; dynamic inputs are bound at Start.
    pub filters: BoundFilterSet<'a, P::BoundPredicate>,
    scan_tuple_desc: pg_sys::TupleDesc,
    tuple_layout: &'a ScanTupleLayout,
    /// Scan relation handle.
    pub relation: RelationHandle<'a>,
    /// Executor snapshot handle.
    pub snapshot: SnapshotHandle<'a>,
    /// True only for the partial path executed under PostgreSQL Gather.
    pub parallel_aware: bool,
    _marker: PhantomData<&'a ()>,
}

impl<'a, P: LagodbCustomScanProvider> BeginContext<'a, P> {
    pub(crate) fn new(
        state: &'a mut P::State,
        private_data: &'a P::PrivateData,
        purpose: ScanPurpose,
        filters: BoundFilterSet<'a, P::BoundPredicate>,
        scan_tuple_desc: pg_sys::TupleDesc,
        tuple_layout: &'a ScanTupleLayout,
        relation: RelationHandle<'a>,
        snapshot: SnapshotHandle<'a>,
        parallel_aware: bool,
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
            parallel_aware,
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

/// Context for [`LagodbCustomScanProvider::start`].
///
/// Begin has prepared the scan; PostgreSQL now supplies valid dynamic values.
/// This callback opens the first cursor and runs once before row production.
pub struct StartContext<'a, P: LagodbCustomScanProvider + ?Sized> {
    pub state: &'a mut P::State,
    pub purpose: ScanPurpose,
    /// Complete predicates, including the first dynamic parameter values.
    pub filters: BoundFilterSet<'a, P::BoundPredicate>,
    pub relation: RelationHandle<'a>,
    pub snapshot: SnapshotHandle<'a>,
}

impl<'a, P: LagodbCustomScanProvider> StartContext<'a, P> {
    pub(crate) fn new(
        state: &'a mut P::State,
        purpose: ScanPurpose,
        filters: BoundFilterSet<'a, P::BoundPredicate>,
        relation: RelationHandle<'a>,
        snapshot: SnapshotHandle<'a>,
    ) -> Self {
        Self {
            state,
            purpose,
            filters,
            relation,
            snapshot,
        }
    }
}

/// Context for [`LagodbCustomScanProvider::rescan`].
pub struct ReScanContext<'a, P: LagodbCustomScanProvider + ?Sized> {
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

impl<'a, P: LagodbCustomScanProvider> ReScanContext<'a, P> {
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

/// Context for [`LagodbCustomScanProvider::end`].
pub struct EndContext<'a, P: LagodbCustomScanProvider + ?Sized> {
    pub state: &'a mut P::State,
    pub relation: RelationHandle<'a>,
    _marker: PhantomData<&'a ()>,
}

impl<'a, P: LagodbCustomScanProvider> EndContext<'a, P> {
    pub(crate) fn new(state: &'a mut P::State, relation: RelationHandle<'a>) -> Self {
        Self {
            state,
            relation,
            _marker: PhantomData,
        }
    }
}
