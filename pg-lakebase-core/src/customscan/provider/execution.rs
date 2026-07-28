//! Executor-facing provider contexts and pushed-predicate translation.

use core::ffi::c_void;
use core::marker::PhantomData;
use core::ptr;

use pgrx::pg_sys;

use crate::batch::ScanBatchDriver;
use crate::customscan::error::CustomScanError;
use crate::customscan::plan_data::tuple_layout::{
    NeededColumns, ScanTupleDescriptor, ScanTupleLayout,
};
use crate::expr::PredicateBuilder;
use crate::expr::contract::{ColumnRef, PushdownContract};
use crate::expr::execution::params::ResolvedParam;
use crate::expr::translator::PgPredicateTranslator;
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

/// All predicate and parameter metadata associated with one executor scan.
///
/// The expression slices are borrowed from the framework-owned scan state;
/// the object does not copy PostgreSQL nodes or resolved parameter values.
pub struct PushedPredicates<'a> {
    pushed_exprs: &'a [*mut pg_sys::Expr],
    column_refs: &'a [ColumnRef],
    pushed_contracts: &'a [PushdownContract],
    resolved_params: &'a [ResolvedParam],
    scan_relid: core::ffi::c_int,
    tuple_layout: &'a ScanTupleLayout,
}

impl<'a> PushedPredicates<'a> {
    pub(crate) fn new(
        pushed_exprs: &'a [*mut pg_sys::Expr],
        column_refs: &'a [ColumnRef],
        pushed_contracts: &'a [PushdownContract],
        resolved_params: &'a [ResolvedParam],
        scan_relid: core::ffi::c_int,
        tuple_layout: &'a ScanTupleLayout,
    ) -> Self {
        Self {
            pushed_exprs,
            column_refs,
            pushed_contracts,
            resolved_params,
            scan_relid,
            tuple_layout,
        }
    }

    /// Whether any pushed predicates were recorded in `custom_exprs`.
    #[inline]
    pub fn has_pushed_predicates(&self) -> bool {
        !self.pushed_exprs.is_empty()
    }

    /// Number of pushed predicates in `custom_exprs`.
    #[inline]
    pub fn pushed_predicate_count(&self) -> usize {
        self.pushed_exprs.len()
    }

    /// Number of resolved executor parameters associated with this scan.
    #[inline]
    pub fn resolved_param_count(&self) -> usize {
        self.resolved_params.len()
    }

    /// Post-`rtoffset` scan relid used by predicate translation.
    #[inline]
    pub fn scan_relid(&self) -> core::ffi::c_int {
        self.scan_relid
    }

    /// Plan-time storage-column contract for this scan.
    #[inline]
    pub fn required_columns(&self) -> NeededColumns<'_> {
        self.tuple_layout.required_columns()
    }

    /// Translate every pushed expression into provider-native predicates.
    pub fn translate<T, F>(
        &self,
        make_translator: F,
    ) -> Result<Vec<T::Predicate>, CustomScanError>
    where
        T: PgPredicateTranslator,
        T::Error: Send + Sync,
        F: FnMut(usize) -> T,
    {
        // SAFETY: the framework builds these slices from one live executor
        // scan state and keeps them alive for the provider callback.
        unsafe { self.translate_selected(make_translator, |_| true) }
    }

    /// Translate only predicates independent of `PARAM_EXTERN` and
    /// `PARAM_EXEC` values.
    pub fn translate_static<T, F>(
        &self,
        make_translator: F,
    ) -> Result<Vec<T::Predicate>, CustomScanError>
    where
        T: PgPredicateTranslator,
        T::Error: Send + Sync,
        F: FnMut(usize) -> T,
    {
        unsafe {
            self.translate_selected(make_translator, |expr| {
                !RuntimeParamDetector::contains(expr)
            })
        }
    }

    /// Translate predicates stable across rescans. `PARAM_EXTERN` is stable
    /// after BeginCustomScan; `PARAM_EXEC` is not stable for parameterized paths.
    pub fn translate_rescan_stable<T, F>(
        &self,
        make_translator: F,
    ) -> Result<Vec<T::Predicate>, CustomScanError>
    where
        T: PgPredicateTranslator,
        T::Error: Send + Sync,
        F: FnMut(usize) -> T,
    {
        unsafe {
            self.translate_selected(make_translator, |expr| {
                !RuntimeParamDetector::contains_exec(expr)
            })
        }
    }

    unsafe fn translate_selected<T, F, I>(
        &self,
        mut make_translator: F,
        mut include: I,
    ) -> Result<Vec<T::Predicate>, CustomScanError>
    where
        T: PgPredicateTranslator,
        T::Error: Send + Sync,
        F: FnMut(usize) -> T,
        I: FnMut(*mut pg_sys::Expr) -> bool,
    {
        debug_assert_eq!(
            self.pushed_exprs.len(),
            self.pushed_contracts.len(),
            "pushed expression and contract slices must align by index",
        );

        let mut out = Vec::with_capacity(self.pushed_exprs.len());
        for index in 0..self.pushed_exprs.len() {
            if !include(self.pushed_exprs[index]) {
                continue;
            }
            let mut translator = make_translator(index);
            let result = unsafe {
                let mut builder = PredicateBuilder::with_var_resolver(
                    &mut translator,
                    self.pushed_exprs,
                    self.column_refs,
                    self.resolved_params,
                    self.tuple_layout.var_resolver(self.scan_relid),
                );
                builder.build_one(index)
            };

            match result {
                Ok(predicate) => out.push(predicate),
                Err(error) => {
                    let contract = self
                        .pushed_contracts
                        .get(index)
                        .copied()
                        .unwrap_or(PushdownContract::ExactRowFilter);
                    match contract {
                        PushdownContract::ConservativePruning => continue,
                        PushdownContract::ExactRowFilter => {
                            return Err(CustomScanError::predicate_build_at(
                                Some(index),
                                error,
                            ));
                        }
                    }
                }
            }
        }
        Ok(out)
    }
}

struct RuntimeParamDetector;

impl RuntimeParamDetector {
    unsafe fn contains(expr: *mut pg_sys::Expr) -> bool {
        unsafe { Self::walk(expr.cast(), ptr::null_mut()) }
    }

    unsafe fn contains_exec(expr: *mut pg_sys::Expr) -> bool {
        unsafe { Self::walk_exec(expr.cast(), ptr::null_mut()) }
    }

    unsafe extern "C-unwind" fn walk(
        node: *mut pg_sys::Node,
        context: *mut c_void,
    ) -> bool {
        if node.is_null() {
            return false;
        }
        match unsafe { (*node).type_ } {
            pg_sys::NodeTag::T_Param => matches!(
                unsafe { (*node.cast::<pg_sys::Param>()).paramkind },
                pg_sys::ParamKind::PARAM_EXTERN | pg_sys::ParamKind::PARAM_EXEC
            ),
            pg_sys::NodeTag::T_RestrictInfo => unsafe {
                Self::walk(
                    (*node.cast::<pg_sys::RestrictInfo>()).clause.cast(),
                    context,
                )
            },
            _ => unsafe {
                pg_sys::expression_tree_walker(node, Some(Self::walk), context)
            },
        }
    }

    unsafe extern "C-unwind" fn walk_exec(
        node: *mut pg_sys::Node,
        context: *mut c_void,
    ) -> bool {
        if node.is_null() {
            return false;
        }
        match unsafe { (*node).type_ } {
            pg_sys::NodeTag::T_Param => unsafe {
                (*node.cast::<pg_sys::Param>()).paramkind
                    == pg_sys::ParamKind::PARAM_EXEC
            },
            pg_sys::NodeTag::T_RestrictInfo => unsafe {
                Self::walk_exec(
                    (*node.cast::<pg_sys::RestrictInfo>()).clause.cast(),
                    context,
                )
            },
            _ => unsafe {
                pg_sys::expression_tree_walker(node, Some(Self::walk_exec), context)
            },
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
    /// Pushed expressions, contracts, and resolved parameter metadata.
    pub pushed_predicates: PushedPredicates<'a>,
    scan_tuple_desc: pg_sys::TupleDesc,
    /// Scan relation handle.
    pub relation: RelationHandle<'a>,
    /// Executor snapshot handle.
    pub snapshot: SnapshotHandle<'a>,
    _marker: PhantomData<&'a ()>,
}

impl<'a, P: LakebaseCustomScanProvider> BeginContext<'a, P> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        state: &'a mut P::State,
        private_data: &'a P::PrivateData,
        purpose: ScanPurpose,
        pushed_predicates: PushedPredicates<'a>,
        scan_tuple_desc: pg_sys::TupleDesc,
        relation: RelationHandle<'a>,
        snapshot: SnapshotHandle<'a>,
    ) -> Self {
        Self {
            state,
            private_data,
            purpose,
            pushed_predicates,
            scan_tuple_desc,
            relation,
            snapshot,
            _marker: PhantomData,
        }
    }

    /// Actual executor descriptor for the provider-filled raw scan slot.
    #[inline]
    pub fn scan_tuple(&self) -> ScanTupleDescriptor<'_> {
        unsafe {
            ScanTupleDescriptor::new(
                self.scan_tuple_desc,
                self.pushed_predicates.tuple_layout,
            )
        }
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
        let target_ctx = self.per_tuple_memory_context;
        emit_into_slot(
            || {
                let mut columns = unsafe { SlotColumns::new(slot, target_ctx) };
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
    /// Whether parameter-dependent predicates must be rebuilt.
    pub params_changed: bool,
    /// Query or modification-target purpose selected by the planner.
    pub purpose: ScanPurpose,
    /// Pushed expressions, contracts, and resolved parameter metadata.
    pub pushed_predicates: PushedPredicates<'a>,
    /// Scan relation handle.
    pub relation: RelationHandle<'a>,
    /// Executor snapshot handle.
    pub snapshot: SnapshotHandle<'a>,
    _marker: PhantomData<&'a ()>,
}

impl<'a, P: LakebaseCustomScanProvider> ReScanContext<'a, P> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        state: &'a mut P::State,
        params_changed: bool,
        purpose: ScanPurpose,
        pushed_predicates: PushedPredicates<'a>,
        relation: RelationHandle<'a>,
        snapshot: SnapshotHandle<'a>,
    ) -> Self {
        Self {
            state,
            params_changed,
            purpose,
            pushed_predicates,
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
