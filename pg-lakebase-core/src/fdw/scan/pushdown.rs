//! Provider-facing scan expression, pushdown, and runtime-value contexts.

use core::ffi::{c_int, c_void};
use core::marker::PhantomData;
use core::ptr;

use pgrx::pg_sys;

use crate::expr::contract::{ColumnRef, PushdownContract};
use crate::expr::inspect::{RelationExprAnalyzer, RelationScope};
use crate::handles::{RelationHandle, SnapshotHandle};

use super::super::row_identity::ForeignRowIdentityRequirement;
use super::context::ForeignPlanPrivate;
use super::error::ForeignScanError;
use super::projection::{ColumnRequirements, ScanProjection};
use super::slot::ScanOutputLayout;

/// Typed wrapper around provider-created `fdw_exprs`.
///
/// PostgreSQL owns the expression nodes and the list after the plan is built.
/// The provider must append planner-owned expression nodes that are valid for
/// the current planning memory context; the framework does not copy or free
/// them here because PostgreSQL performs the normal plan-tree ownership work.
#[derive(Debug, Default)]
pub struct ForeignExprs {
    raw: *mut pg_sys::List,
}

impl ForeignExprs {
    #[inline]
    pub const fn new() -> Self {
        Self {
            raw: ptr::null_mut(),
        }
    }

    /// Append one planner-owned expression.
    ///
    /// # Safety
    ///
    /// `expr` must be a live PostgreSQL expression node in the current planner
    /// memory context.  It must remain valid until PostgreSQL finishes the plan.
    /// The final plan callback also validates that the expression is an
    /// activation/rescan expression and does not depend on the current scan
    /// relation.
    pub unsafe fn push(
        &mut self,
        expr: *mut pg_sys::Expr,
    ) -> Result<(), ForeignScanError> {
        if expr.is_null() {
            return Err(ForeignScanError::framework(
                "ForeignExprs cannot contain a NULL expression node",
            ));
        }
        self.raw = unsafe { pg_sys::lappend(self.raw, expr.cast::<c_void>()) };
        Ok(())
    }

    /// Validate the scan-activation contract for all stored expressions.
    ///
    /// `fdw_exprs` are evaluated when a provider scan is activated or
    /// rescanned.  The current foreign scan tuple does not exist at that
    /// point, so expressions may use external parameters, executor
    /// parameters, constants, and outer-relation values, but not the current
    /// scan relation (including whole-row, system-column, or placeholder
    /// dependencies).
    ///
    /// # Safety
    ///
    /// `root` must be the live planner root for the expression nodes stored in
    /// this object.  The list and every node in it must remain live for the
    /// duration of the call.
    pub(crate) unsafe fn validate_for_scan(
        &self,
        root: *mut pg_sys::PlannerInfo,
        scan_relid: pg_sys::Index,
    ) -> Result<(), ForeignScanError> {
        if root.is_null() || scan_relid == 0 {
            return Err(ForeignScanError::framework(
                "cannot validate fdw_exprs without a planner root and scan relation",
            ));
        }
        if self.raw.is_null() {
            return Ok(());
        }

        let analyzer = RelationExprAnalyzer::new(RelationScope::exact(scan_relid));
        let length = unsafe { pg_sys::list_length(self.raw) };
        for index in 0..length {
            let expr =
                unsafe { pg_sys::list_nth(self.raw, index) } as *mut pg_sys::Expr;
            if expr.is_null() {
                return Err(ForeignScanError::framework(
                    "fdw_exprs contains a NULL expression node",
                ));
            }
            if unsafe { analyzer.depends_on_relation(root, expr) } {
                return Err(ForeignScanError::unsupported(
                    "fdw_exprs cannot depend on the current foreign scan relation",
                ));
            }
        }
        Ok(())
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.raw.is_null()
    }

    #[inline]
    pub(crate) fn as_raw(&self) -> *mut pg_sys::List {
        self.raw
    }
}

/// PG expression list view used for final pushed and residual quals.
#[derive(Clone, Copy)]
pub struct ForeignExprList<'a> {
    raw: *mut pg_sys::List,
    len: usize,
    _marker: PhantomData<&'a pg_sys::List>,
}

impl<'a> ForeignExprList<'a> {
    /// # Safety
    ///
    /// If non-null, `raw` must be a PostgreSQL-owned plan list that remains
    /// live for the returned borrowed view.
    pub(crate) unsafe fn from_raw(raw: *mut pg_sys::List) -> Self {
        let len = if raw.is_null() {
            0
        } else {
            unsafe { pg_sys::list_length(raw) as usize }
        };
        Self {
            raw,
            len,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Return an expression pointer in plan-list order.
    #[inline]
    pub fn get(&self, index: usize) -> Option<*mut pg_sys::Expr> {
        if index >= self.len {
            return None;
        }
        let node = unsafe { pg_sys::list_nth(self.raw, index as i32) };
        (!node.is_null()).then_some(node as *mut pg_sys::Expr)
    }
}

/// Pushdown information made available to a provider at provider start/ReScan.
///
/// `recheck_expressions` is the setrefs-processed exact-expression list that
/// the framework places in `fdw_recheck_quals`.  `pushed_contracts` and
/// `column_refs` describe the complete pushed-expression section in its
/// planner order; conservative pruning expressions remain outside the PG
/// recheck list because they are not exact row filters.
pub struct ForeignPushdown<'a> {
    recheck_expressions: ForeignExprList<'a>,
    pushed_contracts: &'a [PushdownContract],
    column_refs: &'a [ColumnRef],
}

impl<'a> ForeignPushdown<'a> {
    pub(crate) fn new(
        recheck_expressions: ForeignExprList<'a>,
        pushed_contracts: &'a [PushdownContract],
        column_refs: &'a [ColumnRef],
    ) -> Self {
        Self {
            recheck_expressions,
            pushed_contracts,
            column_refs,
        }
    }

    #[inline]
    pub fn recheck_expressions(&self) -> ForeignExprList<'_> {
        self.recheck_expressions
    }

    #[inline]
    pub fn pushed_contracts(&self) -> &[PushdownContract] {
        self.pushed_contracts
    }

    /// Contracts corresponding to `recheck_expressions`, in the same order.
    /// The iterator is allocation-free and filters the complete pushed
    /// contract section down to exact row filters.
    #[inline]
    pub fn recheck_contracts(&self) -> impl Iterator<Item = PushdownContract> + '_ {
        self.pushed_contracts
            .iter()
            .copied()
            .filter(|contract| contract.requires_recheck())
    }

    #[inline]
    pub fn column_refs(&self) -> &[ColumnRef] {
        self.column_refs
    }

    /// Whether the plan contains no pushed predicate of either contract.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.pushed_contracts.is_empty()
    }
}

/// One value produced by evaluating a `fdw_exprs` expression.
///
/// The Datum is valid only for the provider callback that receives it.  It may
/// be pass-by-reference and remain owned by PostgreSQL-managed memory;
/// a provider that retains a value in its state must copy it using the
/// expression's PostgreSQL type semantics before the callback returns.
#[derive(Debug, Clone, Copy)]
pub struct ForeignExpressionValue {
    pub datum: pg_sys::Datum,
    pub is_null: bool,
}

/// Borrowed runtime values corresponding to `ForeignExprs` order.
///
/// Providers consume these values in `begin`/`rescan`; `next_slot` does not
/// expose them because a scan state must own any value needed on the row path.
/// The framework evaluates them in PostgreSQL's standard per-tuple context and
/// does not extend the lifetime of the returned Datum values.
#[derive(Clone, Copy)]
pub struct RuntimeExpressionValues<'a> {
    values: &'a [ForeignExpressionValue],
}

impl<'a> RuntimeExpressionValues<'a> {
    pub(crate) fn new(values: &'a [ForeignExpressionValue]) -> Self {
        Self { values }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<ForeignExpressionValue> {
        self.values.get(index).copied()
    }
}

/// Context passed when the provider is started.
///
/// The framework initializes the executor-side wrapper during PostgreSQL's
/// `BeginForeignScan` callback, but invokes the provider's `begin` only when
/// the first valid parameter set is available or when the first row is
/// requested for an unparameterized scan.
pub struct BeginForeignScanContext<'a, D: ForeignPlanPrivate> {
    pub private_data: &'a D,
    pub relation: RelationHandle<'a>,
    pub snapshot: SnapshotHandle<'a>,
    pub projection: &'a ScanProjection,
    pub required_columns: &'a ColumnRequirements,
    pub output_layout: ScanOutputLayout<'a>,
    pub row_identity_requirement: ForeignRowIdentityRequirement,
    pub pushdown: ForeignPushdown<'a>,
    pub expressions: RuntimeExpressionValues<'a>,
    pub estate: *mut pg_sys::EState,
    pub econtext: *mut pg_sys::ExprContext,
    pub eflags: c_int,
    pub(crate) effective_user_id: pg_sys::Oid,
}

impl<'a, D: ForeignPlanPrivate> BeginForeignScanContext<'a, D> {
    /// The role PostgreSQL selected for the foreign scan's user mapping.
    #[inline]
    pub fn effective_user_id(&self) -> pg_sys::Oid {
        self.effective_user_id
    }
}

/// ReScan callback context.  Runtime expressions are reevaluated before the
/// provider is called, including `PARAM_EXEC` values of a nested-loop path.
pub struct ReScanForeignScanContext<'a> {
    pub relation: RelationHandle<'a>,
    pub snapshot: SnapshotHandle<'a>,
    pub projection: &'a ScanProjection,
    pub required_columns: &'a ColumnRequirements,
    pub pushdown: ForeignPushdown<'a>,
    pub expressions: RuntimeExpressionValues<'a>,
    pub params_changed: bool,
    pub estate: *mut pg_sys::EState,
    pub econtext: *mut pg_sys::ExprContext,
}
