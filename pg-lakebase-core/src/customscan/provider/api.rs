//! Provider SPI: trait plus typed planning and execution contexts.
//! PG-`Expr` lives in the plan tree; native predicates live in `State` at
//! runtime. Use [`PathVariant::kind`], not `param_info.is_some()`, for variants.

use core::ffi::{CStr, c_void};
use core::marker::PhantomData;
use core::ptr;

use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys;

use crate::api::{AmModifyState, TableAccessMethod};
use crate::batch::ScanBatchDriver;
pub use crate::customscan::ScanPurpose;
use crate::customscan::custom_private::CustomScanPrivate;
pub use crate::customscan::custom_private::NoPrivateData;
use crate::expr::nodes::PgParamValue;
use crate::expr::predicate::PlanPredicate;
use crate::expr::split::{ColumnRef, PushdownContract, QualPushdownDecision};
use crate::expr::translator::{PgPredicateTranslator, PredicateBuilder};
use crate::handles::{RelationHandle, SnapshotHandle};
use crate::tuple::{Row, SlotColumns, TupleSlotWriter};

// Error type
/// Provider runtime error; re-exported from [`crate::customscan::error`].
pub use crate::customscan::error::CustomScanError;
pub use crate::customscan::tuple_layout::{
    NeededColumns, ScanTupleDescriptor, ScanTupleLayout, WHOLEROW_NAME,
};
#[cfg(feature = "pg_test")]
#[doc(hidden)]
pub use crate::customscan::tuple_layout::{ScanTuplePlanProbe, ScanTupleShape};

// Relids alias
/// Nullable PG `Bitmapset *` (Relids). Use `bms_is_empty`, not pointer null test.
pub type Relids = *mut pg_sys::Bitmapset;

// PathVariantKind / PathVariant
/// Plain vs join-parameterized CustomPath variant. Use [`Self::kind`], not
/// `param_info.is_some()`, to tell them apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PathVariantKind {
    /// Plain variant (`required_outer = lateral_relids`).
    Plain,

    /// Join-parameterized variant (one per surviving `outer_relids`).
    JoinParameterized,
}

/// Path-stage pushdown metadata for [`PathVariant`]; no raw PG `Expr` pointers.
///
/// Built by core from an internal [`crate::expr::split::PlanPushdownSplit`] before
/// [`LakebaseCustomScanProvider::create_path`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PathPushdownSummary {
    /// Number of clauses core classified as pushed for this variant.
    pub pushed_count: usize,
    /// Pushed clauses with [`PushdownContract::ExactRowFilter`].
    pub exact_row_filter_count: usize,
    /// Pushed clauses with [`PushdownContract::ConservativePruning`].
    pub conservative_pruning_count: usize,
    /// Pushed clauses with [`PushdownCosting::CostedPruning`].
    pub costed_pruning_count: usize,
    /// Combined selectivity of costed-pruning pushed clauses (`clauselist_selectivity`).
    pub pruning_selectivity: f64,
}

impl PathPushdownSummary {
    /// Whether this variant has any pushed predicates (core already gated security/movability).
    #[inline]
    pub fn has_pushed_predicates(self) -> bool {
        self.pushed_count > 0
    }

    /// Summarize an internal plan split plus planner-computed costed-pruning selectivity.
    pub(crate) fn from_split(
        split: &crate::expr::split::PlanPushdownSplit,
        pruning_selectivity: f64,
    ) -> Self {
        let pushed_count = split.pushed.len();
        let mut exact_row_filter_count = 0;
        let mut conservative_pruning_count = 0;
        let mut costed_pruning_count = 0;
        for p in &split.pushed {
            match p.contract {
                PushdownContract::ExactRowFilter => exact_row_filter_count += 1,
                PushdownContract::ConservativePruning => {
                    conservative_pruning_count += 1
                }
            }
            if p.costing.is_costed() {
                costed_pruning_count += 1;
            }
        }
        Self {
            pushed_count,
            exact_row_filter_count,
            conservative_pruning_count,
            costed_pruning_count,
            pruning_selectivity: pruning_selectivity.clamp(0.0, 1.0),
        }
    }
}

/// Per-variant input to [`LakebaseCustomScanProvider::create_path`].
pub struct PathVariant<'a> {
    /// Query scan or modification-target scan using the same provider.
    pub purpose: ScanPurpose,

    /// Branch on this, not `param_info.is_some()`.
    pub kind: PathVariantKind,

    /// Set when `required_outer` is non-empty (use `bms_is_empty`, not pointer test).
    pub param_info: Option<&'a pg_sys::ParamPathInfo>,

    /// Required outer relids for this variant.
    pub required_outer: Relids,

    /// Pre-gated pushdown summary for this variant (`baserestrictinfo` + `ppi_clauses`).
    pub pushdown: PathPushdownSummary,
}

// Path-stage / plan-stage classifier contexts
/// Path-stage context for `supports_relation` / `create_path` (typed planner accessors).
pub struct RelPathContext {
    /// Range table entry for the relation under consideration.
    rte: *mut pg_sys::RangeTblEntry,

    /// Planner root; NULL only in supports_relation-only context.
    root: *mut pg_sys::PlannerInfo,

    /// Baserel; NULL in supports_relation-only context.
    baserel: *mut pg_sys::RelOptInfo,
}

impl RelPathContext {
    /// RTE-only; non-null live `rte`.
    ///
    /// # Safety
    ///
    /// `rte` must be a non-NULL planner-owned `RangeTblEntry` that remains live
    /// for the duration of use.
    #[inline]
    pub unsafe fn new(rte: *mut pg_sys::RangeTblEntry) -> Self {
        Self {
            rte,
            root: ptr::null_mut(),
            baserel: ptr::null_mut(),
        }
    }

    /// Full planner context; non-null live `rte`/`root`/`baserel`.
    ///
    /// # Safety
    ///
    /// All pointers must be non-NULL planner-owned nodes from the same planning
    /// invocation and remain live for the duration of use.
    #[inline]
    pub unsafe fn with_planner(
        rte: *mut pg_sys::RangeTblEntry,
        root: *mut pg_sys::PlannerInfo,
        baserel: *mut pg_sys::RelOptInfo,
    ) -> Self {
        Self { rte, root, baserel }
    }

    /// The relation's `pg_class` OID (`rte->relid`) — the same OID the
    /// framework forwards to every provider.
    #[inline]
    pub fn rel_oid(&self) -> pg_sys::Oid {
        // SAFETY: `rte` is a non-NULL planner-owned `RangeTblEntry` live
        // per the constructor contract; `relid` is a plain field.
        unsafe { (*self.rte).relid }
    }

    /// The range-table entry kind (`rte->rtekind`). Providers compare this
    /// against [`pg_sys::RTEKind::RTE_RELATION`] to reject subquery /
    /// values / function / CTE RTEs as a defense-in-depth re-check.
    #[inline]
    pub fn rtekind(&self) -> pg_sys::RTEKind::Type {
        // SAFETY: see `rel_oid`.
        unsafe { (*self.rte).rtekind }
    }

    /// The relation kind (`rte->relkind`) as `u8`, ready to match against
    /// PG's `RELKIND_*` constants.
    #[inline]
    pub fn relkind(&self) -> u8 {
        // SAFETY: see `rel_oid`.
        unsafe { (*self.rte).relkind as u8 }
    }

    /// The relation's table access method OID (`pg_class.relam`), resolved
    /// through the syscache. Returns [`pg_sys::Oid::INVALID`] for relations
    /// without a TableAM (indexes, sequences, views).
    #[inline]
    pub fn access_method_oid(&self) -> pg_sys::Oid {
        // SAFETY: `get_rel_relam` is a syscache lookup that tolerates any
        // OID (returning `InvalidOid` for a missing row); `rel_oid()` is a
        // valid `pg_class` OID resolved by the planner.
        unsafe { pg_sys::get_rel_relam(self.rel_oid()) }
    }

    /// The relation's tablespace OID (`pg_class.reltablespace`), resolved
    /// through the syscache. May be [`pg_sys::Oid::INVALID`] for the
    /// database's default tablespace.
    #[inline]
    pub fn tablespace_oid(&self) -> pg_sys::Oid {
        // SAFETY: `get_rel_tablespace` tolerates any OID; `rel_oid()` is a
        // valid `pg_class` OID resolved by the planner.
        unsafe { pg_sys::get_rel_tablespace(self.rel_oid()) }
    }

    /// `baserel->pages` (unpruned baseline). Requires [`Self::with_planner`].
    #[inline]
    pub fn baserel_pages(&self) -> f64 {
        debug_assert!(
            !self.baserel.is_null(),
            "RelPathContext::baserel_pages requires a with_planner context",
        );
        // SAFETY: `baserel` is a non-NULL planner-owned `RelOptInfo` live
        // per the `with_planner` contract; `pages` is a plain field.
        unsafe { (*self.baserel).pages as f64 }
    }

    /// `baserel->tuples` (unpruned baseline). Requires [`Self::with_planner`].
    #[inline]
    pub fn baserel_tuples(&self) -> f64 {
        debug_assert!(
            !self.baserel.is_null(),
            "RelPathContext::baserel_tuples requires a with_planner context",
        );
        // SAFETY: see `baserel_pages`; `tuples` is a plain field.
        unsafe { (*self.baserel).tuples }
    }

    /// Planner-estimated width of the relation's ordinary output tuple.
    #[inline]
    pub fn baserel_width(&self) -> i32 {
        debug_assert!(
            !self.baserel.is_null(),
            "RelPathContext::baserel_width requires a with_planner context",
        );
        let target = unsafe { (*self.baserel).reltarget };
        debug_assert!(!target.is_null());
        unsafe { (*target).width }
    }

    /// Whether the base relation PathTarget requests a whole-row value from
    /// this relation. Modify providers use this to cost the optional PostgreSQL
    /// `wholerow` column from the same planner-visible contract that drives the
    /// final scan tuple layout.
    pub fn modify_requests_wholerow(&self) -> bool {
        debug_assert!(
            !self.baserel.is_null(),
            "RelPathContext::modify_requests_wholerow requires a with_planner context",
        );
        let target = unsafe { (*self.baserel).reltarget };
        if target.is_null() {
            return false;
        }
        let exprs = unsafe { (*target).exprs };
        let len = unsafe { pg_sys::list_length(exprs) };
        for index in 0..len {
            let mut expr =
                unsafe { pg_sys::list_nth(exprs, index) }.cast::<pg_sys::Expr>();
            if !expr.is_null()
                && unsafe { (*expr).type_ } == pg_sys::NodeTag::T_ConvertRowtypeExpr
            {
                expr = unsafe { (*expr.cast::<pg_sys::ConvertRowtypeExpr>()).arg };
            }
            if expr.is_null() || unsafe { (*expr).type_ } != pg_sys::NodeTag::T_Var {
                continue;
            }
            let var = expr.cast::<pg_sys::Var>();
            if unsafe {
                (*var).varno == (*self.baserel).relid as i32
                    && (*var).varattno
                        == pg_sys::InvalidAttrNumber as pg_sys::AttrNumber
                    && (*var).varlevelsup == 0
            } {
                return true;
            }
        }

        // Partition child PathTargets can lose the nominal target's direct
        // whole-row Var during appendrel translation. The rewrite-complete
        // Query targetlist carries the injected target wholerow before path
        // creation, so every Modify leaf must retain all business columns.
        let parse = unsafe { (*self.root).parse };
        let query_targetlist = if parse.is_null() {
            ptr::null_mut()
        } else {
            unsafe { (*parse).targetList }
        };
        let len = unsafe { pg_sys::list_length(query_targetlist) };
        for index in 0..len {
            let tle = unsafe { pg_sys::list_nth(query_targetlist, index) }
                .cast::<pg_sys::TargetEntry>();
            if tle.is_null() {
                continue;
            }
            let mut expr = unsafe { (*tle).expr };
            if !expr.is_null()
                && unsafe { (*expr).type_ } == pg_sys::NodeTag::T_ConvertRowtypeExpr
            {
                expr = unsafe { (*expr.cast::<pg_sys::ConvertRowtypeExpr>()).arg };
            }
            if !expr.is_null() && unsafe { (*expr).type_ } == pg_sys::NodeTag::T_Var {
                let var = expr.cast::<pg_sys::Var>();
                if unsafe {
                    (*var).varno == (*parse).resultRelation
                        && (*var).varattno
                            == pg_sys::InvalidAttrNumber as pg_sys::AttrNumber
                        && (*var).varlevelsup == 0
                } {
                    return true;
                }
            }
        }
        false
    }

    /// Combined selectivity of the given qual exprs via `clauselist_selectivity`;
    /// returns `1.0` when empty. Path stage passes costed-pruning pushed exprs only.
    pub(crate) fn clauselist_selectivity_for_exprs(
        &self,
        exprs: &[*mut pg_sys::Expr],
    ) -> f64 {
        debug_assert!(
            !self.root.is_null(),
            "RelPathContext::clauselist_selectivity_for_exprs requires a with_planner context",
        );
        if exprs.is_empty() {
            return 1.0;
        }

        // SAFETY: planner-owned Expr pointers; list in planner memory context.
        let mut clauses: *mut pg_sys::List = ptr::null_mut();
        for &expr in exprs {
            clauses = unsafe { pg_sys::lappend(clauses, expr.cast()) };
        }

        // SAFETY: live PlannerInfo; base-restriction selectivity for this rel.
        let sel = unsafe {
            pg_sys::clauselist_selectivity(
                self.root,
                clauses,
                (*self.baserel).relid as core::ffi::c_int,
                pg_sys::JoinType::JOIN_INNER,
                ptr::null_mut(),
            )
        };

        sel.clamp(0.0, 1.0)
    }
}

/// Plan-stage context for [`LakebaseCustomScanProvider::classify_predicate`].
/// Leaf exprs are pre-gated; compare `Var.varno` to [`Self::scan_relid`].
pub struct PlanTranslateContext {
    /// The base relation whose clause is being classified.
    baserel: *mut pg_sys::RelOptInfo,
}

impl PlanTranslateContext {
    /// From live `baserel`.
    ///
    /// # Safety
    ///
    /// `baserel` must be a non-NULL planner-owned `RelOptInfo` that remains live
    /// for the duration of use.
    #[inline]
    pub unsafe fn new(baserel: *mut pg_sys::RelOptInfo) -> Self {
        Self { baserel }
    }

    /// The scan relation's range-table index (`baserel->relid`), i.e. the
    /// `Var.varno` value that identifies this relation's own columns at
    /// classification time (before `replace_nestloop_params`).
    #[inline]
    pub fn scan_relid(&self) -> core::ffi::c_int {
        // SAFETY: `baserel` is a non-NULL planner-owned `RelOptInfo` live
        // per the constructor contract; `relid` is a plain field.
        unsafe { (*self.baserel).relid as core::ffi::c_int }
    }
}

pub use crate::customscan::builder::{CustomPathBuilder, CustomPathPlan};

/// Context for [`LakebaseCustomScanProvider::create_state`].
pub struct CreateStateContext<P: LakebaseCustomScanProvider + ?Sized> {
    _marker: PhantomData<fn() -> P>,
}

impl<P: LakebaseCustomScanProvider + ?Sized> CreateStateContext<P> {
    /// Construct a [`CreateStateContext<P>`] by initializing every
    /// field explicitly.
    ///
    /// This deliberately avoids `mem::zeroed`: every field is named
    /// here, so adding a non-zero-sized field becomes a compile error
    /// unless it too is initialized to a defined value (no reliance on
    /// the struct staying zero-sized for soundness).
    pub(crate) fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

struct PushedPredicateInputs<'a> {
    pushed_exprs: &'a [*mut pg_sys::Expr],
    column_refs: &'a [ColumnRef],
    pushed_contracts: &'a [PushdownContract],
    resolved_params: &'a [PgParamValue],
    scan_relid: core::ffi::c_int,
    tuple_layout: &'a ScanTupleLayout,
}

impl<'a> PushedPredicateInputs<'a> {
    fn new(
        pushed_exprs: &'a [*mut pg_sys::Expr],
        column_refs: &'a [ColumnRef],
        pushed_contracts: &'a [PushdownContract],
        resolved_params: &'a [PgParamValue],
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

    /// # Safety
    ///
    /// `pushed_exprs` must be the live `custom_exprs[pushed]` slice for the
    /// current executor callback.
    unsafe fn translate<T, F>(
        &self,
        make_translator: F,
    ) -> Result<Vec<T::Predicate>, CustomScanError>
    where
        T: PgPredicateTranslator,
        T::Error: Send + Sync,
        F: FnMut(usize) -> T,
    {
        unsafe { self.translate_selected(make_translator, |_| true) }
    }

    /// Translate only predicates whose executor expression contains no
    /// PARAM_EXTERN/PARAM_EXEC node. This is suitable for commit-time conflict
    /// scopes, which must not depend on values that can change across rescans.
    unsafe fn translate_static<T, F>(
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

    /// Translate predicates whose executor expression is stable for the life of
    /// one scan execution. `PARAM_EXTERN` is fixed after `BeginCustomScan`, but
    /// `PARAM_EXEC` can change when PostgreSQL rescans a parameterized inner
    /// path.
    unsafe fn translate_rescan_stable<T, F>(
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
            "translate_pushed_predicates: pushed_exprs and pushed_contracts must align by index",
        );

        let mut out: Vec<T::Predicate> = Vec::with_capacity(self.pushed_exprs.len());
        for i in 0..self.pushed_exprs.len() {
            if !include(self.pushed_exprs[i]) {
                continue;
            }
            let mut translator = make_translator(i);
            let result = unsafe {
                let mut builder = PredicateBuilder::with_var_resolver(
                    &mut translator,
                    self.pushed_exprs,
                    self.column_refs,
                    self.resolved_params,
                    self.tuple_layout.var_resolver(self.scan_relid),
                );
                builder.build_one(i)
            };

            match result {
                Ok(pred) => out.push(pred),
                Err(err) => {
                    let contract = self
                        .pushed_contracts
                        .get(i)
                        .copied()
                        .unwrap_or(PushdownContract::ExactRowFilter);
                    match contract {
                        PushdownContract::ConservativePruning => continue,
                        PushdownContract::ExactRowFilter => {
                            return Err(CustomScanError::predicate_build_at(
                                Some(i),
                                err,
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
            pg_sys::NodeTag::T_Param => {
                matches!(
                    unsafe { (*node.cast::<pg_sys::Param>()).paramkind },
                    pg_sys::ParamKind::PARAM_EXTERN | pg_sys::ParamKind::PARAM_EXEC
                )
            }
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
            pg_sys::NodeTag::T_Param => {
                let paramkind = unsafe { (*node.cast::<pg_sys::Param>()).paramkind };
                paramkind == pg_sys::ParamKind::PARAM_EXEC
            }
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

    pushed_exprs: &'a [*mut pg_sys::Expr],

    column_refs: &'a [crate::expr::split::ColumnRef],
    pushed_contracts: &'a [crate::expr::split::PushdownContract],
    resolved_params: &'a [crate::expr::nodes::PgParamValue],
    scan_relid: core::ffi::c_int,
    tuple_layout: &'a ScanTupleLayout,
    scan_tuple_desc: pg_sys::TupleDesc,

    /// Scan relation handle.
    pub relation: RelationHandle<'a>,

    /// Executor snapshot handle.
    pub snapshot: SnapshotHandle<'a>,

    #[allow(dead_code)]
    estate: *mut pg_sys::EState,

    #[allow(dead_code)]
    per_tuple_memory_context: pg_sys::MemoryContext,

    #[allow(dead_code)]
    eflags: core::ffi::c_int,

    _marker: PhantomData<&'a ()>,
}

impl<'a, P: LakebaseCustomScanProvider> BeginContext<'a, P> {
    /// Internal constructor. Only the framework's `BeginCustomScan`
    /// trampoline  builds [`BeginContext`] values; providers
    /// receive them as a parameter.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        state: &'a mut P::State,
        private_data: &'a P::PrivateData,
        purpose: ScanPurpose,
        pushed_exprs: &'a [*mut pg_sys::Expr],
        column_refs: &'a [crate::expr::split::ColumnRef],
        pushed_contracts: &'a [crate::expr::split::PushdownContract],
        resolved_params: &'a [crate::expr::nodes::PgParamValue],
        scan_relid: core::ffi::c_int,
        tuple_layout: &'a ScanTupleLayout,
        scan_tuple_desc: pg_sys::TupleDesc,
        relation: RelationHandle<'a>,
        snapshot: SnapshotHandle<'a>,
        estate: *mut pg_sys::EState,
        per_tuple_memory_context: pg_sys::MemoryContext,
        eflags: core::ffi::c_int,
    ) -> Self {
        Self {
            state,
            private_data,
            purpose,
            pushed_exprs,
            column_refs,
            pushed_contracts,
            resolved_params,
            scan_relid,
            tuple_layout,
            scan_tuple_desc,
            relation,
            snapshot,
            estate,
            per_tuple_memory_context,
            eflags,
            _marker: PhantomData,
        }
    }

    /// Whether any pushed predicates were recorded in `custom_exprs`.
    #[inline]
    pub fn has_pushed_predicates(&self) -> bool {
        !self.pushed_exprs.is_empty()
    }

    /// Number of pushed predicates in `custom_exprs` (metadata only; no PG pointers).
    #[inline]
    pub fn pushed_predicate_count(&self) -> usize {
        self.pushed_exprs.len()
    }

    /// Translate pushed PG expressions into provider-native predicates.
    pub fn translate_pushed_predicates<T, F>(
        &self,
        make_translator: F,
    ) -> Result<Vec<T::Predicate>, CustomScanError>
    where
        T: PgPredicateTranslator,
        T::Error: Send + Sync,
        F: FnMut(usize) -> T,
    {
        // SAFETY: `pushed_exprs` is the live `custom_exprs[pushed]` slice built
        // by the BeginCustomScan trampoline for this scan.
        unsafe { self.pushed_predicate_inputs().translate(make_translator) }
    }

    /// Translate only pushed predicates that are independent of executor
    /// parameters. Dropping dynamic conjuncts widens the result and is safe
    /// for provider conflict-detection scopes.
    pub fn translate_static_pushed_predicates<T, F>(
        &self,
        make_translator: F,
    ) -> Result<Vec<T::Predicate>, CustomScanError>
    where
        T: PgPredicateTranslator,
        T::Error: Send + Sync,
        F: FnMut(usize) -> T,
    {
        // SAFETY: same live expression contract as
        // `translate_pushed_predicates`; the static filter only inspects nodes.
        unsafe {
            self.pushed_predicate_inputs()
                .translate_static(make_translator)
        }
    }

    /// Translate pushed predicates that are stable across rescans of this scan
    /// node. These predicates may use `PARAM_EXTERN`, but never `PARAM_EXEC`.
    /// Providers can use them for stable file/task planning while still using
    /// the full pushed predicate set as the exact row filter.
    pub fn translate_rescan_stable_pushed_predicates<T, F>(
        &self,
        make_translator: F,
    ) -> Result<Vec<T::Predicate>, CustomScanError>
    where
        T: PgPredicateTranslator,
        T::Error: Send + Sync,
        F: FnMut(usize) -> T,
    {
        // SAFETY: same live expression contract as
        // `translate_pushed_predicates`; the stable filter only inspects nodes.
        unsafe {
            self.pushed_predicate_inputs()
                .translate_rescan_stable(make_translator)
        }
    }

    fn pushed_predicate_inputs(&self) -> PushedPredicateInputs<'_> {
        PushedPredicateInputs::new(
            self.pushed_exprs,
            self.column_refs,
            self.pushed_contracts,
            self.resolved_params,
            self.scan_relid,
            self.tuple_layout,
        )
    }

    /// Plan-time storage-column contract. No executor expression walk or
    /// allocation occurs here.
    pub fn required_columns(&self) -> NeededColumns<'_> {
        self.tuple_layout.required_columns()
    }

    /// Actual executor descriptor for the provider-filled raw scan slot.
    pub fn scan_tuple(&self) -> ScanTupleDescriptor<'_> {
        unsafe { ScanTupleDescriptor::new(self.scan_tuple_desc, self.tuple_layout) }
    }
}

/// One-time outer-executor binding for a provider scan in `Modify` purpose.
pub struct ModifyBindContext<'a, P: LakebaseCustomModifyProvider + ?Sized> {
    pub state: &'a mut P::State,
    pub relation: RelationHandle<'a>,
    pub binding: crate::access::mutation::ModifyScanBinding<
        <P::AccessMethod as TableAccessMethod>::ModifyQueryState,
    >,
    _marker: PhantomData<&'a ()>,
}

impl<'a, P: LakebaseCustomModifyProvider> ModifyBindContext<'a, P> {
    pub(crate) fn new(
        state: &'a mut P::State,
        relation: RelationHandle<'a>,
        binding: crate::access::mutation::ModifyScanBinding<
            <P::AccessMethod as TableAccessMethod>::ModifyQueryState,
        >,
    ) -> Self {
        Self {
            state,
            relation,
            binding,
            _marker: PhantomData,
        }
    }
}

/// Context for [`LakebaseCustomScanProvider::next_slot`].
pub struct NextSlotContext<'a, P: LakebaseCustomScanProvider + ?Sized> {
    /// Provider's per-scan runtime state.
    pub state: &'a mut P::State,

    /// Scan relation handle.
    pub relation: RelationHandle<'a>,

    slot: *mut pg_sys::TupleTableSlot,

    #[allow(dead_code)]
    estate: *mut pg_sys::EState,

    #[allow(dead_code)]
    econtext: *mut pg_sys::ExprContext,

    /// The scan node's per-tuple memory context (`econtext->ecxt_per_tuple_memory`).
    ///
    /// This is the context emitted slot datums (varlena / array payloads) are
    /// palloc'd into. `ExecScan` calls `ResetExprContext` at the start of every
    /// tuple cycle (before invoking the access method), so each row's payload is
    /// reclaimed once the consumer has processed the prior row — bounding scan
    /// memory to a single live tuple. Writing into the slot's own `tts_mcxt`
    /// (per-query lifetime) instead would accumulate one row's worth of
    /// by-reference data per scanned row for the whole query.
    per_tuple_memory_context: pg_sys::MemoryContext,

    _marker: PhantomData<&'a ()>,
}

impl<'a, P: LakebaseCustomScanProvider> NextSlotContext<'a, P> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        state: &'a mut P::State,
        relation: RelationHandle<'a>,
        slot: *mut pg_sys::TupleTableSlot,
        estate: *mut pg_sys::EState,
        econtext: *mut pg_sys::ExprContext,
        per_tuple_memory_context: pg_sys::MemoryContext,
    ) -> Self {
        Self {
            state,
            relation,
            slot,
            estate,
            econtext,
            per_tuple_memory_context,
            _marker: PhantomData,
        }
    }

    /// Write `row` into the scan slot; marks slot non-empty on success.
    ///
    /// Datums are materialized into the scan node's per-tuple memory context
    /// (reclaimed by `ExecScan`'s per-cycle `ResetExprContext`), not the slot's
    /// per-query `tts_mcxt`, so a long scan over by-reference columns does not
    /// accumulate one row's payload per scanned row.
    pub fn emit_row(&mut self, row: &mut Row) -> Result<(), CustomScanError> {
        // SAFETY: slot and the per-tuple context live for this next_slot callback.
        let writer =
            unsafe { TupleSlotWriter::new(self.slot, self.per_tuple_memory_context) };
        unsafe { writer.write_row(row) }.map_err(CustomScanError::from)
    }

    /// Drive a slot-first scan driver straight into the scan slot; marks the
    /// slot non-empty on a produced row. `Ok(false)` is end-of-scan.
    pub fn emit_columns<D: ScanBatchDriver>(
        &mut self,
        driver: &mut D,
    ) -> Result<bool, CustomScanError> {
        self.emit_with(|columns| driver.next_into_slot(columns))
    }

    /// Fill the raw scan slot through one cohesive slot-first operation.
    /// Providers use this when a tuple layout contains metadata fields in
    /// addition to the ordinary decoded relation columns.
    pub fn emit_with<F>(&mut self, advance: F) -> Result<bool, CustomScanError>
    where
        F: FnOnce(&mut SlotColumns<'_>) -> crate::api::AmResult<bool>,
    {
        let slot = self.slot;
        // Slot datums are palloc'd into the scan node's per-tuple memory context,
        // which `ExecScan` resets at the start of each tuple cycle (after the
        // consumer has processed the prior row). Using the slot's own `tts_mcxt`
        // here would leak one row's by-reference payload per scanned row for the
        // lifetime of the query, since virtual-slot clear only frees a
        // materialized buffer, never the per-value pallocs.
        let target_ctx = self.per_tuple_memory_context;
        emit_into_slot(
            || unsafe {
                PgMemoryContexts::For(target_ctx).switch_to(|_| {
                    // SAFETY: the live slot descriptor is the sole source of
                    // width; `target_ctx` is the target for varlena palloc.
                    let mut cols = SlotColumns::new(slot, target_ctx);
                    advance(&mut cols)
                })
            },
            // SAFETY: slot is the live scan slot for this callback.
            || unsafe {
                pg_sys::ExecStoreVirtualTuple(slot);
            },
        )
    }

    /// Cooperative cancellation check (pgrx `check_for_interrupts!`).
    #[inline]
    pub fn check_for_interrupts(&self) {
        pgrx::pg_sys::check_for_interrupts!();
    }
}

/// Produced-row/end-of-scan decision shared by the slot-first emit path.
///
/// `advance` fills the slot and reports whether a row was produced; `store` is
/// invoked exactly once per produced row and never at end-of-scan. Splitting the
/// decision from the slot/context wiring lets it be exercised without a PG
/// backend.
fn emit_into_slot<A, S>(advance: A, store: S) -> Result<bool, CustomScanError>
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

    /// Rebuild predicate when true; else reopen cursor only.
    pub params_changed: bool,

    /// Query or modification-target purpose selected by the planner.
    pub purpose: ScanPurpose,

    pushed_exprs: &'a [*mut pg_sys::Expr],

    column_refs: &'a [crate::expr::split::ColumnRef],
    pushed_contracts: &'a [crate::expr::split::PushdownContract],
    resolved_params: &'a [crate::expr::nodes::PgParamValue],
    scan_relid: core::ffi::c_int,
    tuple_layout: &'a ScanTupleLayout,

    /// Scan relation handle.
    pub relation: RelationHandle<'a>,

    /// Executor snapshot handle.
    pub snapshot: SnapshotHandle<'a>,

    // --- framework-internal, no longer `pub` ---
    /// Executor state. Framework-internal.
    #[allow(dead_code)]
    estate: *mut pg_sys::EState,

    /// `econtext->ecxt_per_tuple_memory`.
    #[allow(dead_code)]
    per_tuple_memory_context: pg_sys::MemoryContext,

    _marker: PhantomData<&'a ()>,
}

impl<'a, P: LakebaseCustomScanProvider> ReScanContext<'a, P> {
    /// Internal constructor. Only the framework's `ReScanCustomScan`
    /// trampoline  builds [`ReScanContext`] values;
    /// providers receive them as a parameter.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        state: &'a mut P::State,
        params_changed: bool,
        purpose: ScanPurpose,
        pushed_exprs: &'a [*mut pg_sys::Expr],
        column_refs: &'a [crate::expr::split::ColumnRef],
        pushed_contracts: &'a [crate::expr::split::PushdownContract],
        resolved_params: &'a [crate::expr::nodes::PgParamValue],
        scan_relid: core::ffi::c_int,
        tuple_layout: &'a ScanTupleLayout,
        relation: RelationHandle<'a>,
        snapshot: SnapshotHandle<'a>,
        estate: *mut pg_sys::EState,
        per_tuple_memory_context: pg_sys::MemoryContext,
    ) -> Self {
        Self {
            state,
            params_changed,
            purpose,
            pushed_exprs,
            column_refs,
            pushed_contracts,
            resolved_params,
            scan_relid,
            tuple_layout,
            relation,
            snapshot,
            estate,
            per_tuple_memory_context,
            _marker: PhantomData,
        }
    }

    /// Whether any pushed predicates were recorded in `custom_exprs`.
    #[inline]
    pub fn has_pushed_predicates(&self) -> bool {
        !self.pushed_exprs.is_empty()
    }

    /// Number of pushed predicates in `custom_exprs` (metadata only; no PG pointers).
    #[inline]
    pub fn pushed_predicate_count(&self) -> usize {
        self.pushed_exprs.len()
    }

    /// Translate pushed PG expressions into provider-native predicates.
    pub fn translate_pushed_predicates<T, F>(
        &self,
        make_translator: F,
    ) -> Result<Vec<T::Predicate>, CustomScanError>
    where
        T: PgPredicateTranslator,
        T::Error: Send + Sync,
        F: FnMut(usize) -> T,
    {
        // SAFETY: `pushed_exprs` is the live `custom_exprs[pushed]` slice built
        // by the ReScanCustomScan trampoline for this scan.
        unsafe { self.pushed_predicate_inputs().translate(make_translator) }
    }

    fn pushed_predicate_inputs(&self) -> PushedPredicateInputs<'_> {
        PushedPredicateInputs::new(
            self.pushed_exprs,
            self.column_refs,
            self.pushed_contracts,
            self.resolved_params,
            self.scan_relid,
            self.tuple_layout,
        )
    }

    /// Resolved extern/exec params when `params_changed`; empty slice otherwise.
    #[inline]
    pub fn resolved_param_count(&self) -> usize {
        self.resolved_params.len()
    }

    /// Post-`rtoffset` scan relid (metadata only).
    #[inline]
    pub fn scan_relid(&self) -> core::ffi::c_int {
        self.scan_relid
    }
}

/// Context for [`LakebaseCustomScanProvider::end`].
pub struct EndContext<'a, P: LakebaseCustomScanProvider + ?Sized> {
    pub state: &'a mut P::State,
    pub relation: RelationHandle<'a>,
    #[allow(dead_code)]
    estate: *mut pg_sys::EState,
    _marker: PhantomData<&'a ()>,
}

impl<'a, P: LakebaseCustomScanProvider> EndContext<'a, P> {
    pub(crate) fn new(
        state: &'a mut P::State,
        relation: RelationHandle<'a>,
        estate: *mut pg_sys::EState,
    ) -> Self {
        Self {
            state,
            relation,
            estate,
            _marker: PhantomData,
        }
    }
}

// LakebaseCustomScanProvider trait
/// Lake backend provider trait: path classification, CustomPath emission, scan lifecycle.
///
/// Native predicates live in [`Self::State`] at runtime, not on the trait surface.
/// Distinguish path variants via [`PathVariant::kind`], not `param_info.is_some()`.
pub trait LakebaseCustomScanProvider: 'static {
    /// Unique provider name (EXPLAIN + registry).
    const NAME: &'static CStr;

    /// Provider tail of `custom_private`; framework owns the envelope ([`custom_private`](crate::customscan::custom_private)).
    type PrivateData: CustomScanPrivate;

    /// Per-scan runtime state inside `CustomScanStateWrapper`.
    type State;

    /// Whether this provider claims the relation (after framework path-stage gates).
    fn supports_relation(ctx: &RelPathContext) -> bool;

    /// Classify one parsed leaf predicate; framework handles composites and security gates.
    fn classify_predicate(
        ctx: &PlanTranslateContext,
        predicate: &PlanPredicate,
    ) -> QualPushdownDecision;

    /// Build one CustomPath for a framework-emitted variant; `None` declines.
    fn create_path(
        ctx: &RelPathContext,
        variant: &PathVariant<'_>,
        builder: CustomPathBuilder<Self>,
    ) -> Option<CustomPathPlan<Self>>
    where
        Self: Sized;

    /// Construct per-scan state before [`Self::begin`].
    fn create_state(ctx: CreateStateContext<Self>) -> Self::State;

    /// Open scan cursor; framework calls from BeginCustomScan.
    fn begin(ctx: BeginContext<'_, Self>) -> Result<(), CustomScanError>;

    /// Produce next row via [`NextSlotContext::emit_row`]; `Ok(false)` = EOF.
    fn next_slot(ctx: NextSlotContext<'_, Self>) -> Result<bool, CustomScanError>;

    /// Rescan: rebuild predicate when `params_changed`, else reopen cursor.
    fn rescan(ctx: ReScanContext<'_, Self>) -> Result<(), CustomScanError>;

    /// Close cursor and release provider-owned runtime resources.
    fn end(ctx: EndContext<'_, Self>) -> Result<(), CustomScanError>;

    /// Reparameterize `PrivateData` for appendrel child; default no-op.
    ///
    /// # Safety
    ///
    /// All pointers must be live planner-owned nodes for the same appendrel
    /// planning operation. Implementations must return a `List` allocated in a
    /// PostgreSQL memory context that outlives the planned path.
    #[allow(unused_variables)]
    unsafe fn reparameterize_private_data(
        root: *mut pg_sys::PlannerInfo,
        private: *mut pg_sys::List,
        child_rel: *mut pg_sys::RelOptInfo,
    ) -> *mut pg_sys::List {
        private
    }
}

/// PostgreSQL executor features accepted by one Custom ModifyTable provider.
///
/// Core validates these capabilities once when it initializes the wrapped
/// ModifyTable. Provider-specific policy therefore does not leak into the
/// generic executor or its C fork.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ModifyCapabilities {
    postgres_indexes: bool,
    speculative_insert: bool,
}

impl ModifyCapabilities {
    pub const NONE: Self = Self::new(false, false);
    /// Core maintains PostgreSQL index entries after provider INSERT/UPDATE.
    /// The provider must return a valid index-fetchable `tts_tid` for the new
    /// row and implement the corresponding TableAM index visibility contract.
    pub const POSTGRES_INDEXES: Self = Self::new(true, false);
    /// In addition to [`Self::POSTGRES_INDEXES`], the provider's TableAM
    /// implements PostgreSQL's speculative insertion callbacks.
    pub const POSTGRES_INDEXES_AND_SPECULATIVE_INSERT: Self = Self::new(true, true);

    const fn new(postgres_indexes: bool, speculative_insert: bool) -> Self {
        Self {
            postgres_indexes,
            speculative_insert,
        }
    }

    pub const fn postgres_indexes(self) -> bool {
        self.postgres_indexes
    }

    pub const fn speculative_insert(self) -> bool {
        self.speculative_insert
    }
}

/// Provider contract for the generic Custom ModifyTable framework.
///
/// This deliberately links an existing scan provider to one TableAM identity;
/// core owns PostgreSQL plan/executor mechanics while the provider owns scan
/// metadata extraction and the AM owns relation-local mutation state.
pub trait LakebaseCustomModifyProvider: LakebaseCustomScanProvider {
    type AccessMethod: TableAccessMethod;

    /// Unique PostgreSQL CustomScan method name for the outer ModifyTable
    /// wrapper. It must not equal [`LakebaseCustomScanProvider::NAME`] or any
    /// other registered modify provider name.
    const MODIFY_NAME: &'static CStr;

    const MODIFY_CAPABILITIES: ModifyCapabilities;

    /// Attach this provider's Modify-purpose scan to the stable relation state
    /// owned by the outer ModifyTable execution.
    fn bind_modify(ctx: ModifyBindContext<'_, Self>) -> Result<(), CustomScanError>
    where
        Self: Sized;

    /// Whether the relation can be the nominal target of the outer
    /// ModifyTable. The default matches a directly scannable relation;
    /// partition-aware providers override this to accept partitioned roots
    /// while [`LakebaseCustomScanProvider::supports_relation`] continues to
    /// accept only physical scan leaves.
    fn supports_modify_target(context: &RelPathContext) -> bool {
        Self::supports_relation(context)
    }

    /// Return the immutable storage context captured when a Modify-purpose
    /// scan opened. Core clones it only once when constructing the relation's
    /// AM modify state.
    fn modify_scan_context(
        state: &Self::State,
    ) -> Option<
        <<Self::AccessMethod as TableAccessMethod>::ModifyState as AmModifyState>::ScanContext,
    >;
}

#[cfg(test)]
mod emit_tests {
    use super::*;
    use crate::api::AmResult;
    use crate::batch::ScanBatchDriver;
    use crate::tuple::SlotColumns;

    /// Produces a fixed number of rows, then end-of-scan forever.
    struct FakeDriver {
        remaining: usize,
    }

    impl ScanBatchDriver for FakeDriver {
        fn next_into_slot(&mut self, out: &mut SlotColumns<'_>) -> AmResult<bool> {
            if self.remaining == 0 {
                return Ok(false);
            }
            self.remaining -= 1;
            out.set_datum(0, Some(pg_sys::Datum::from(1usize)));
            Ok(true)
        }
    }

    /// Owns the arrays a [`SlotColumns`] writes through; the backing vectors
    /// never reallocate after construction, so the raw pointers stay valid.
    struct HostSlot {
        slot: pg_sys::TupleTableSlot,
        tuple_desc: Box<pg_sys::TupleDescData>,
        values: Vec<pg_sys::Datum>,
        nulls: Vec<bool>,
    }

    impl HostSlot {
        fn new(natts: usize) -> Box<Self> {
            let mut tuple_desc: Box<pg_sys::TupleDescData> =
                Box::new(unsafe { std::mem::zeroed() });
            tuple_desc.natts = natts as i32;
            let mut boxed = Box::new(HostSlot {
                slot: unsafe { std::mem::zeroed() },
                tuple_desc,
                values: vec![pg_sys::Datum::from(0usize); natts],
                nulls: vec![true; natts],
            });
            boxed.slot.tts_values = boxed.values.as_mut_ptr();
            boxed.slot.tts_isnull = boxed.nulls.as_mut_ptr();
            boxed.slot.tts_tupleDescriptor = &mut *boxed.tuple_desc;
            boxed
        }

        fn columns(&mut self) -> SlotColumns<'_> {
            unsafe { SlotColumns::new(&mut self.slot, std::ptr::null_mut()) }
        }
    }

    #[test]
    fn store_runs_once_per_produced_row_and_eof_is_terminal() {
        let natts = 1;
        let mut driver = FakeDriver { remaining: 3 };
        let mut host = HostSlot::new(natts);
        let mut stores = 0usize;

        let mut produced = 0usize;
        loop {
            let found = emit_into_slot(
                || {
                    let mut cols = host.columns();
                    driver.next_into_slot(&mut cols)
                },
                || stores += 1,
            )
            .unwrap();
            if !found {
                break;
            }
            produced += 1;
        }

        assert_eq!(produced, 3);
        assert_eq!(stores, 3);

        // End-of-scan stays terminal and never takes the store path again.
        let again = emit_into_slot(
            || {
                let mut cols = host.columns();
                driver.next_into_slot(&mut cols)
            },
            || stores += 1,
        )
        .unwrap();
        assert!(!again);
        assert_eq!(stores, 3);
    }

    #[test]
    fn eof_with_no_rows_never_stores() {
        let natts = 1;
        let mut driver = FakeDriver { remaining: 0 };
        let mut host = HostSlot::new(natts);
        let mut stores = 0usize;

        let found = emit_into_slot(
            || {
                let mut cols = host.columns();
                driver.next_into_slot(&mut cols)
            },
            || stores += 1,
        )
        .unwrap();

        assert!(!found);
        assert_eq!(stores, 0);
    }
}
