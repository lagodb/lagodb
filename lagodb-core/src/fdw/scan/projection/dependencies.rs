//! Planner-only semantic column dependencies and scan-tuple coverage.

use core::ffi::c_void;

use pgrx::{pg_guard, pg_sys};

use crate::expr::inspect::{
    ExprVarRef, RelationExprAnalyzer, RelationExprUsage, RelationScope,
};
use crate::expr::relation::RelationVarsByAttno;

use super::super::super::system_column::SystemColumnRequirement;
use super::super::pathkeys::ForeignPathKeys;

pub(super) struct ProjectionAnalysis {
    pub(super) vars_by_attno: RelationVarsByAttno,
    pub(super) executor_vars_by_attno: RelationVarsByAttno,
    pub(super) direct_outputs: Vec<DirectOutput>,
    pub(super) can_narrow: bool,
    pub(super) provider_requires_all_columns: bool,
    pub(super) executor_requires_all_columns: bool,
    pub(super) system_columns: Vec<SystemColumnRequirement>,
}

pub(super) struct DirectOutput {
    pub(super) attno: pg_sys::AttrNumber,
    pub(super) var: *mut pg_sys::Var,
    pub(super) resjunk: bool,
}

#[derive(Clone, Copy)]
enum DependencyScope {
    Executor,
    Provider,
}

impl Default for ProjectionAnalysis {
    fn default() -> Self {
        Self {
            vars_by_attno: RelationVarsByAttno::default(),
            executor_vars_by_attno: RelationVarsByAttno::default(),
            direct_outputs: Vec::new(),
            can_narrow: true,
            provider_requires_all_columns: false,
            executor_requires_all_columns: false,
            system_columns: Vec::new(),
        }
    }
}

impl ProjectionAnalysis {
    /// Collect finalized semantic dependencies, then determine whether
    /// PostgreSQL's plan targetlist can be backed by a narrowed scan tuple.
    ///
    /// # Safety
    ///
    /// Every non-NULL list argument must be a live PostgreSQL planner list for
    /// the current `GetForeignPlan` callback. Every pathkey expression must
    /// remain live for this call.
    pub(super) unsafe fn analyze(
        scan_relid: pg_sys::Index,
        targetlist: *mut pg_sys::List,
        path_target_exprs: *mut pg_sys::List,
        pathkeys: &ForeignPathKeys,
        residual_quals: *mut pg_sys::List,
        fdw_exprs: *mut pg_sys::List,
        recheck_quals: *mut pg_sys::List,
    ) -> Self {
        let analyzer = RelationExprAnalyzer::new(RelationScope::exact(scan_relid));
        let mut analysis = Self::default();

        // SAFETY: the caller's contract establishes every planner list and
        // expression for this synchronous analysis.
        unsafe {
            analysis.inspect_expr_list(
                path_target_exprs,
                &analyzer,
                DependencyScope::Executor,
            );
            for expr in pathkeys.expressions() {
                analysis.inspect_expr(
                    expr,
                    &analyzer,
                    // PostgreSQL selects the local sort member from the
                    // relation target through the EC. The provider-selected
                    // member need not be written unless another semantic
                    // executor dependency also requires it.
                    DependencyScope::Provider,
                );
            }
            analysis.inspect_expr_list(
                residual_quals,
                &analyzer,
                DependencyScope::Executor,
            );
            analysis.inspect_expr_list(
                fdw_exprs,
                &analyzer,
                DependencyScope::Provider,
            );
            analysis.inspect_expr_list(
                recheck_quals,
                &analyzer,
                DependencyScope::Provider,
            );
            // Exact pushed predicates are retained as EPQ recheck quals. They
            // are provider read dependencies, not normal-row output. Relation
            // shape keeps their base Vars addressable by setrefs without
            // adding them to the executor write set.
            if !recheck_quals.is_null() {
                analysis.can_narrow = false;
            }
            analysis.inspect_plan_targetlist(targetlist, &analyzer);
        }

        analysis
    }

    fn absorb(&mut self, usage: RelationExprUsage, scope: DependencyScope) {
        if usage.has_whole_row() {
            self.provider_requires_all_columns = true;
            if matches!(scope, DependencyScope::Executor) {
                self.executor_requires_all_columns = true;
                self.can_narrow = false;
            }
        }
        if !usage.system_attnos().is_empty() {
            self.system_columns.extend(
                usage
                    .system_attnos()
                    .iter()
                    .copied()
                    .map(SystemColumnRequirement::from_attno),
            );
            self.can_narrow = false;
        }
        for var in usage.user_vars() {
            if let Some(existing) = self.vars_by_attno.get(var.attno) {
                // SAFETY: the analyzer returns live planner Vars, and the
                // attno index stores those same planner-owned nodes.
                let same_nullingrels = unsafe {
                    pg_sys::bms_equal(
                        existing.as_ref().varnullingrels,
                        var.raw.as_ref().varnullingrels,
                    )
                };
                if !same_nullingrels {
                    self.can_narrow = false;
                }
            } else {
                self.vars_by_attno.insert(var.raw);
            }
            if matches!(scope, DependencyScope::Executor) {
                if let Some(existing) = self.executor_vars_by_attno.get(var.attno) {
                    // SAFETY: both Vars are live for the current planner call.
                    let same_nullingrels = unsafe {
                        pg_sys::bms_equal(
                            existing.as_ref().varnullingrels,
                            var.raw.as_ref().varnullingrels,
                        )
                    };
                    if !same_nullingrels {
                        self.can_narrow = false;
                    }
                } else {
                    self.executor_vars_by_attno.insert(var.raw);
                }
            }
        }
    }

    /// Whether a plan-targetlist `Var` has an identical semantic executor
    /// dependency that can back it in a narrowed `fdw_scan_tlist`.
    fn executor_covers(&self, candidate: ExprVarRef) -> bool {
        let Some(existing) = self.executor_vars_by_attno.get(candidate.attno) else {
            return false;
        };
        // SAFETY: both nodes are planner-owned live Vars. `setrefs.c` uses
        // node equality when replacing scan references, so the planning gate
        // must use the same identity rather than attribute number alone.
        unsafe {
            pg_sys::equal(
                existing.as_ptr().cast::<c_void>(),
                candidate.raw.as_ptr().cast::<c_void>(),
            )
        }
    }

    /// Inspect the plan targetlist only as a scan-tuple/setrefs carrier. Its
    /// extra physical Vars never become semantic provider dependencies.
    ///
    /// # Safety
    ///
    /// `targetlist` must be NIL or a live planner TargetEntry list, and every
    /// entry expression must remain live for this call.
    unsafe fn inspect_plan_targetlist(
        &mut self,
        targetlist: *mut pg_sys::List,
        analyzer: &RelationExprAnalyzer,
    ) {
        if targetlist.is_null() {
            return;
        }
        let length = unsafe { pg_sys::list_length(targetlist) };
        for index in 0..length {
            let entry = unsafe { pg_sys::list_nth(targetlist, index) }
                as *mut pg_sys::TargetEntry;
            let expr = unsafe { (*entry).expr };
            if unsafe { contains_placeholder(expr.cast()) } {
                self.can_narrow = false;
                continue;
            }

            let usage = unsafe { analyzer.collect_expr(expr) };
            if usage.has_whole_row()
                || !usage.system_attnos().is_empty()
                || usage
                    .user_vars()
                    .iter()
                    .copied()
                    .any(|var| !self.executor_covers(var))
            {
                // PG may supply every relation Var as a physical targetlist.
                // Extra carrier Vars keep relation shape but do not expand
                // either semantic dependency set.
                self.can_narrow = false;
            }

            if unsafe { (*expr).type_ } == pg_sys::NodeTag::T_Var
                && usage.user_vars().len() == 1
                && usage.system_attnos().is_empty()
                && !usage.has_whole_row()
                && self.executor_covers(usage.user_vars()[0])
            {
                let var = expr.cast::<pg_sys::Var>();
                self.direct_outputs.push(DirectOutput {
                    attno: unsafe { (*var).varattno },
                    var,
                    resjunk: unsafe { (*entry).resjunk },
                });
            }
        }
    }

    /// # Safety
    ///
    /// `list` must be NIL or a live planner expression list whose nodes remain
    /// valid for this call.
    unsafe fn inspect_expr_list(
        &mut self,
        list: *mut pg_sys::List,
        analyzer: &RelationExprAnalyzer,
        scope: DependencyScope,
    ) {
        if list.is_null() {
            return;
        }
        let length = unsafe { pg_sys::list_length(list) };
        for index in 0..length {
            let expr = unsafe { pg_sys::list_nth(list, index) } as *mut pg_sys::Expr;
            unsafe { self.inspect_expr(expr, analyzer, scope) };
        }
    }

    /// # Safety
    ///
    /// `expr` must be a live planner expression for the relation scope owned by
    /// `analyzer` and must remain valid for this call.
    unsafe fn inspect_expr(
        &mut self,
        expr: *mut pg_sys::Expr,
        analyzer: &RelationExprAnalyzer,
        scope: DependencyScope,
    ) {
        if matches!(scope, DependencyScope::Executor)
            && unsafe { contains_placeholder(expr.cast()) }
        {
            self.can_narrow = false;
        }
        let usage = unsafe { analyzer.collect_expr(expr) };
        self.absorb(usage, scope);
    }
}

/// Return true when an expression tree contains a PlaceholderVar. The planner
/// falls back to relation shape because its setrefs contract is not a plain
/// base-Var map.
///
/// # Safety
///
/// `node` must be NULL or a live planner expression node whose tree remains
/// valid while PostgreSQL invokes the walker callback.
unsafe fn contains_placeholder(node: *mut pg_sys::Node) -> bool {
    let mut found = false;
    unsafe {
        pg_sys::expression_tree_walker(
            node,
            Some(placeholder_walker),
            (&mut found as *mut bool).cast(),
        );
    }
    found
}

#[pg_guard]
/// # Safety
///
/// PostgreSQL invokes this callback synchronously with a live expression node
/// and the non-NULL context pointer supplied by `contains_placeholder` or a
/// recursive call from PostgreSQL's expression walker.
unsafe extern "C-unwind" fn placeholder_walker(
    node: *mut pg_sys::Node,
    context: *mut c_void,
) -> bool {
    if node.is_null() {
        return false;
    }
    let found = unsafe { &mut *(context.cast::<bool>()) };
    if unsafe { (*node).type_ } == pg_sys::NodeTag::T_PlaceHolderVar {
        *found = true;
        return true;
    }
    unsafe { pg_sys::expression_tree_walker(node, Some(placeholder_walker), context) }
}
