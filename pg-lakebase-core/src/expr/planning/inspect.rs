//! Planning-time expression inspection built on PostgreSQL's expression walker.
//!
//! This module is about dependency analysis, not pushdown semantics.  It keeps
//! PG expression-tree FFI at the core layer and returns owned, relation-aware
//! facts to CustomScan planning/execution code.

use core::ffi::c_void;
use core::ptr::NonNull;

use pgrx::pg_guard;
use pgrx::pg_sys;

/// Return true when a subtree contains volatile functions or SubPlans.
///
/// # Safety
///
/// `expr` must be NULL or a live PostgreSQL expression tree in the current
/// backend memory context.
pub(crate) unsafe fn subtree_is_unsafe_to_push(expr: *mut pg_sys::Expr) -> bool {
    if expr.is_null() {
        return false;
    }
    if unsafe { pg_sys::contain_volatile_functions(expr.cast::<pg_sys::Node>()) } {
        return true;
    }
    unsafe { pg_sys::contain_subplans(expr.cast::<pg_sys::Node>()) }
}

/// Planner relation scope used to decide whether a `Var` belongs to the scan.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RelationScope {
    /// Exact base-relation RTI (`RelOptInfo.relid` / `Var.varno`).
    Exact(pg_sys::Index),

    /// Planner relids bitmap (`RelOptInfo.relids`) for path-stage gate checks.
    Relids(*mut pg_sys::Bitmapset),
}

impl RelationScope {
    #[inline]
    pub(crate) fn exact(relid: pg_sys::Index) -> Self {
        Self::Exact(relid)
    }

    #[inline]
    pub(crate) fn relids(relids: *mut pg_sys::Bitmapset) -> Self {
        Self::Relids(relids)
    }

    #[inline]
    unsafe fn contains_varno(self, varno: pg_sys::Index) -> bool {
        match self {
            Self::Exact(relid) => varno == relid,
            Self::Relids(relids) => {
                !relids.is_null()
                    && unsafe { pg_sys::bms_is_member(varno as i32, relids) }
            }
        }
    }

    #[inline]
    unsafe fn overlaps_relids(self, relids: *const pg_sys::Bitmapset) -> bool {
        if relids.is_null() {
            return false;
        }
        match self {
            Self::Exact(relid) => unsafe {
                pg_sys::bms_is_member(relid as i32, relids)
            },
            Self::Relids(scope) => {
                !scope.is_null() && unsafe { pg_sys::bms_overlap(scope, relids) }
            }
        }
    }
}

/// One user-column `Var` belonging to the requested relation scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ExprVarRef {
    pub(crate) attno: pg_sys::AttrNumber,
    pub(crate) atttypid: pg_sys::Oid,
    pub(crate) attcollation: pg_sys::Oid,
    /// Planner-owned source node. Consumers that manufacture a scan tlist
    /// copy this node so PostgreSQL's `varnullingrels` identity is preserved.
    pub(crate) raw: NonNull<pg_sys::Var>,
}

/// Owned expression-usage facts for one relation scope.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct RelationExprUsage {
    user_vars: Vec<ExprVarRef>,
    system_attnos: Vec<pg_sys::AttrNumber>,
    has_whole_row: bool,
}

impl RelationExprUsage {
    #[inline]
    pub(crate) fn user_vars(&self) -> &[ExprVarRef] {
        &self.user_vars
    }

    #[inline]
    pub(crate) fn has_whole_row(&self) -> bool {
        self.has_whole_row
    }

    #[inline]
    pub(crate) fn system_attnos(&self) -> &[pg_sys::AttrNumber] {
        &self.system_attnos
    }

    pub(crate) fn extend(&mut self, other: RelationExprUsage) {
        self.user_vars.extend(other.user_vars);
        self.system_attnos.extend(other.system_attnos);
        self.has_whole_row |= other.has_whole_row;
    }

    fn record_var(&mut self, var: *mut pg_sys::Var) {
        let attno = unsafe { (*var).varattno };
        if attno > 0 {
            // `relation_var_walker` calls this only for a non-null T_Var node.
            let raw = unsafe { NonNull::new_unchecked(var) };
            self.user_vars.push(ExprVarRef {
                attno,
                atttypid: unsafe { (*var).vartype },
                attcollation: unsafe { (*var).varcollid },
                raw,
            });
        } else if attno == 0 {
            self.has_whole_row = true;
        } else {
            // `tableoid` does not require a storage column, but a projected
            // user-column custom tuple cannot represent it. Keep it in the
            // usage contract so tuple-layout planning falls back atomically to
            // the relation-shaped slot.
            self.system_attnos.push(attno);
        }
    }
}

/// Relation-aware expression inspector.
pub(crate) struct RelationExprAnalyzer {
    scope: RelationScope,
}

impl RelationExprAnalyzer {
    #[inline]
    pub(crate) fn new(scope: RelationScope) -> Self {
        Self { scope }
    }

    /// Determine whether PostgreSQL considers an expression dependent on the
    /// analyzer's relation scope.
    ///
    /// This delegates to PostgreSQL's `pull_varnos` rather than only walking
    /// direct `Var` nodes.  In particular, PG accounts for correlated query
    /// levels, `PlaceHolderVar` evaluation relids, nulling rels, and other
    /// planner representations that a local expression walker cannot
    /// reproduce safely.
    ///
    /// # Safety
    ///
    /// `root` must be the live `PlannerInfo` for `expr`, and `expr` must be a
    /// live planner expression node owned by that planning invocation.
    pub(crate) unsafe fn depends_on_relation(
        &self,
        root: *mut pg_sys::PlannerInfo,
        expr: *mut pg_sys::Expr,
    ) -> bool {
        if root.is_null() || expr.is_null() {
            return true;
        }
        let relids = unsafe { pg_sys::pull_varnos(root, expr.cast()) };
        if relids.is_null() {
            return false;
        }
        let depends = unsafe { self.scope.overlaps_relids(relids) };
        unsafe { pg_sys::bms_free(relids) };
        depends
    }

    /// Collect relation-local `Var` usage from a single expression node.
    ///
    /// # Safety
    ///
    /// `expr` is NULL or a planner/executor-owned expression node live for the
    /// duration of the call.
    pub(crate) unsafe fn collect_expr(
        &self,
        expr: *mut pg_sys::Expr,
    ) -> RelationExprUsage {
        unsafe { self.collect_node(expr.cast::<pg_sys::Node>()) }
    }

    /// Collect usage from a `List<Expr>`.
    ///
    /// # Safety
    ///
    /// `list` is NULL or a valid PostgreSQL `List` whose cells are expression
    /// nodes.
    pub(crate) unsafe fn collect_expr_list(
        &self,
        list: *mut pg_sys::List,
    ) -> RelationExprUsage {
        let mut usage = RelationExprUsage::default();
        if list.is_null() {
            return usage;
        }
        let len = unsafe { pg_sys::list_length(list) };
        for i in 0..len {
            let expr = unsafe { pg_sys::list_nth(list, i) } as *mut pg_sys::Expr;
            usage.extend(unsafe { self.collect_expr(expr) });
        }
        usage
    }

    /// Collect usage from a `List<RestrictInfo>`.
    ///
    /// # Safety
    ///
    /// `list` is NULL or a valid PostgreSQL list of `RestrictInfo*`.
    pub(crate) unsafe fn collect_restrictinfo_list(
        &self,
        list: *mut pg_sys::List,
    ) -> RelationExprUsage {
        let mut usage = RelationExprUsage::default();
        if list.is_null() {
            return usage;
        }
        let len = unsafe { pg_sys::list_length(list) };
        for i in 0..len {
            let rinfo =
                unsafe { pg_sys::list_nth(list, i) } as *mut pg_sys::RestrictInfo;
            if rinfo.is_null() {
                continue;
            }
            let clause = unsafe { (*rinfo).clause };
            usage.extend(unsafe { self.collect_expr(clause) });
        }
        usage
    }

    unsafe fn collect_node(&self, node: *mut pg_sys::Node) -> RelationExprUsage {
        let mut state = RelationVarWalkerState {
            scope: self.scope,
            usage: RelationExprUsage::default(),
        };
        unsafe {
            relation_var_walker(
                node,
                (&mut state as *mut RelationVarWalkerState).cast(),
            );
        }
        state.usage
    }
}

struct RelationVarWalkerState {
    scope: RelationScope,
    usage: RelationExprUsage,
}

#[pg_guard]
unsafe extern "C-unwind" fn relation_var_walker(
    node: *mut pg_sys::Node,
    context: *mut c_void,
) -> bool {
    if node.is_null() {
        return false;
    }
    let state = unsafe { &mut *(context.cast::<RelationVarWalkerState>()) };
    let tag = unsafe { (*node).type_ };
    match tag {
        pg_sys::NodeTag::T_Var => {
            let var = node.cast::<pg_sys::Var>();
            let varlevelsup = unsafe { (*var).varlevelsup };
            let varno = unsafe { (*var).varno } as pg_sys::Index;
            if varlevelsup == 0 && unsafe { state.scope.contains_varno(varno) } {
                state.usage.record_var(var);
            }
            false
        }
        pg_sys::NodeTag::T_RestrictInfo => {
            let rinfo = node.cast::<pg_sys::RestrictInfo>();
            let clause = unsafe { (*rinfo).clause }.cast::<pg_sys::Node>();
            unsafe { relation_var_walker(clause, context) }
        }
        _ => unsafe {
            pg_sys::expression_tree_walker(node, Some(relation_var_walker), context)
        },
    }
}
