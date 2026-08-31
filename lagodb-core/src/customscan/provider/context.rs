//! Typed planner contexts for provider matching and path construction.

use core::ffi::c_int;
use core::marker::PhantomData;
use core::ptr::{self, NonNull};

use pgrx::pg_sys;

/// Planner context available while deciding whether a provider owns a relation.
///
/// This context intentionally contains only the relation's range-table entry.
/// Planner-wide and baserel fields are available through [`PathContext`] once
/// PostgreSQL has entered path construction.
pub struct RelationContext<'a> {
    rte: NonNull<pg_sys::RangeTblEntry>,
    _marker: PhantomData<&'a pg_sys::RangeTblEntry>,
}

impl<'a> RelationContext<'a> {
    /// Construct a relation context from a live planner-owned RTE reference.
    #[inline]
    pub fn from_ref(rte: &'a pg_sys::RangeTblEntry) -> Self {
        Self {
            rte: NonNull::from(rte),
            _marker: PhantomData,
        }
    }

    /// The relation's `pg_class` OID (`rte->relid`).
    #[inline]
    pub fn rel_oid(&self) -> pg_sys::Oid {
        // SAFETY: `rte` is a live planner-owned node per `from_ref`.
        unsafe { self.rte.as_ref().relid }
    }

    /// The range-table entry kind (`rte->rtekind`).
    #[inline]
    pub fn rtekind(&self) -> pg_sys::RTEKind::Type {
        unsafe { self.rte.as_ref().rtekind }
    }

    /// The relation kind (`rte->relkind`) as `u8`.
    #[inline]
    pub fn relkind(&self) -> u8 {
        unsafe { self.rte.as_ref().relkind as u8 }
    }

    /// The relation's table access method OID (`pg_class.relam`).
    ///
    /// Returns [`pg_sys::Oid::INVALID`] for relations without a TableAM.
    #[inline]
    pub fn access_method_oid(&self) -> pg_sys::Oid {
        // SAFETY: `get_rel_relam` accepts the valid relation OID returned by
        // `rel_oid` and resolves it through PostgreSQL's syscache.
        unsafe { pg_sys::get_rel_relam(self.rel_oid()) }
    }

    /// The relation's tablespace OID (`pg_class.reltablespace`).
    ///
    /// This can be [`pg_sys::Oid::INVALID`] for the database default
    /// tablespace.
    #[inline]
    pub fn tablespace_oid(&self) -> pg_sys::Oid {
        // SAFETY: `get_rel_tablespace` accepts the valid relation OID returned
        // by `rel_oid` and resolves it through PostgreSQL's syscache.
        unsafe { pg_sys::get_rel_tablespace(self.rel_oid()) }
    }
}

/// Planner context available while constructing a path for a base relation.
///
/// Unlike [`RelationContext`], this type guarantees that the planner root and
/// baserel pointers are present. Provider methods that need planner estimates
/// or path-target metadata receive this type instead of a nullable context.
pub struct PathContext<'a> {
    relation: RelationContext<'a>,
    root: NonNull<pg_sys::PlannerInfo>,
    baserel: NonNull<pg_sys::RelOptInfo>,
    _marker: PhantomData<(&'a pg_sys::PlannerInfo, &'a pg_sys::RelOptInfo)>,
}

impl<'a> PathContext<'a> {
    /// Construct a full path context from one planning invocation.
    #[inline]
    pub(crate) fn from_refs(
        rte: &'a pg_sys::RangeTblEntry,
        root: &'a pg_sys::PlannerInfo,
        baserel: &'a pg_sys::RelOptInfo,
    ) -> Self {
        let relation = RelationContext::from_ref(rte);
        Self {
            relation,
            root: NonNull::from(root),
            baserel: NonNull::from(baserel),
            _marker: PhantomData,
        }
    }

    /// The relation-only view used by provider matching.
    #[inline]
    pub fn relation(&self) -> &RelationContext<'a> {
        &self.relation
    }

    /// The relation's `pg_class` OID (`rte->relid`).
    #[inline]
    pub fn rel_oid(&self) -> pg_sys::Oid {
        self.relation.rel_oid()
    }

    /// The range-table entry kind (`rte->rtekind`).
    #[inline]
    pub fn rtekind(&self) -> pg_sys::RTEKind::Type {
        self.relation.rtekind()
    }

    /// The relation kind (`rte->relkind`) as `u8`.
    #[inline]
    pub fn relkind(&self) -> u8 {
        self.relation.relkind()
    }

    /// The relation's table access method OID (`pg_class.relam`).
    #[inline]
    pub fn access_method_oid(&self) -> pg_sys::Oid {
        self.relation.access_method_oid()
    }

    /// The relation's tablespace OID (`pg_class.reltablespace`).
    #[inline]
    pub fn tablespace_oid(&self) -> pg_sys::Oid {
        self.relation.tablespace_oid()
    }

    /// `baserel->pages` (unpruned baseline).
    #[inline]
    pub fn baserel_pages(&self) -> f64 {
        // SAFETY: `baserel` is a live, non-NULL planner-owned node per
        // `from_refs`; `pages` is a plain field.
        unsafe { self.baserel.as_ref().pages as f64 }
    }

    /// `baserel->tuples` (unpruned baseline).
    #[inline]
    pub fn baserel_tuples(&self) -> f64 {
        unsafe { self.baserel.as_ref().tuples }
    }

    /// Whether the base-relation path target contains no expressions. Such a
    /// path produces rows for an upper node without exposing a relation
    /// column, so a provider can offer projection pruning even when it has no
    /// pushed filter.
    #[inline]
    pub fn has_empty_path_target(&self) -> bool {
        let target = unsafe { self.baserel.as_ref().reltarget };
        unsafe { pg_sys::list_length((*target).exprs) == 0 }
    }

    /// Whether the base relation PathTarget requests a whole-row value.
    pub fn modify_requests_wholerow(&self) -> bool {
        // SAFETY: `baserel`, `root`, and the nodes reachable from them are live
        // planner-owned data for this path-construction invocation. The
        // traversal below preserves PostgreSQL's NULL-list and node-tag checks.
        let baserel = unsafe { self.baserel.as_ref() };
        let target = baserel.reltarget;
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
                (*var).varno == baserel.relid as i32
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
        let parse = unsafe { self.root.as_ref().parse };
        let query_targetlist = unsafe { (*parse).targetList };
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

    /// Combined selectivity of the given qual exprs via
    /// `clauselist_selectivity`; returns `1.0` when empty.
    pub(crate) fn clauselist_selectivity_for_exprs(
        &self,
        exprs: &[*mut pg_sys::Expr],
    ) -> f64 {
        if exprs.is_empty() {
            return 1.0;
        }

        // SAFETY: `exprs` and planner nodes are live for this path callback;
        // PostgreSQL owns the list allocations and the selectivity call.
        let mut clauses: *mut pg_sys::List = ptr::null_mut();
        for &expr in exprs {
            clauses = unsafe { pg_sys::lappend(clauses, expr.cast()) };
        }

        let sel = unsafe {
            pg_sys::clauselist_selectivity(
                self.root.as_ptr(),
                clauses,
                self.baserel.as_ref().relid as c_int,
                pg_sys::JoinType::JOIN_INNER,
                ptr::null_mut(),
            )
        };

        sel.clamp(0.0, 1.0)
    }
}
