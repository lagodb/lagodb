//! Plan-stage relation metadata used while building pushed expression metadata.

use core::ffi::c_int;

use pgrx::pg_sys;

use crate::expr::column::ColumnNameResolver;
use crate::expr::contract::ColumnRef;
use crate::expr::inspect::{RelationExprAnalyzer, RelationScope};
use crate::expr::predicate::PlanPredicateContext;

#[derive(Debug, Clone, Copy)]
pub struct PlanRelationResolver {
    root: *mut pg_sys::PlannerInfo,
}

impl PlanRelationResolver {
    #[inline]
    pub fn new(root: *mut pg_sys::PlannerInfo) -> Self {
        Self { root }
    }

    /// `pg_class` OID for a scan RTI, mirroring PostgreSQL's
    /// `planner_rt_fetch`.
    ///
    /// # Safety
    ///
    /// `self.root` must point to a live `PlannerInfo`, `relid` must be a valid
    /// one-based RTI, and the corresponding `RangeTblEntry` must be non-NULL.
    /// When `root.simple_rte_array` is NULL, `root.parse->rtable` must be live.
    pub unsafe fn rel_oid(self, relid: pg_sys::Index) -> pg_sys::Oid {
        let simple_rte_array = unsafe { (*self.root).simple_rte_array };
        let rte = if simple_rte_array.is_null() {
            let parse = unsafe { (*self.root).parse };
            let rtable = unsafe { (*parse).rtable };
            unsafe { pg_sys::list_nth(rtable, (relid - 1) as c_int) }
                .cast::<pg_sys::RangeTblEntry>()
        } else {
            unsafe { *simple_rte_array.add(relid as usize) }
        };
        unsafe { (*rte).relid }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PlanScanRelation {
    relid: pg_sys::Index,
    scan_relid: c_int,
    rel_oid: pg_sys::Oid,
}

impl PlanScanRelation {
    /// # Safety
    ///
    /// Valid `PlannerInfo` and `RelOptInfo` in the planner context.
    pub(crate) unsafe fn new(
        root: *mut pg_sys::PlannerInfo,
        baserel: *mut pg_sys::RelOptInfo,
    ) -> Self {
        let relid = unsafe { (*baserel).relid };
        Self {
            relid,
            scan_relid: relid as c_int,
            rel_oid: unsafe { PlanRelationResolver::new(root).rel_oid(relid) },
        }
    }

    #[inline]
    pub(crate) fn predicate_context(self) -> PlanPredicateContext {
        PlanPredicateContext {
            rel_oid: self.rel_oid,
            scan_relid: self.scan_relid,
        }
    }
}

pub(crate) struct ColumnRefCollector {
    relation: PlanScanRelation,
    analyzer: RelationExprAnalyzer,
    names: ColumnNameResolver,
}

impl ColumnRefCollector {
    pub(crate) fn new(relation: PlanScanRelation) -> Self {
        Self {
            relation,
            analyzer: RelationExprAnalyzer::new(RelationScope::exact(relation.relid)),
            names: ColumnNameResolver::new(relation.rel_oid),
        }
    }

    /// # Safety
    ///
    /// Every pushed expression pointer must be live in the planner context.
    pub(crate) unsafe fn collect_exprs<I>(&self, pushed_exprs: I) -> Vec<ColumnRef>
    where
        I: IntoIterator<Item = *mut pg_sys::Expr>,
    {
        let mut column_refs = Vec::new();
        for (expr_index, expr) in pushed_exprs.into_iter().enumerate() {
            let usage = unsafe { self.analyzer.collect_expr(expr) };
            let mut seen_attnos: Vec<pg_sys::AttrNumber> = Vec::new();
            for var in usage.user_vars() {
                if seen_attnos.contains(&var.attno) {
                    continue;
                }
                seen_attnos.push(var.attno);
                column_refs.push(ColumnRef {
                    expr_index,
                    rel_oid: self.relation.rel_oid,
                    attno: var.attno,
                    atttypid: var.atttypid,
                    attcollation: var.attcollation,
                    name: self.names.resolve(var.attno),
                });
            }
        }
        column_refs
    }
}
