//! Planner relation metadata used while building pushed expression metadata.

use pgrx::pg_sys;

use crate::expr::inspect::{RelationExprAnalyzer, RelationScope};
use crate::expr::predicate::PlanPredicateContext;

/// Plan-time metadata for one scan-relation user column in one pushed expr.
///
/// No `varno`: `set_customscan_references` renumbers `Var` in `custom_exprs` but not
/// `custom_private`; runtime uses `Var.varno == scan.scanrelid` plus `(expr_index, attno)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    pub expr_index: usize,
    pub rel_oid: pg_sys::Oid,
    pub attno: pg_sys::AttrNumber,
    pub atttypid: pg_sys::Oid,
    pub attcollation: pg_sys::Oid,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct PlanRelationResolver {
    root: *mut pg_sys::PlannerInfo,
}

impl PlanRelationResolver {
    #[inline]
    pub fn new(root: *mut pg_sys::PlannerInfo) -> Self {
        Self { root }
    }

    /// `pg_class` OID for scan RTI via `list_nth(rtable, relid - 1)` (plan-time `rt_fetch`).
    ///
    /// # Safety
    ///
    /// Valid `PlannerInfo` with live `parse->rtable`.
    pub unsafe fn rel_oid(self, relid: pg_sys::Index) -> pg_sys::Oid {
        if self.root.is_null() {
            return pg_sys::Oid::INVALID;
        }
        let parse = unsafe { (*self.root).parse };
        if parse.is_null() {
            return pg_sys::Oid::INVALID;
        }
        let rtable = unsafe { (*parse).rtable };
        if rtable.is_null() {
            return pg_sys::Oid::INVALID;
        }
        let len = unsafe { pg_sys::list_length(rtable) };
        // RTI is 1-based, list index is 0-based.
        if relid == 0 || (relid as core::ffi::c_int) > len {
            return pg_sys::Oid::INVALID;
        }
        let idx = (relid - 1) as core::ffi::c_int;
        let rte =
            unsafe { pg_sys::list_nth(rtable, idx) } as *mut pg_sys::RangeTblEntry;
        if rte.is_null() {
            return pg_sys::Oid::INVALID;
        }
        unsafe { (*rte).relid }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PlanScanRelation {
    relid: pg_sys::Index,
    scan_relid: core::ffi::c_int,
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
            scan_relid: relid as core::ffi::c_int,
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

#[derive(Debug, Clone, Copy)]
pub struct ColumnNameResolver {
    rel_oid: pg_sys::Oid,
}

impl ColumnNameResolver {
    #[inline]
    pub fn new(rel_oid: pg_sys::Oid) -> Self {
        Self { rel_oid }
    }

    /// `(rel_oid, attno) -> attname` for plan-time `ColumnRef::name`.
    pub fn resolve(self, attno: pg_sys::AttrNumber) -> Option<String> {
        self.try_resolve(attno).ok().flatten()
    }

    /// Checked variant of [`Self::resolve`] for callers that need to preserve
    /// invalid-UTF8 diagnostics.
    pub fn try_resolve(
        self,
        attno: pg_sys::AttrNumber,
    ) -> Result<Option<String>, core::str::Utf8Error> {
        if attno <= 0 {
            return Ok(None);
        }
        // SAFETY: `get_attname` tolerates any OID and returns NULL for a missing
        // row when `missing_ok = true`. It returns a palloc'd cstring in the
        // current memory context; we copy it before local cleanup so callers do
        // not depend on that context's lifetime.
        let raw = unsafe {
            pg_sys::get_attname(self.rel_oid, attno, /*missing_ok=*/ true)
        };
        if raw.is_null() {
            return Ok(None);
        }
        let cstr = unsafe { core::ffi::CStr::from_ptr(raw) };
        let name = cstr.to_str().map(|s| Some(s.to_owned()));
        unsafe { pg_sys::pfree(raw as *mut core::ffi::c_void) };
        name
    }
}
