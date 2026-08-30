//! Base-scan tuple planning and planner-owned scan layout analysis.

use core::ffi::{c_char, c_int, c_void};
use core::ptr;
use core::ptr::NonNull;

use pgrx::pg_sys;

use crate::customscan::plan_data::tuple_layout::NeededColumns;
use crate::customscan::plan_data::tuple_layout::ScanTupleLayout;
use crate::expr::inspect::{RelationExprAnalyzer, RelationExprUsage, RelationScope};
use crate::expr::relation::RelationVarsByAttno;

/// Output of [`BaseScanTuplePlanner`].
pub(crate) struct PlannedScanTuple {
    pub(crate) custom_scan_tlist: *mut pg_sys::List,
    pub(crate) layout: ScanTupleLayout,
}

impl PlannedScanTuple {
    pub(crate) fn relation() -> Self {
        Self {
            custom_scan_tlist: ptr::null_mut(),
            layout: ScanTupleLayout::relation(),
        }
    }

    fn relation_with_storage_hint(attnos: Option<Vec<pg_sys::AttrNumber>>) -> Self {
        Self {
            custom_scan_tlist: ptr::null_mut(),
            layout: ScanTupleLayout::relation_with_storage_hint(attnos),
        }
    }
}

/// Cohesive pre-setrefs planner for a base-relation CustomScan tuple.
pub(crate) struct BaseScanTuplePlanner {
    scan_relid: pg_sys::Index,
    relation_oid: pg_sys::Oid,
    analyzer: RelationExprAnalyzer,
}

impl BaseScanTuplePlanner {
    pub(crate) fn new(scan_relid: pg_sys::Index, relation_oid: pg_sys::Oid) -> Self {
        Self {
            scan_relid,
            relation_oid,
            analyzer: RelationExprAnalyzer::new(RelationScope::exact(scan_relid)),
        }
    }

    /// Analyze every executor-visible expression before setrefs, then build a
    /// Var-only custom tlist plus the matching base-attno mapping. Any shape
    /// that cannot be proven safe falls back atomically to relation layout.
    pub(crate) unsafe fn plan(
        &self,
        targetlist: *mut pg_sys::List,
        path_target_exprs: *mut pg_sys::List,
        qual: *mut pg_sys::List,
        custom_exprs: *mut pg_sys::List,
    ) -> PlannedScanTuple {
        let mut analysis = LayoutAnalysis::default();
        unsafe { self.inspect_targetlist(targetlist, &mut analysis) };
        unsafe { self.inspect_path_target(path_target_exprs, &mut analysis) };
        unsafe { self.inspect_expr_list(qual, &mut analysis) };
        unsafe { self.inspect_expr_list(custom_exprs, &mut analysis) };

        if !analysis.can_narrow_tuple {
            let storage_attnos =
                if analysis.can_prune_storage && !analysis.vars_by_attno.is_empty() {
                    Some(analysis.vars_by_attno.attnos().collect::<Vec<_>>())
                } else {
                    None
                };
            return PlannedScanTuple::relation_with_storage_hint(storage_attnos);
        }

        if analysis.vars_by_attno.is_empty() {
            let Some(dummy) = (unsafe { self.first_live_user_var() }) else {
                return PlannedScanTuple::relation();
            };
            analysis.vars_by_attno.insert(dummy);
        }

        let mut custom_scan_tlist = ptr::null_mut();
        let mut attnos_by_resno = Vec::with_capacity(analysis.vars_by_attno.len());

        for direct in &analysis.direct_outputs {
            if analysis.vars_by_attno.take(direct.attno).is_none() {
                continue;
            }
            attnos_by_resno.push(direct.attno);
            custom_scan_tlist = unsafe {
                Self::append_tlist_entry(
                    custom_scan_tlist,
                    direct.var,
                    attnos_by_resno.len(),
                    direct.resname,
                    direct.resjunk,
                )
            };
        }

        for (attno, var) in analysis.vars_by_attno.iter() {
            attnos_by_resno.push(attno);
            custom_scan_tlist = unsafe {
                Self::append_tlist_entry(
                    custom_scan_tlist,
                    var.as_ptr(),
                    attnos_by_resno.len(),
                    ptr::null_mut(),
                    true,
                )
            };
        }

        PlannedScanTuple {
            custom_scan_tlist,
            layout: ScanTupleLayout::projected_base(attnos_by_resno),
        }
    }

    /// Build a relation-shaped scan tuple while preserving storage-column
    /// pruning. Modify scans use this form so PostgreSQL can evaluate standard
    /// system Vars (`ctid`, `tableoid`) from slot metadata and form whole-row
    /// values in executor projection.
    pub(crate) unsafe fn plan_relation_scan(
        &self,
        targetlist: *mut pg_sys::List,
        path_target_exprs: *mut pg_sys::List,
        qual: *mut pg_sys::List,
        custom_exprs: *mut pg_sys::List,
    ) -> PlannedScanTuple {
        let mut analysis = LayoutAnalysis::default();
        unsafe { self.inspect_targetlist(targetlist, &mut analysis) };
        unsafe { self.inspect_path_target(path_target_exprs, &mut analysis) };
        unsafe { self.inspect_expr_list(qual, &mut analysis) };
        unsafe { self.inspect_expr_list(custom_exprs, &mut analysis) };

        let storage_attnos = analysis
            .can_prune_storage
            .then(|| analysis.vars_by_attno.attnos().collect());
        PlannedScanTuple::relation_with_storage_hint(storage_attnos)
    }

    /// `CUSTOMPATH_SUPPORT_PROJECTION` lets PostgreSQL call PlanCustomPath with
    /// a NIL tlist and replace `plan.targetlist` afterwards. The PathTarget is
    /// therefore part of the authoritative pre-setrefs dependency input.
    unsafe fn inspect_path_target(
        &self,
        exprs: *mut pg_sys::List,
        analysis: &mut LayoutAnalysis,
    ) {
        if exprs.is_null() {
            return;
        }
        let len = unsafe { pg_sys::list_length(exprs) };
        for index in 0..len {
            let expr = unsafe { pg_sys::list_nth(exprs, index) } as *mut pg_sys::Expr;
            unsafe { self.inspect_expr(expr, analysis) };
            if !expr.is_null() && unsafe { (*expr).type_ } == pg_sys::NodeTag::T_Var {
                let var = expr.cast::<pg_sys::Var>();
                if unsafe { self.is_local_user_var(var) } {
                    analysis.direct_outputs.push(DirectOutput {
                        attno: unsafe { (*var).varattno },
                        var,
                        resname: ptr::null_mut(),
                        resjunk: false,
                    });
                }
            }
        }
    }

    unsafe fn inspect_targetlist(
        &self,
        targetlist: *mut pg_sys::List,
        analysis: &mut LayoutAnalysis,
    ) {
        if targetlist.is_null() {
            return;
        }
        let len = unsafe { pg_sys::list_length(targetlist) };
        for index in 0..len {
            let tle = unsafe { pg_sys::list_nth(targetlist, index) }
                as *mut pg_sys::TargetEntry;
            if tle.is_null()
                || unsafe { (*tle).xpr.type_ } != pg_sys::NodeTag::T_TargetEntry
            {
                analysis.can_narrow_tuple = false;
                continue;
            }
            let expr = unsafe { (*tle).expr };
            unsafe { self.inspect_expr(expr, analysis) };

            if !expr.is_null() && unsafe { (*expr).type_ } == pg_sys::NodeTag::T_Var {
                let var = expr.cast::<pg_sys::Var>();
                if unsafe { self.is_local_user_var(var) } {
                    analysis.direct_outputs.push(DirectOutput {
                        attno: unsafe { (*var).varattno },
                        var,
                        resname: unsafe { (*tle).resname },
                        resjunk: unsafe { (*tle).resjunk },
                    });
                }
            }
        }
    }

    unsafe fn inspect_expr_list(
        &self,
        list: *mut pg_sys::List,
        analysis: &mut LayoutAnalysis,
    ) {
        if list.is_null() {
            return;
        }
        let len = unsafe { pg_sys::list_length(list) };
        for index in 0..len {
            let expr = unsafe { pg_sys::list_nth(list, index) } as *mut pg_sys::Expr;
            unsafe { self.inspect_expr(expr, analysis) };
        }
    }

    unsafe fn inspect_expr(
        &self,
        expr: *mut pg_sys::Expr,
        analysis: &mut LayoutAnalysis,
    ) {
        if expr.is_null() {
            return;
        }
        // PostgreSQL's set_customscan_references() rewrites plan.qual and
        // custom_exprs against custom_scan_tlist through fix_upper_expr().
        // Its expression mutator descends into SubPlan.testexpr and
        // SubPlan.args, exactly as the walker used by RelationExprAnalyzer
        // does. A SubPlan therefore needs its relation-local Vars in the
        // Var-only tlist, not a relation-shaped scan tuple.
        let usage = unsafe { self.analyzer.collect_expr(expr) };
        analysis.absorb(usage);
    }

    unsafe fn is_local_user_var(&self, var: *mut pg_sys::Var) -> bool {
        unsafe {
            (*var).varlevelsup == 0
                && (*var).varno == self.scan_relid as c_int
                && (*var).varattno > 0
        }
    }

    unsafe fn first_live_user_var(&self) -> Option<NonNull<pg_sys::Var>> {
        if self.relation_oid == pg_sys::Oid::INVALID {
            return None;
        }
        let relation = unsafe {
            pg_sys::relation_open(self.relation_oid, pg_sys::NoLock as i32)
        };
        let tuple_desc = unsafe { (*relation).rd_att };
        let natts = unsafe { (*tuple_desc).natts as usize };
        let attrs = unsafe {
            std::slice::from_raw_parts((*tuple_desc).attrs.as_ptr(), natts)
        };
        let result = attrs.iter().enumerate().find_map(|(index, attr)| {
            if attr.attisdropped {
                return None;
            }
            let attno = pg_sys::AttrNumber::try_from(index + 1).ok()?;
            let var = unsafe {
                pg_sys::makeVar(
                    self.scan_relid as c_int,
                    attno,
                    attr.atttypid,
                    attr.atttypmod,
                    attr.attcollation,
                    0,
                )
            };
            // PostgreSQL palloc-backed constructors report OOM instead of
            // returning NULL.
            Some(unsafe { NonNull::new_unchecked(var) })
        });
        unsafe { pg_sys::relation_close(relation, pg_sys::NoLock as i32) };
        result
    }

    unsafe fn append_tlist_entry(
        list: *mut pg_sys::List,
        source_var: *mut pg_sys::Var,
        resno: usize,
        resname: *mut c_char,
        resjunk: bool,
    ) -> *mut pg_sys::List {
        let copied = unsafe { pg_sys::copyObjectImpl(source_var.cast::<c_void>()) }
            .cast::<pg_sys::Expr>();
        let resno = pg_sys::AttrNumber::try_from(resno)
            .expect("custom scan tlist cannot exceed AttrNumber::MAX entries");
        let tle = unsafe { pg_sys::makeTargetEntry(copied, resno, resname, resjunk) };
        unsafe { pg_sys::lappend(list, tle.cast()) }
    }
}

struct LayoutAnalysis {
    vars_by_attno: RelationVarsByAttno,
    direct_outputs: Vec<DirectOutput>,
    /// The scan slot can be narrowed to a custom_scan_tlist (ProjectedBase).
    can_narrow_tuple: bool,
    /// The provider may read only the referenced columns even when the slot
    /// must stay full-width. Only `false` for whole-row Var, which
    /// legitimately needs every column.
    can_prune_storage: bool,
}

impl Default for LayoutAnalysis {
    fn default() -> Self {
        Self {
            vars_by_attno: RelationVarsByAttno::default(),
            direct_outputs: Vec::new(),
            can_narrow_tuple: true,
            can_prune_storage: true,
        }
    }
}

impl LayoutAnalysis {
    fn absorb(&mut self, usage: RelationExprUsage) {
        if usage.has_whole_row() {
            self.can_narrow_tuple = false;
            self.can_prune_storage = false;
            return;
        }
        if !usage.system_attnos().is_empty() {
            self.can_narrow_tuple = false;
        }
        for var in usage.user_vars() {
            if let Some(existing) = self.vars_by_attno.get(var.attno) {
                let equal = unsafe {
                    pg_sys::bms_equal(
                        existing.as_ref().varnullingrels,
                        var.raw.as_ref().varnullingrels,
                    )
                };
                if !equal {
                    self.can_narrow_tuple = false;
                }
            } else {
                self.vars_by_attno.insert(var.raw);
            }
        }
    }
}

#[derive(Clone, Copy)]
struct DirectOutput {
    attno: pg_sys::AttrNumber,
    var: *mut pg_sys::Var,
    resname: *mut c_char,
    resjunk: bool,
}

/// Backend-test view of a planned base-scan tuple contract.
///
/// This adapter is compiled only for the dedicated `pg-backend-tests`
/// extension. Keeping construction here lets those tests exercise the real
/// private planner without widening the production CustomScan API.
#[doc(hidden)]
pub struct ScanTuplePlanProbe {
    custom_scan_tlist: *mut pg_sys::List,
    layout: ScanTupleLayout,
}

/// Raw scan-tuple shape reported by [`ScanTuplePlanProbe`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanTupleShape<'a> {
    Relation,
    ProjectedBase(&'a [pg_sys::AttrNumber]),
}

impl ScanTuplePlanProbe {
    /// Run the production base-scan tuple planner over backend-owned nodes.
    ///
    /// # Safety
    ///
    /// Every pointer must be NULL or a live PostgreSQL planner node of the
    /// documented list shape. `scan_relid` and `relation_oid` must identify
    /// the same base relation whenever planning needs a count-only dummy Var.
    pub unsafe fn plan_base_scan(
        scan_relid: pg_sys::Index,
        relation_oid: pg_sys::Oid,
        targetlist: *mut pg_sys::List,
        path_target_exprs: *mut pg_sys::List,
        qual: *mut pg_sys::List,
        custom_exprs: *mut pg_sys::List,
    ) -> Self {
        let planned = unsafe {
            BaseScanTuplePlanner::new(scan_relid, relation_oid).plan(
                targetlist,
                path_target_exprs,
                qual,
                custom_exprs,
            )
        };
        Self {
            custom_scan_tlist: planned.custom_scan_tlist,
            layout: planned.layout,
        }
    }

    /// Run the relation-shaped storage-pruning planner used by Modify scans.
    ///
    /// # Safety
    ///
    /// Every pointer must be NULL or a live PostgreSQL planner node of the
    /// documented list shape.
    pub unsafe fn plan_relation_scan(
        scan_relid: pg_sys::Index,
        relation_oid: pg_sys::Oid,
        targetlist: *mut pg_sys::List,
        path_target_exprs: *mut pg_sys::List,
        qual: *mut pg_sys::List,
        custom_exprs: *mut pg_sys::List,
    ) -> Self {
        let planned = unsafe {
            BaseScanTuplePlanner::new(scan_relid, relation_oid).plan_relation_scan(
                targetlist,
                path_target_exprs,
                qual,
                custom_exprs,
            )
        };
        Self {
            custom_scan_tlist: planned.custom_scan_tlist,
            layout: planned.layout,
        }
    }

    pub fn shape(&self) -> ScanTupleShape<'_> {
        if self.custom_scan_tlist.is_null() {
            return ScanTupleShape::Relation;
        }

        match self.layout.required_columns() {
            NeededColumns::Subset(attnos) => ScanTupleShape::ProjectedBase(attnos),
            NeededColumns::All => ScanTupleShape::Relation,
        }
    }

    pub fn required_columns(&self) -> NeededColumns<'_> {
        self.layout.required_columns()
    }

    pub fn custom_scan_tlist(&self) -> *mut pg_sys::List {
        self.custom_scan_tlist
    }
}
