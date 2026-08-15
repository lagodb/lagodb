use std::ffi::{c_char, c_void};
use std::ptr;
use std::sync::OnceLock;

use pgrx::prelude::PgSqlErrorCode;
use pgrx::{pg_guard, pg_sys};

use crate::customscan::modify::LakebaseCustomModifyProvider;
use crate::customscan::provider::RelationContext;
use crate::diag::{PgReportError, ReportableError};

use super::{methods, registry};

pub(super) use crate::customscan::provider::WHOLEROW_NAME;

static PREV_PLANNER: OnceLock<pg_sys::planner_hook_type> = OnceLock::new();
static PREV_UPPER: OnceLock<pg_sys::create_upper_paths_hook_type> = OnceLock::new();

unsafe extern "C-unwind" {
    fn find_all_inheritors(
        parent_rel_id: pg_sys::Oid,
        lock_mode: i32,
        numparents: *mut *mut pg_sys::List,
    ) -> *mut pg_sys::List;
}

struct SystemColumnContext {
    target_rti: pg_sys::Index,
    found: bool,
}

pub(super) fn install_hooks() {
    // SAFETY: `_PG_init` is single-threaded and each slot is installed once in
    // this backend. Every callback chains the previously installed hook.
    unsafe {
        PREV_PLANNER.get_or_init(|| {
            let previous = pg_sys::planner_hook;
            pg_sys::planner_hook = Some(planner);
            previous
        });
        PREV_UPPER.get_or_init(|| {
            let previous = pg_sys::create_upper_paths_hook;
            pg_sys::create_upper_paths_hook = Some(create_upper_paths);
            previous
        });
    }
}

unsafe fn list_ptr(list: *mut pg_sys::List, index: i32) -> *mut c_void {
    unsafe { pg_sys::list_nth(list, index) }
}

unsafe fn rte_for(
    root: *mut pg_sys::PlannerInfo,
    rti: pg_sys::Index,
) -> *mut pg_sys::RangeTblEntry {
    let parse = unsafe { (*root).parse };
    unsafe { list_ptr((*parse).rtable, (rti - 1) as i32).cast() }
}

unsafe fn provider_for_rti(
    root: *mut pg_sys::PlannerInfo,
    rti: pg_sys::Index,
) -> Option<&'static dyn registry::ErasedModifyProvider> {
    let rte = unsafe { rte_for(root, rti) };
    unsafe { provider_for_rte(rte) }
}

unsafe fn provider_for_rte(
    rte: *mut pg_sys::RangeTblEntry,
) -> Option<&'static dyn registry::ErasedModifyProvider> {
    if unsafe { (*rte).rtekind } != pg_sys::RTEKind::RTE_RELATION {
        return None;
    }
    let context = RelationContext::from_ref(unsafe { &*rte });
    registry::matching(&context)
}

struct WholeRowPlanner {
    parse: *mut pg_sys::Query,
    rte: *mut pg_sys::RangeTblEntry,
    target_rti: pg_sys::Index,
}

impl WholeRowPlanner {
    unsafe fn is_standard_wholerow_tle(
        tle: *mut pg_sys::TargetEntry,
        target_rti: pg_sys::Index,
    ) -> bool {
        if tle.is_null()
            || unsafe { !(*tle).resjunk }
            || unsafe { (*tle).resname.is_null() }
            || unsafe { std::ffi::CStr::from_ptr((*tle).resname) } != WHOLEROW_NAME
        {
            return false;
        }
        let expr = unsafe { (*tle).expr };
        if expr.is_null() || unsafe { (*expr).type_ } != pg_sys::NodeTag::T_Var {
            return false;
        }
        let var = expr.cast::<pg_sys::Var>();
        unsafe {
            (*var).varno == target_rti as i32
                && (*var).varattno == pg_sys::InvalidAttrNumber as pg_sys::AttrNumber
                && (*var).varlevelsup == 0
        }
    }

    /// # Safety
    ///
    /// `parse` and its range table must be live rewrite-complete planner input.
    unsafe fn inspect(parse: *mut pg_sys::Query) -> Option<Self> {
        if !matches!(
            unsafe { (*parse).commandType },
            pg_sys::CmdType::CMD_UPDATE
                | pg_sys::CmdType::CMD_DELETE
                | pg_sys::CmdType::CMD_MERGE
        ) {
            return None;
        }
        let target_rti = unsafe { (*parse).resultRelation as pg_sys::Index };
        let rte = unsafe {
            list_ptr((*parse).rtable, target_rti as i32 - 1)
                .cast::<pg_sys::RangeTblEntry>()
        };
        unsafe { provider_for_rte(rte) }.map(|_| Self {
            parse,
            rte,
            target_rti,
        })
    }

    /// # Safety
    ///
    /// Planner nodes captured by `inspect` must still be live.
    unsafe fn targetlist_contains_wholerow(&self) -> bool {
        let targetlist = unsafe { (*self.parse).targetList };
        let len = unsafe { pg_sys::list_length(targetlist) };
        for index in 0..len {
            let tle =
                unsafe { list_ptr(targetlist, index).cast::<pg_sys::TargetEntry>() };
            if unsafe { Self::is_standard_wholerow_tle(tle, self.target_rti) } {
                return true;
            }
        }
        false
    }

    /// # Safety
    ///
    /// The target relation must remain locked and its relcache entry valid.
    unsafe fn is_required(&self) -> bool {
        if unsafe { (*self.parse).commandType } != pg_sys::CmdType::CMD_DELETE
            || !unsafe { (*self.parse).returningList }.is_null()
        {
            return true;
        }
        // This hook runs before `standard_planner()` expands inheritance, so
        // child relcache entries are not locked yet.  Lock descendants with
        // the target RTE's eventual lock mode, matching PostgreSQL's
        // inheritance expansion.  AccessShareLock would make relation_open()
        // safe, but would then require a later lock upgrade to RowExclusiveLock
        // for DELETE and can introduce an avoidable upgrade deadlock.
        let lockmode = unsafe { (*self.rte).rellockmode };
        let relations = unsafe {
            pg_sys::ffi::pg_guard_ffi_boundary(|| unsafe {
                find_all_inheritors((*self.rte).relid, lockmode, ptr::null_mut())
            })
        };
        let count = unsafe { pg_sys::list_length(relations) };
        for index in 0..count {
            let relation_oid = unsafe { pg_sys::list_nth_oid(relations, index) };
            let relation =
                unsafe { pg_sys::relation_open(relation_oid, pg_sys::NoLock as i32) };
            let trigger_desc = unsafe { (*relation).trigdesc };
            let required = !trigger_desc.is_null()
                && unsafe {
                    (*trigger_desc).trig_delete_before_row
                        || (*trigger_desc).trig_delete_after_row
                        || (*trigger_desc).trig_delete_instead_row
                        || (*trigger_desc).trig_delete_old_table
                };
            unsafe { pg_sys::relation_close(relation, pg_sys::NoLock as i32) };
            if required {
                return true;
            }
        }
        false
    }

    /// # Safety
    ///
    /// The query must be planner-owned and mutable in the current memory context.
    unsafe fn inject(&self) {
        if unsafe { self.targetlist_contains_wholerow() }
            || !unsafe { self.is_required() }
        {
            return;
        }
        let var = unsafe {
            pg_sys::makeWholeRowVar(self.rte, self.target_rti as i32, 0, false)
        };
        let resno = unsafe { pg_sys::list_length((*self.parse).targetList) + 1 };
        let tle = unsafe {
            pg_sys::makeTargetEntry(
                var.cast(),
                resno as pg_sys::AttrNumber,
                pg_sys::pstrdup(WHOLEROW_NAME.as_ptr()),
                true,
            )
        };
        unsafe {
            (*self.parse).targetList =
                pg_sys::lappend((*self.parse).targetList, tle.cast());
        }
    }
}

#[cfg(test)]
mod wholerow_tests {
    use super::*;

    fn whole_row_tle(
        resjunk: bool,
        name: &'static std::ffi::CStr,
    ) -> (pg_sys::Var, pg_sys::TargetEntry) {
        let var = pg_sys::Var {
            xpr: pg_sys::Expr {
                type_: pg_sys::NodeTag::T_Var,
            },
            varno: 7,
            varattno: pg_sys::InvalidAttrNumber as pg_sys::AttrNumber,
            varlevelsup: 0,
            ..Default::default()
        };
        let tle = pg_sys::TargetEntry {
            xpr: pg_sys::Expr {
                type_: pg_sys::NodeTag::T_TargetEntry,
            },
            expr: std::ptr::null_mut(),
            resjunk,
            resname: name.as_ptr().cast_mut(),
            ..Default::default()
        };
        (var, tle)
    }

    #[test]
    fn wholerow_dedup_requires_exact_junk_contract() {
        let (mut var, mut exact) = whole_row_tle(true, c"wholerow");
        exact.expr = std::ptr::from_mut(&mut var).cast();
        assert!(unsafe { WholeRowPlanner::is_standard_wholerow_tle(&mut exact, 7) });

        let (mut var, mut non_junk) = whole_row_tle(false, c"wholerow");
        non_junk.expr = std::ptr::from_mut(&mut var).cast();
        assert!(!unsafe {
            WholeRowPlanner::is_standard_wholerow_tle(&mut non_junk, 7)
        });

        let (mut var, mut wrong_name) = whole_row_tle(true, c"other_wholerow");
        wrong_name.expr = std::ptr::from_mut(&mut var).cast();
        assert!(!unsafe {
            WholeRowPlanner::is_standard_wholerow_tle(&mut wrong_name, 7)
        });

        let (mut var, mut wrong_relation) = whole_row_tle(true, c"wholerow");
        var.varno = 8;
        wrong_relation.expr = std::ptr::from_mut(&mut var).cast();
        assert!(!unsafe {
            WholeRowPlanner::is_standard_wholerow_tle(&mut wrong_relation, 7)
        });

        let (mut var, mut wrong_level) = whole_row_tle(true, c"wholerow");
        var.varlevelsup = 1;
        wrong_level.expr = std::ptr::from_mut(&mut var).cast();
        assert!(!unsafe {
            WholeRowPlanner::is_standard_wholerow_tle(&mut wrong_level, 7)
        });
    }
}

/// # Safety
///
/// `parse` must be a live rewrite-complete Query owned by the planner call.
unsafe fn inject_wholerow(parse: *mut pg_sys::Query) {
    if let Some(planner) = unsafe { WholeRowPlanner::inspect(parse) } {
        unsafe { planner.inject() };
    }
}

unsafe extern "C-unwind" fn find_target_system_column(
    node: *mut pg_sys::Node,
    raw_context: *mut c_void,
) -> bool {
    if node.is_null() {
        return false;
    }
    let context = unsafe { &mut *raw_context.cast::<SystemColumnContext>() };
    if unsafe { (*node).type_ } == pg_sys::NodeTag::T_Var {
        let var = node.cast::<pg_sys::Var>();
        if unsafe {
            (*var).varlevelsup == 0
                && (*var).varno == context.target_rti as i32
                && (*var).varattno < 0
                && (*var).varattno
                    != pg_sys::TableOidAttributeNumber as pg_sys::AttrNumber
        } {
            context.found = true;
            return true;
        }
    }
    unsafe {
        pg_sys::expression_tree_walker_impl(
            node,
            Some(find_target_system_column),
            raw_context,
        )
    }
}

unsafe fn reject_explicit_target_system_columns(parse: *mut pg_sys::Query) {
    if !matches!(
        unsafe { (*parse).commandType },
        pg_sys::CmdType::CMD_UPDATE
            | pg_sys::CmdType::CMD_DELETE
            | pg_sys::CmdType::CMD_MERGE
    ) {
        return;
    }
    let target_rti = unsafe { (*parse).resultRelation as pg_sys::Index };
    let rte = unsafe {
        list_ptr((*parse).rtable, target_rti as i32 - 1)
            .cast::<pg_sys::RangeTblEntry>()
    };
    if unsafe { (*rte).rtekind } != pg_sys::RTEKind::RTE_RELATION
        || unsafe { provider_for_rte(rte) }.is_none()
    {
        return;
    }

    let mut context = SystemColumnContext {
        target_rti,
        found: false,
    };
    unsafe {
        pg_sys::query_tree_walker_impl(
            parse,
            Some(find_target_system_column),
            std::ptr::from_mut(&mut context).cast(),
            (pg_sys::QTW_IGNORE_RT_SUBQUERIES | pg_sys::QTW_IGNORE_CTE_SUBQUERIES)
                as i32,
        );
    }
    if context.found {
        Err::<(), _>(PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            "Lakebase row-level mutation cannot reference PostgreSQL heap system row columns",
        ))
        .report_unwrap();
    }
}

/// # Safety
///
/// `parse` and every nested Query must be live rewrite-complete planner input.
unsafe fn prepare_query_tree(parse: *mut pg_sys::Query) {
    unsafe {
        inject_wholerow(parse);
        reject_explicit_target_system_columns(parse);
        pg_sys::query_tree_walker_impl(
            parse,
            Some(prepare_query_walker),
            ptr::null_mut(),
            0,
        );
    }
}

unsafe extern "C-unwind" fn prepare_query_walker(
    node: *mut pg_sys::Node,
    context: *mut c_void,
) -> bool {
    if node.is_null() {
        return false;
    }
    if unsafe { (*node).type_ } == pg_sys::NodeTag::T_Query {
        unsafe { prepare_query_tree(node.cast()) };
        return false;
    }
    unsafe {
        pg_sys::expression_tree_walker_impl(node, Some(prepare_query_walker), context)
    }
}

fn copy_node<T>(node: *mut T) -> *mut T {
    if node.is_null() {
        return ptr::null_mut();
    }
    // SAFETY: planner nodes are copyObject-compatible and live in the current
    // planner memory context.
    unsafe { pg_sys::copyObjectImpl(node.cast()).cast() }
}

unsafe fn replace_rowid_vars(
    root: *mut pg_sys::PlannerInfo,
    tlist: *mut pg_sys::List,
    varno: pg_sys::Index,
) -> *mut pg_sys::List {
    let copied = unsafe { pg_sys::list_copy(tlist) };
    let len = unsafe { pg_sys::list_length(copied) };
    for index in 0..len {
        let cell = unsafe { pg_sys::list_nth_cell(copied, index) };
        let tle = unsafe { (*cell).ptr_value.cast::<pg_sys::TargetEntry>() };
        let expr = unsafe { (*tle).expr };
        if unsafe { (*expr).type_ } != pg_sys::NodeTag::T_Var {
            continue;
        }
        let var = expr.cast::<pg_sys::Var>();
        if unsafe { (*var).varno } != pg_sys::ROWID_VAR {
            continue;
        }
        let new_tle = copy_node(tle);
        let row_info = unsafe {
            list_ptr((*root).row_identity_vars, (*var).varattno as i32 - 1)
                .cast::<pg_sys::RowIdentityVarInfo>()
        };
        let new_var = copy_node(unsafe { (*row_info).rowidvar });
        unsafe {
            (*new_var).varno = varno as i32;
            (*new_var).varnosyn = 0;
            (*new_var).varattnosyn = 0;
            (*new_tle).expr = new_var.cast();
            (*cell).ptr_value = new_tle.cast();
        }
    }
    copied
}

/// Replace planner-only ROWID_VAR entries in projection/pseudoconstant Result
/// nodes between ModifyTable and its scan tree. The lower scan/append has
/// already resolved its row identity, so leaving the wrapper targetlist in the
/// planner namespace makes PostgreSQL's `set_upper_references` unable to match
/// it. This mirrors TimescaleDB's ModifyHypertable PG17 fix.
unsafe fn replace_result_rowid_vars(
    root: *mut pg_sys::PlannerInfo,
    mut plan: *mut pg_sys::Plan,
    varno: pg_sys::Index,
) {
    while !plan.is_null()
        && unsafe { (*plan).type_ } == pg_sys::NodeTag::T_Result
        && !unsafe { (*plan).lefttree }.is_null()
    {
        unsafe {
            (*plan).targetlist = replace_rowid_vars(root, (*plan).targetlist, varno);
            plan = (*plan).lefttree;
        }
    }
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn plan_modify_table<
    P: LakebaseCustomModifyProvider,
>(
    root: *mut pg_sys::PlannerInfo,
    _rel: *mut pg_sys::RelOptInfo,
    best_path: *mut pg_sys::CustomPath,
    _tlist: *mut pg_sys::List,
    _clauses: *mut pg_sys::List,
    custom_plans: *mut pg_sys::List,
) -> *mut pg_sys::Plan {
    let mt = unsafe { list_ptr(custom_plans, 0).cast::<pg_sys::ModifyTable>() };
    let scan = unsafe {
        pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScan>())
            .cast::<pg_sys::CustomScan>()
    };
    let path = unsafe { &(*best_path).path };
    let mut targetlist = copy_node(unsafe { (*root).processed_tlist });
    if matches!(
        unsafe { (*mt).operation },
        pg_sys::CmdType::CMD_UPDATE
            | pg_sys::CmdType::CMD_DELETE
            | pg_sys::CmdType::CMD_MERGE
    ) {
        targetlist =
            unsafe { replace_rowid_vars(root, targetlist, (*mt).nominalRelation) };
        unsafe {
            replace_result_rowid_vars(
                root,
                (*mt).plan.lefttree,
                (*mt).nominalRelation,
            )
        };
    }
    unsafe {
        (*scan).scan.plan.type_ = pg_sys::NodeTag::T_CustomScan;
        (*scan).scan.plan.startup_cost = path.startup_cost;
        (*scan).scan.plan.total_cost = path.total_cost;
        (*scan).scan.plan.plan_rows = path.rows;
        (*scan).scan.plan.plan_width = (*path.pathtarget).width;
        (*scan).scan.plan.targetlist = targetlist;
        (*scan).scan.scanrelid = 0;
        (*scan).custom_plans = custom_plans;
        (*scan).custom_scan_tlist = targetlist;
        (*scan).methods = &methods::tables::<P>().modify_scan;
    }
    scan.cast()
}

#[pg_guard]
unsafe extern "C-unwind" fn create_upper_paths(
    root: *mut pg_sys::PlannerInfo,
    stage: pg_sys::UpperRelationKind::Type,
    input_rel: *mut pg_sys::RelOptInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    extra: *mut c_void,
) {
    if let Some(Some(previous)) = PREV_UPPER.get() {
        unsafe { previous(root, stage, input_rel, output_rel, extra) };
    }
    if stage != pg_sys::UpperRelationKind::UPPERREL_FINAL {
        return;
    }
    let paths = unsafe { (*output_rel).pathlist };
    let len = unsafe { pg_sys::list_length(paths) };
    for index in 0..len {
        let cell = unsafe { pg_sys::list_nth_cell(paths, index) };
        let path = unsafe { (*cell).ptr_value.cast::<pg_sys::Path>() };
        if unsafe { (*path).type_ } != pg_sys::NodeTag::T_ModifyTablePath {
            continue;
        }
        let mt = path.cast::<pg_sys::ModifyTablePath>();
        let Some(provider) =
            (unsafe { provider_for_rti(root, (*mt).nominalRelation) })
        else {
            continue;
        };
        let custom = unsafe {
            pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomPath>())
                .cast::<pg_sys::CustomPath>()
        };
        unsafe {
            (*custom).path = (*mt).path;
            (*custom).path.type_ = pg_sys::NodeTag::T_CustomPath;
            (*custom).path.pathtype = pg_sys::NodeTag::T_CustomScan;
            (*custom).custom_paths =
                pg_sys::lappend(ptr::null_mut(), mt.cast::<c_void>());
            (*custom).methods = provider.path_methods();
            (*cell).ptr_value = custom.cast();
        }
    }
}

unsafe fn make_var_targetlist(source: *mut pg_sys::List) -> *mut pg_sys::List {
    let mut output = ptr::null_mut();
    let len = unsafe { pg_sys::list_length(source) };
    for index in 0..len {
        let tle = unsafe { list_ptr(source, index).cast::<pg_sys::TargetEntry>() };
        let var = unsafe { pg_sys::makeVarFromTargetEntry(pg_sys::INDEX_VAR, tle) };
        unsafe {
            (*var).varattno = (index + 1) as i16;
            output = pg_sys::lappend(
                output,
                pg_sys::makeTargetEntry(
                    var.cast(),
                    (index + 1) as i16,
                    (*tle).resname,
                    false,
                )
                .cast(),
            );
        }
    }
    output
}

unsafe fn fixup_plan(plan: *mut pg_sys::Plan) {
    if plan.is_null() {
        return;
    }
    if unsafe { (*plan).type_ } == pg_sys::NodeTag::T_CustomScan {
        let scan = plan.cast::<pg_sys::CustomScan>();
        if registry::is_modify_scan_methods(unsafe { (*scan).methods }) {
            let mt = unsafe {
                list_ptr((*scan).custom_plans, 0).cast::<pg_sys::ModifyTable>()
            };
            unsafe {
                (*scan).custom_scan_tlist = (*mt).plan.targetlist;
                (*scan).scan.plan.targetlist =
                    make_var_targetlist((*mt).plan.targetlist);
            }
        }
        let children = unsafe { (*scan).custom_plans };
        for index in 0..unsafe { pg_sys::list_length(children) } {
            unsafe { fixup_plan(list_ptr(children, index).cast()) };
        }
    }
    unsafe {
        fixup_plan((*plan).lefttree);
        fixup_plan((*plan).righttree);
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn planner(
    parse: *mut pg_sys::Query,
    query_string: *const c_char,
    cursor_options: i32,
    bound_params: pg_sys::ParamListInfo,
) -> *mut pg_sys::PlannedStmt {
    unsafe { prepare_query_tree(parse) };
    let planned = if let Some(Some(previous)) = PREV_PLANNER.get() {
        unsafe { previous(parse, query_string, cursor_options, bound_params) }
    } else {
        unsafe {
            pg_sys::standard_planner(
                parse,
                query_string,
                cursor_options,
                bound_params,
            )
        }
    };
    if planned.is_null() {
        return planned;
    }
    unsafe { fixup_plan((*planned).planTree) };
    let subplans = unsafe { (*planned).subplans };
    for index in 0..unsafe { pg_sys::list_length(subplans) } {
        unsafe { fixup_plan(list_ptr(subplans, index).cast()) };
    }
    planned
}
