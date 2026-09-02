//! Strict S1M semantic gate and Aggregate CustomPath materialization.

use std::ffi::{c_int, c_void};
use std::mem::size_of;
use std::ptr;

use lagodb_query::plan::{
    CostingContext, PlanCost, QueryCostEstimator, QueryPlanData, QueryPlanEnvelope,
    SourceCatalog, SourceEstimateTable,
};
use pgrx::pg_guard;
use pgrx::pg_sys;

use crate::runtime_api::source_directory;

use super::error::QueryHostError;
use super::methods;

use crate::gucs::QueryOffloadMode;

/// A query source leaf backed by one physical, non-inherited base relation.
struct PhysicalSourceRelation {
    input_rel: *mut pg_sys::RelOptInfo,
    range_table_index: pg_sys::Index,
    range_table_entry: *mut pg_sys::RangeTblEntry,
}

impl PhysicalSourceRelation {
    /// Apply PostgreSQL relation-shape facts once, before provider recognition.
    ///
    /// # Safety
    ///
    /// Both pointers must be live planner-owned nodes for the current upper
    /// path callback, and `range_table_index` must identify `range_table_entry`.
    unsafe fn inspect(
        input_rel: *mut pg_sys::RelOptInfo,
        range_table_index: pg_sys::Index,
        range_table_entry: *mut pg_sys::RangeTblEntry,
    ) -> Option<Self> {
        if unsafe { (*input_rel).reloptkind } != pg_sys::RelOptKind::RELOPT_BASEREL
            || unsafe { pg_sys::bms_membership((*input_rel).relids) }
                != pg_sys::BMS_Membership::BMS_SINGLETON
            || unsafe { (*input_rel).relid } != range_table_index
            || unsafe { (*range_table_entry).rtekind }
                != pg_sys::RTEKind::RTE_RELATION
            || unsafe { (*range_table_entry).relkind as u8 }
                != pg_sys::RELKIND_RELATION
            || unsafe { (*range_table_entry).inh }
        {
            return None;
        }

        Some(Self {
            input_rel,
            range_table_index,
            range_table_entry,
        })
    }
}

struct ScalarCountCandidate {
    root: *mut pg_sys::PlannerInfo,
    input_rel: *mut pg_sys::RelOptInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    range_table_index: pg_sys::Index,
    range_table_entry: *mut pg_sys::RangeTblEntry,
    aggregate: *mut pg_sys::Aggref,
}

impl ScalarCountCandidate {
    /// Recognize only the exact S1M product shape. Every rejected condition is
    /// a semantic gate, not a defensive check for a PostgreSQL invariant.
    unsafe fn inspect(
        root: *mut pg_sys::PlannerInfo,
        stage: pg_sys::UpperRelationKind::Type,
        input_rel: *mut pg_sys::RelOptInfo,
        output_rel: *mut pg_sys::RelOptInfo,
    ) -> Option<Self> {
        if crate::gucs::query_offload_mode() == QueryOffloadMode::Off
            || stage != pg_sys::UpperRelationKind::UPPERREL_GROUP_AGG
        {
            return None;
        }

        let parse = unsafe { (*root).parse };
        if unsafe { (*parse).commandType } != pg_sys::CmdType::CMD_SELECT
            || !unsafe { (*parse).hasAggs }
            || unsafe { (*parse).hasWindowFuncs }
            || unsafe { (*parse).hasTargetSRFs }
            || unsafe { (*parse).hasSubLinks }
            || unsafe { (*parse).hasDistinctOn }
            || unsafe { (*parse).hasRecursive }
            || unsafe { (*parse).hasModifyingCTE }
            || unsafe { (*parse).hasForUpdate }
            || unsafe { (*parse).hasRowSecurity }
            || !unsafe { (*parse).cteList }.is_null()
            || !unsafe { (*parse).groupClause }.is_null()
            || unsafe { (*parse).groupDistinct }
            || !unsafe { (*parse).groupingSets }.is_null()
            || !unsafe { (*parse).havingQual }.is_null()
            || !unsafe { (*parse).windowClause }.is_null()
            || !unsafe { (*parse).distinctClause }.is_null()
            || !unsafe { (*parse).sortClause }.is_null()
            || !unsafe { (*parse).limitOffset }.is_null()
            || !unsafe { (*parse).limitCount }.is_null()
            || !unsafe { (*parse).rowMarks }.is_null()
            || !unsafe { (*parse).setOperations }.is_null()
            || unsafe { pg_sys::list_length((*parse).rtable) } != 1
            || unsafe { pg_sys::list_length((*parse).targetList) } != 1
        {
            return None;
        }

        let jointree = unsafe { (*parse).jointree };
        if !unsafe { (*jointree).quals }.is_null()
            || unsafe { pg_sys::list_length((*jointree).fromlist) } != 1
            || !unsafe { (*input_rel).baserestrictinfo }.is_null()
            || !unsafe { (*input_rel).joininfo }.is_null()
            || !unsafe { (*input_rel).lateral_relids }.is_null()
        {
            return None;
        }

        let range_ref = unsafe { pg_sys::list_nth((*jointree).fromlist, 0) }
            .cast::<pg_sys::RangeTblRef>();
        if unsafe { (*range_ref).type_ } != pg_sys::NodeTag::T_RangeTblRef {
            return None;
        }
        let Ok(range_table_index) =
            pg_sys::Index::try_from(unsafe { (*range_ref).rtindex })
        else {
            return None;
        };
        if range_table_index == 0 {
            return None;
        }
        let range_table_entry = unsafe {
            pg_sys::list_nth((*parse).rtable, (range_table_index - 1) as c_int)
        }
        .cast::<pg_sys::RangeTblEntry>();
        let physical_relation = unsafe {
            PhysicalSourceRelation::inspect(
                input_rel,
                range_table_index,
                range_table_entry,
            )
        }?;
        if unsafe { (*range_table_entry).lateral }
            || !unsafe { (*range_table_entry).securityQuals }.is_null()
            || !unsafe { (*range_table_entry).tablesample }.is_null()
        {
            return None;
        }

        let target = unsafe { pg_sys::list_nth((*parse).targetList, 0) }
            .cast::<pg_sys::TargetEntry>();
        if unsafe { (*target).xpr.type_ } != pg_sys::NodeTag::T_TargetEntry
            || unsafe { (*target).resjunk }
            || unsafe { (*target).expr }.is_null()
            || unsafe { (*(*target).expr).type_ } != pg_sys::NodeTag::T_Aggref
        {
            return None;
        }
        let aggregate = unsafe { (*target).expr }.cast::<pg_sys::Aggref>();
        if unsafe { (*aggregate).aggfnoid } != pg_sys::Oid::from(pg_sys::F_COUNT_)
            || unsafe { (*aggregate).aggtype } != pg_sys::INT8OID
            || unsafe { (*aggregate).aggcollid } != pg_sys::InvalidOid
            || unsafe { (*aggregate).inputcollid } != pg_sys::InvalidOid
            || !unsafe { (*aggregate).aggargtypes }.is_null()
            || !unsafe { (*aggregate).aggdirectargs }.is_null()
            || !unsafe { (*aggregate).args }.is_null()
            || !unsafe { (*aggregate).aggorder }.is_null()
            || !unsafe { (*aggregate).aggdistinct }.is_null()
            || !unsafe { (*aggregate).aggfilter }.is_null()
            || !unsafe { (*aggregate).aggstar }
            || unsafe { (*aggregate).aggvariadic }
            || unsafe { (*aggregate).aggkind as u8 } != b'n'
            || unsafe { (*aggregate).agglevelsup } != 0
            || unsafe { (*aggregate).aggsplit } != pg_sys::AggSplit::AGGSPLIT_SIMPLE
        {
            return None;
        }

        Some(Self {
            root,
            input_rel: physical_relation.input_rel,
            output_rel,
            range_table_index: physical_relation.range_table_index,
            range_table_entry: physical_relation.range_table_entry,
            aggregate,
        })
    }

    unsafe fn plan(self) -> Result<(), QueryHostError> {
        let catalog = SourceCatalog::for_single_source(self.range_table_index)
            .map_err(QueryHostError::invalid_plan)?;
        let source = catalog
            .source_for_rti(self.range_table_index)
            .expect("single-source catalog contains its construction RTI");
        let Some(planned_source) = source_directory::resolve_count_rows(
            source,
            self.root,
            self.input_rel,
            self.range_table_index,
            self.range_table_entry,
        )?
        else {
            return Ok(());
        };
        if planned_source.source != source {
            return Err(QueryHostError::ExecutorContract(
                "query source directory returned a mismatched source identity",
            ));
        }
        let query = QueryPlanData::scalar_count(
            source,
            unsafe { (*self.aggregate).aggfnoid },
            unsafe { (*self.aggregate).aggtype },
        )
        .map_err(QueryHostError::invalid_plan)?;
        let execution = crate::gucs::query_execution_profile();
        let (cost, rows) = match crate::gucs::query_offload_mode() {
            QueryOffloadMode::Off => {
                unreachable!("the query-offload mode is gated before source planning")
            }
            QueryOffloadMode::Auto => {
                let sources = SourceEstimateTable::for_single_source(
                    source,
                    planned_source.estimate,
                )
                .map_err(QueryHostError::invalid_plan)?;
                // SAFETY: PostgreSQL initializes backend-local cost GUCs before
                // invoking upper-path hooks.
                let cpu_tuple_cost = unsafe { pg_sys::cpu_tuple_cost };
                // SAFETY: same backend-local planner-global invariant.
                let cpu_operator_cost = unsafe { pg_sys::cpu_operator_cost };
                let context = CostingContext::try_new(
                    execution,
                    cpu_tuple_cost,
                    cpu_operator_cost,
                )
                .map_err(QueryHostError::invalid_plan)?;
                let estimate = QueryCostEstimator::new(context, &sources)
                    .estimate(query.fragment())
                    .map_err(QueryHostError::invalid_plan)?;
                (estimate.cost(), estimate.rows())
            }
            QueryOffloadMode::Force => (PlanCost::forced(), 1.0),
        };
        let envelope = unsafe {
            QueryPlanEnvelope::encode(
                &query,
                execution,
                planned_source.provider_id,
                source,
                self.range_table_index,
                planned_source.estimate,
                planned_source.plan_data,
            )
        }
        .map_err(QueryHostError::invalid_plan)?;
        unsafe { self.emit_path(envelope, cost, rows) };
        Ok(())
    }

    unsafe fn emit_path(
        self,
        envelope: *mut pg_sys::List,
        cost: PlanCost,
        rows: f64,
    ) {
        let custom_path = unsafe {
            pg_sys::palloc0(size_of::<pg_sys::CustomPath>())
                .cast::<pg_sys::CustomPath>()
        };
        unsafe {
            let path = &mut (*custom_path).path;
            path.type_ = pg_sys::NodeTag::T_CustomPath;
            path.pathtype = pg_sys::NodeTag::T_CustomScan;
            path.parent = self.output_rel;
            path.pathtarget = (*self.output_rel).reltarget;
            path.param_info = ptr::null_mut();
            path.parallel_aware = false;
            path.parallel_safe = false;
            path.parallel_workers = 0;
            path.rows = rows;
            path.startup_cost = cost.startup();
            path.total_cost = cost.total();
            path.pathkeys = ptr::null_mut();

            (*custom_path).flags = pg_sys::CUSTOMPATH_SUPPORT_PROJECTION;
            (*custom_path).custom_paths = ptr::null_mut();
            (*custom_path).custom_restrictinfo = ptr::null_mut();
            (*custom_path).custom_private = envelope;
            (*custom_path).methods = methods::tables().path();
            pg_sys::add_path(self.output_rel, path);
        }
    }
}

pub(super) unsafe fn create_upper_paths(
    root: *mut pg_sys::PlannerInfo,
    stage: pg_sys::UpperRelationKind::Type,
    input_rel: *mut pg_sys::RelOptInfo,
    output_rel: *mut pg_sys::RelOptInfo,
) -> Result<(), QueryHostError> {
    let Some(candidate) = (unsafe {
        ScalarCountCandidate::inspect(root, stage, input_rel, output_rel)
    }) else {
        return Ok(());
    };
    unsafe { candidate.plan() }
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn plan_custom_path(
    _root: *mut pg_sys::PlannerInfo,
    _rel: *mut pg_sys::RelOptInfo,
    best_path: *mut pg_sys::CustomPath,
    target_list: *mut pg_sys::List,
    clauses: *mut pg_sys::List,
    custom_plans: *mut pg_sys::List,
) -> *mut pg_sys::Plan {
    match unsafe { materialize_plan(best_path, target_list, clauses, custom_plans) } {
        Ok(plan) => plan,
        Err(error) => error.into_report().report(),
    }
}

unsafe fn materialize_plan(
    best_path: *mut pg_sys::CustomPath,
    target_list: *mut pg_sys::List,
    clauses: *mut pg_sys::List,
    custom_plans: *mut pg_sys::List,
) -> Result<*mut pg_sys::Plan, QueryHostError> {
    if target_list.is_null() || !clauses.is_null() || !custom_plans.is_null() {
        return Err(QueryHostError::ExecutorContract(
            "selected AggregateScan path received an invalid final plan shape",
        ));
    }
    let envelope = unsafe { QueryPlanEnvelope::decode((*best_path).custom_private) }
        .map_err(QueryHostError::invalid_plan)?;
    let range_table_index = envelope.source().range_table_index();
    let scan_target_list = unsafe {
        pg_sys::copyObjectImpl(target_list.cast::<c_void>()).cast::<pg_sys::List>()
    };
    let custom_scan = unsafe {
        pg_sys::palloc0(size_of::<pg_sys::CustomScan>()).cast::<pg_sys::CustomScan>()
    };
    unsafe {
        let plan = &mut (*custom_scan).scan.plan;
        let path = &(*best_path).path;
        plan.type_ = pg_sys::NodeTag::T_CustomScan;
        plan.startup_cost = path.startup_cost;
        plan.total_cost = path.total_cost;
        plan.plan_rows = path.rows;
        plan.plan_width = (*path.pathtarget).width;
        plan.parallel_aware = false;
        plan.parallel_safe = false;
        plan.async_capable = false;
        plan.targetlist = target_list;
        plan.qual = ptr::null_mut();
        plan.lefttree = ptr::null_mut();
        plan.righttree = ptr::null_mut();
        plan.initPlan = ptr::null_mut();
        plan.extParam = ptr::null_mut();
        plan.allParam = ptr::null_mut();

        (*custom_scan).scan.scanrelid = 0;
        (*custom_scan).flags = (*best_path).flags;
        (*custom_scan).custom_plans = ptr::null_mut();
        (*custom_scan).custom_exprs = ptr::null_mut();
        (*custom_scan).custom_private = (*best_path).custom_private;
        (*custom_scan).custom_scan_tlist = scan_target_list;
        (*custom_scan).custom_relids =
            pg_sys::bms_make_singleton(range_table_index as c_int);
        (*custom_scan).methods = methods::tables().scan();
    }
    Ok(custom_scan.cast())
}
