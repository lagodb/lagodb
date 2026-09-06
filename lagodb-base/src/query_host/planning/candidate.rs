//! PostgreSQL semantic recognition and provider-neutral aggregate planning.

use std::ffi::c_int;
use std::ptr;

use pgrx::pg_sys;

use crate::gucs::QueryOffloadMode;
use crate::query_host::error::QueryHostError;

use super::aggregate_plan::AggregatePlanBuilder;
use super::distinct_plan::DistinctPlanBuilder;
use super::path_installation::QueryPathInstallation;

struct ScannableRelation {
    input_rel: *mut pg_sys::RelOptInfo,
    range_table_index: pg_sys::Index,
    range_table_entry: *mut pg_sys::RangeTblEntry,
}

impl ScannableRelation {
    unsafe fn inspect(
        input_rel: *mut pg_sys::RelOptInfo,
        range_table_index: pg_sys::Index,
        range_table_entry: *mut pg_sys::RangeTblEntry,
    ) -> Option<Self> {
        let relation = unsafe { &*input_rel };
        let range_table_entry_ref = unsafe { &*range_table_entry };
        if relation.reloptkind != pg_sys::RelOptKind::RELOPT_BASEREL
            || range_table_entry_ref.rtekind != pg_sys::RTEKind::RTE_RELATION
            || range_table_entry_ref.relkind as u8 != pg_sys::RELKIND_RELATION
            || range_table_entry_ref.inh
            || range_table_entry_ref.lateral
            || !range_table_entry_ref.securityQuals.is_null()
            || !range_table_entry_ref.tablesample.is_null()
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

pub(super) struct SingleRelationCandidate {
    pub(super) root: *mut pg_sys::PlannerInfo,
    pub(super) input_rel: *mut pg_sys::RelOptInfo,
    pub(super) output_rel: *mut pg_sys::RelOptInfo,
    pub(super) path_target: *mut pg_sys::PathTarget,
    pub(super) range_table_index: pg_sys::Index,
    pub(super) range_table_entry: *mut pg_sys::RangeTblEntry,
}

impl SingleRelationCandidate {
    pub(super) unsafe fn inspect_aggregate(
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
        let planner = unsafe { &*root };
        let parse = unsafe { &*planner.parse };
        let input = unsafe { &*input_rel };
        if parse.commandType != pg_sys::CmdType::CMD_SELECT
            || (!parse.hasAggs && parse.groupClause.is_null())
            || parse.hasWindowFuncs
            || parse.hasTargetSRFs
            || parse.hasSubLinks
            || parse.hasDistinctOn
            || parse.hasRecursive
            || parse.hasModifyingCTE
            || parse.hasForUpdate
            || parse.hasRowSecurity
            || !parse.cteList.is_null()
            || parse.groupDistinct
            || !parse.groupingSets.is_null()
            || !parse.windowClause.is_null()
            || !parse.rowMarks.is_null()
            || !parse.setOperations.is_null()
            || unsafe { pg_sys::list_length(parse.rtable) } != 1
            || !input.joininfo.is_null()
            || !input.lateral_relids.is_null()
        {
            return None;
        }
        let jointree = unsafe { &*parse.jointree };
        if unsafe { pg_sys::list_length(jointree.fromlist) } != 1 {
            return None;
        }
        let range_ref = unsafe { pg_sys::list_nth(jointree.fromlist, 0) }
            .cast::<pg_sys::RangeTblRef>();
        if unsafe { (*range_ref).type_ } != pg_sys::NodeTag::T_RangeTblRef {
            return None;
        }
        let range_table_index = unsafe { (*range_ref).rtindex as pg_sys::Index };
        let range_table_entry = unsafe {
            pg_sys::list_nth(parse.rtable, (range_table_index - 1) as c_int)
        }
        .cast::<pg_sys::RangeTblEntry>();
        let scannable = unsafe {
            ScannableRelation::inspect(
                input_rel,
                range_table_index,
                range_table_entry,
            )
        }?;
        if !unsafe { Self::restrictions_are_safe(input_rel) }
            || !unsafe { Self::expression_is_safe(root, jointree.quals) }
            || !unsafe { Self::target_list_is_safe(root, parse.targetList) }
            || !unsafe {
                Self::expression_list_is_safe(root, (*(*output_rel).reltarget).exprs)
            }
            || !unsafe { Self::expression_is_safe(root, parse.havingQual) }
        {
            return None;
        }
        Some(Self {
            root,
            input_rel: scannable.input_rel,
            output_rel,
            path_target: unsafe { (*output_rel).reltarget },
            range_table_index: scannable.range_table_index,
            range_table_entry: scannable.range_table_entry,
        })
    }

    pub(super) unsafe fn inspect_distinct(
        root: *mut pg_sys::PlannerInfo,
        stage: pg_sys::UpperRelationKind::Type,
        input_rel: *mut pg_sys::RelOptInfo,
        output_rel: *mut pg_sys::RelOptInfo,
    ) -> Option<Self> {
        if crate::gucs::query_offload_mode() == QueryOffloadMode::Off
            || stage != pg_sys::UpperRelationKind::UPPERREL_DISTINCT
        {
            return None;
        }
        let planner = unsafe { &*root };
        let parse = unsafe { &*planner.parse };
        if parse.commandType != pg_sys::CmdType::CMD_SELECT
            || parse.hasAggs
            || parse.hasWindowFuncs
            || parse.hasTargetSRFs
            || parse.hasSubLinks
            || parse.hasDistinctOn
            || parse.hasRecursive
            || parse.hasModifyingCTE
            || parse.hasForUpdate
            || parse.hasRowSecurity
            || !parse.cteList.is_null()
            || parse.groupDistinct
            || !parse.groupingSets.is_null()
            || !parse.groupClause.is_null()
            || !parse.havingQual.is_null()
            || !parse.windowClause.is_null()
            || parse.distinctClause.is_null()
            || !parse.rowMarks.is_null()
            || !parse.setOperations.is_null()
            || unsafe { pg_sys::list_length(parse.rtable) } != 1
        {
            return None;
        }
        let jointree = unsafe { &*parse.jointree };
        if unsafe { pg_sys::list_length(jointree.fromlist) } != 1 {
            return None;
        }
        let range_ref = unsafe { pg_sys::list_nth(jointree.fromlist, 0) }
            .cast::<pg_sys::RangeTblRef>();
        if unsafe { (*range_ref).type_ } != pg_sys::NodeTag::T_RangeTblRef {
            return None;
        }
        let range_table_index = unsafe { (*range_ref).rtindex as pg_sys::Index };
        if range_table_index as c_int >= planner.simple_rel_array_size {
            return None;
        }
        let path_target = unsafe { (*input_rel).reltarget };
        let base_rel =
            unsafe { *planner.simple_rel_array.add(range_table_index as usize) };
        if base_rel.is_null() {
            return None;
        }
        let range_table_entry = unsafe {
            pg_sys::list_nth(parse.rtable, (range_table_index - 1) as c_int)
        }
        .cast::<pg_sys::RangeTblEntry>();
        let scannable = unsafe {
            ScannableRelation::inspect(base_rel, range_table_index, range_table_entry)
        }?;
        if !unsafe { Self::restrictions_are_safe(base_rel) }
            || !unsafe { Self::expression_is_safe(root, jointree.quals) }
            || !unsafe { Self::target_list_is_safe(root, parse.targetList) }
            || !unsafe { Self::expression_list_is_safe(root, (*path_target).exprs) }
        {
            return None;
        }
        Some(Self {
            root,
            input_rel: scannable.input_rel,
            output_rel,
            path_target,
            range_table_index: scannable.range_table_index,
            range_table_entry: scannable.range_table_entry,
        })
    }

    unsafe fn restrictions_are_safe(input_rel: *mut pg_sys::RelOptInfo) -> bool {
        let restrictions = unsafe { (*input_rel).baserestrictinfo };
        let count = unsafe { pg_sys::list_length(restrictions) };
        for index in 0..count {
            let restriction = unsafe { pg_sys::list_nth(restrictions, index) }
                .cast::<pg_sys::RestrictInfo>();
            if unsafe { (*restriction).pseudoconstant }
                || !unsafe {
                    pg_sys::restriction_is_securely_promotable(restriction, input_rel)
                }
            {
                return false;
            }
        }
        true
    }

    unsafe fn expression_is_safe(
        _root: *mut pg_sys::PlannerInfo,
        expression: *mut pg_sys::Node,
    ) -> bool {
        // The current query path is leader-only and serial, and PG fallback
        // expressions retain their volatility classification in DataFusion.
        // SubPlans are rejected because they require PostgreSQL executor state
        // that the query engine does not own.
        // TODO(join/MPP): add a placement-aware parallel-hazard gate before
        // enabling a parallel query path under this execution contract.
        expression.is_null() || !unsafe { pg_sys::contain_subplans(expression) }
    }

    unsafe fn target_list_is_safe(
        root: *mut pg_sys::PlannerInfo,
        target_list: *mut pg_sys::List,
    ) -> bool {
        let count = unsafe { pg_sys::list_length(target_list) };
        (0..count).all(|index| {
            let entry = unsafe { pg_sys::list_nth(target_list, index) }
                .cast::<pg_sys::TargetEntry>();
            unsafe { Self::expression_is_safe(root, (*entry).expr.cast()) }
        })
    }

    unsafe fn expression_list_is_safe(
        root: *mut pg_sys::PlannerInfo,
        expressions: *mut pg_sys::List,
    ) -> bool {
        let count = unsafe { pg_sys::list_length(expressions) };
        (0..count).all(|index| {
            let expression = unsafe { pg_sys::list_nth(expressions, index) }
                .cast::<pg_sys::Node>();
            unsafe { Self::expression_is_safe(root, expression) }
        })
    }

    pub(super) unsafe fn plan_aggregate(self) -> Result<(), QueryHostError> {
        let mut builder = unsafe { AggregatePlanBuilder::new(&self) };
        let Some(planned) = (unsafe { builder.build() }) else {
            return Ok(());
        };
        let scan_rows = unsafe { (*self.input_rel).rows };
        let aggregate_rows = unsafe { self.estimate_group_rows(scan_rows) };
        let output_rows = unsafe { self.postgres_output_rows() };
        QueryPathInstallation::new(self, planned, aggregate_rows, output_rows)
            .install()
    }

    pub(super) unsafe fn plan_distinct(self) -> Result<(), QueryHostError> {
        let mut builder = DistinctPlanBuilder::new(&self);
        let Some(planned) = (unsafe { builder.build() }) else {
            return Ok(());
        };
        let rows = unsafe { self.postgres_output_rows() };
        QueryPathInstallation::new(self, planned, rows, rows).install()
    }

    unsafe fn estimate_group_rows(&self, input_rows: f64) -> f64 {
        let root = unsafe { &*self.root };
        if root.processed_groupClause.is_null() {
            return 1.0;
        }
        let parse = unsafe { &*root.parse };
        let group_exprs = unsafe {
            pg_sys::get_sortgrouplist_exprs(
                root.processed_groupClause,
                parse.targetList,
            )
        };
        unsafe {
            pg_sys::estimate_num_groups(
                self.root,
                group_exprs,
                input_rows,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        }
    }

    unsafe fn postgres_output_rows(&self) -> f64 {
        // PostgreSQL invokes the upper-path hook after it has costed at least
        // one native grouping path. Unlike `output_rel.rows`, that path's row
        // estimate already includes HAVING selectivity.
        let first_path = unsafe { pg_sys::list_nth((*self.output_rel).pathlist, 0) }
            .cast::<pg_sys::Path>();
        unsafe { (*first_path).rows }
    }
}
