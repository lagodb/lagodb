//! Shared PostgreSQL relation and expression admission gates.

use pgrx::pg_sys;

#[derive(Clone, Copy)]
pub(super) struct ScannableRelation {
    pub(super) input_rel: *mut pg_sys::RelOptInfo,
    pub(super) range_table_index: pg_sys::Index,
    pub(super) range_table_entry: *mut pg_sys::RangeTblEntry,
}

impl ScannableRelation {
    pub(super) unsafe fn inspect(
        input_rel: *mut pg_sys::RelOptInfo,
        range_table_index: pg_sys::Index,
        range_table_entry: *mut pg_sys::RangeTblEntry,
        local_relids: *mut pg_sys::Bitmapset,
        runtime_outer_relids: *mut pg_sys::Bitmapset,
    ) -> Option<Self> {
        let relation = unsafe { &*input_rel };
        let range_table_entry_ref = unsafe { &*range_table_entry };
        if relation.reloptkind != pg_sys::RelOptKind::RELOPT_BASEREL
            || range_table_entry_ref.rtekind != pg_sys::RTEKind::RTE_RELATION
            || range_table_entry_ref.relkind as u8 != pg_sys::RELKIND_RELATION
            || range_table_entry_ref.inh
            || !unsafe {
                Self::lateral_dependencies_are_available(
                    relation.lateral_relids,
                    local_relids,
                    runtime_outer_relids,
                )
            }
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

    unsafe fn lateral_dependencies_are_available(
        dependencies: *mut pg_sys::Bitmapset,
        local_relids: *mut pg_sys::Bitmapset,
        runtime_outer_relids: *mut pg_sys::Bitmapset,
    ) -> bool {
        let mut member = -1;
        loop {
            member = unsafe { pg_sys::bms_next_member(dependencies, member) };
            if member < 0 {
                return true;
            }
            if !unsafe { pg_sys::bms_is_member(member, local_relids) }
                && !unsafe { pg_sys::bms_is_member(member, runtime_outer_relids) }
            {
                return false;
            }
        }
    }
}

pub(super) struct SingleRelationCandidate;

impl SingleRelationCandidate {
    pub(super) unsafe fn restrictions_are_safe(
        input_rel: *mut pg_sys::RelOptInfo,
    ) -> bool {
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

    pub(super) unsafe fn expression_is_safe(
        _root: *mut pg_sys::PlannerInfo,
        expression: *mut pg_sys::Node,
    ) -> bool {
        // The current query path is leader-only and serial, and PG fallback
        // expressions retain their volatility classification in DataFusion.
        // A raw SubPlan is rejected because it requires PostgreSQL executor
        // state that the query engine does not own. RelationTreePlanner first
        // removes the supported EXISTS/IN shapes and represents them as joins;
        // every other expression entry point retains this rejection gate.
        // TODO(join/parallel-query): add a placement-aware parallel-hazard gate
        // before enabling a parallel query path under this execution contract.
        expression.is_null() || !unsafe { pg_sys::contain_subplans(expression) }
    }

    pub(super) unsafe fn target_list_is_safe(
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

    pub(super) unsafe fn expression_list_is_safe(
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
}
