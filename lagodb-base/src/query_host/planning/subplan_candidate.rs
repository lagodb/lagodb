//! Relation-level candidate for SubPlans lifted from base restrictions.

use std::ptr;

use pgrx::pg_sys;

use super::candidate::{ScannableRelation, SingleRelationCandidate};
use super::path_installation::QueryPathInstallation;
use super::query_shape::QueryShape;
use super::relation_plan::RelationPlanBuilder;
use super::relation_tree::RelationTreePlanner;
use crate::gucs::{QueryOffloadMode, query_offload_mode};
use crate::query_host::error::QueryHostError;

pub(super) struct SubPlanCandidate {
    root: *mut pg_sys::PlannerInfo,
    relation: ScannableRelation,
    path_target: *mut pg_sys::PathTarget,
}

impl SubPlanCandidate {
    pub(super) unsafe fn inspect(
        root: *mut pg_sys::PlannerInfo,
        relation: *mut pg_sys::RelOptInfo,
        rti: pg_sys::Index,
        rte: *mut pg_sys::RangeTblEntry,
    ) -> Option<Self> {
        if query_offload_mode() == QueryOffloadMode::Off {
            return None;
        }
        let planner = unsafe { &*root };
        // Nested scopes are folded into the outer provider-neutral tree. A
        // nested CustomPath would otherwise hide the standard relation paths
        // needed for recursive SubPlan lifting under Force mode.
        if !planner.parent_root.is_null() {
            return None;
        }
        let parse = unsafe { &*planner.parse };
        let relation_ref = unsafe { &*relation };
        if !QueryShape::new(parse).supports_subplan_root()
            || !relation_ref.lateral_relids.is_null()
            || !unsafe { SingleRelationCandidate::restrictions_are_safe(relation) }
            || !unsafe {
                SingleRelationCandidate::expression_list_is_safe(
                    root,
                    (*relation_ref.reltarget).exprs,
                )
            }
        {
            return None;
        }
        Some(Self {
            root,
            relation: unsafe {
                ScannableRelation::inspect(
                    relation,
                    rti,
                    rte,
                    relation_ref.relids,
                    ptr::null_mut(),
                )
            }?,
            path_target: relation_ref.reltarget,
        })
    }

    pub(super) unsafe fn plan(self) -> Result<(), QueryHostError> {
        let Some(relation_tree) = (unsafe {
            RelationTreePlanner::build_base(
                self.root,
                self.relation.input_rel,
                self.relation.range_table_index,
                true,
            )
        }) else {
            return Ok(());
        };
        // A plain base scan belongs to the relation provider path. This entry
        // exists only for a relation tree that actually absorbed a SubPlan.
        if !relation_tree.contains_join() {
            return Ok(());
        }
        let mut builder =
            RelationPlanBuilder::new(self.root, self.path_target, relation_tree);
        let Some(planned) = (unsafe { builder.build() }) else {
            return Ok(());
        };
        let rows = unsafe { (*self.relation.input_rel).rows };
        QueryPathInstallation::new(
            self.relation.input_rel,
            self.path_target,
            planned,
            rows,
        )
        .install()
    }
}
