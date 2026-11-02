//! PostgreSQL cardinality adapter for relation-tree operators.

use std::ptr;

use lagodb_query::plan::JoinType;
use pgrx::pg_sys;

use super::{RelationNode, RelationTreePlanner};

impl RelationTreePlanner {
    /// PostgreSQL's `approx_tuple_count` model for a selected set of join
    /// restrictions under INNER semantics. It supplies both hash-key input
    /// rows and, when a post-filter exists, the Join node's pre-filter rows.
    /// Only this adapter touches PostgreSQL planner statistics.
    pub(super) unsafe fn estimate_inner_join_rows(
        &self,
        root: *mut pg_sys::PlannerInfo,
        left: &RelationNode,
        right: &RelationNode,
        restrictions: &[*mut pg_sys::RestrictInfo],
    ) -> f64 {
        let left_relids = unsafe { left.add_relids(ptr::null_mut()) };
        let right_relids = unsafe { right.add_relids(ptr::null_mut()) };
        let mut join_info = Self::dummy_join_info(left_relids, right_relids);
        let selectivity = restrictions.iter().fold(1.0, |value, restriction| {
            value
                * unsafe {
                    pg_sys::clause_selectivity(
                        root,
                        (*restriction).cast(),
                        0,
                        pg_sys::JoinType::JOIN_INNER,
                        &mut join_info,
                    )
                }
        });
        let rows = unsafe {
            pg_sys::clamp_row_est(
                left.estimated_rows() * right.estimated_rows() * selectivity,
            )
        };
        unsafe {
            pg_sys::bms_free(left_relids);
            pg_sys::bms_free(right_relids);
        }
        rows
    }

    /// Build the placeholder inner-join descriptor selectivity estimation
    /// expects. PostgreSQL 16 keeps `init_dummy_sjinfo` private; the zeroed
    /// remainder matches what it assigns.
    #[cfg(feature = "pg16")]
    fn dummy_join_info(
        left_relids: pg_sys::Relids,
        right_relids: pg_sys::Relids,
    ) -> pg_sys::SpecialJoinInfo {
        pg_sys::SpecialJoinInfo {
            type_: pg_sys::NodeTag::T_SpecialJoinInfo,
            min_lefthand: left_relids,
            min_righthand: right_relids,
            syn_lefthand: left_relids,
            syn_righthand: right_relids,
            jointype: pg_sys::JoinType::JOIN_INNER,
            ..Default::default()
        }
    }

    /// Build the placeholder inner-join descriptor selectivity estimation
    /// expects.
    #[cfg(any(feature = "pg17", feature = "pg18"))]
    fn dummy_join_info(
        left_relids: pg_sys::Relids,
        right_relids: pg_sys::Relids,
    ) -> pg_sys::SpecialJoinInfo {
        let mut join_info = pg_sys::SpecialJoinInfo::default();
        // SAFETY: PostgreSQL only records the relid pointers it receives here.
        unsafe {
            pg_sys::init_dummy_sjinfo(&mut join_info, left_relids, right_relids);
        }
        join_info
    }

    pub(super) fn pre_filter_rows(
        join_type: JoinType,
        left: &RelationNode,
        right: &RelationNode,
        matched_rows: f64,
        final_rows: f64,
    ) -> f64 {
        match join_type {
            JoinType::Inner => final_rows.max(matched_rows),
            JoinType::Left => final_rows.max(matched_rows).max(left.estimated_rows()),
            JoinType::Right => {
                final_rows.max(matched_rows).max(right.estimated_rows())
            }
            JoinType::Full => final_rows
                .max(matched_rows)
                .max(left.estimated_rows())
                .max(right.estimated_rows()),
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
                final_rows.max(left.estimated_rows())
            }
        }
    }
}
