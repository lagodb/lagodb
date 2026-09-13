//! Recursive relation-tree reconstruction from PostgreSQL join paths.

use std::ptr;

use lagodb_query::plan::{ExecutionExpr, JoinKey, JoinType};
use pgrx::pg_sys;

use super::{PlannedRelationTree, RelationNode, RelationTreePlanner};
use crate::query_host::planning::candidate::SingleRelationCandidate;
use crate::query_host::planning::expression::{ExpressionScope, PredicateDomain};

struct JoinLevelPredicates {
    applied: Vec<i32>,
    keys: Vec<JoinKey>,
    key_restrictions: Vec<*mut pg_sys::RestrictInfo>,
    join_restrictions: Vec<*mut pg_sys::RestrictInfo>,
    on_filters: Vec<ExecutionExpr>,
    post_filters: Vec<ExecutionExpr>,
}

struct JoinLevel {
    root: *mut pg_sys::PlannerInfo,
    relation: *mut pg_sys::RelOptInfo,
    left: RelationNode,
    right: RelationNode,
    join_type: JoinType,
    restrictions: *mut pg_sys::List,
    parameterized: *mut pg_sys::List,
    estimated_rows: Option<f64>,
}

#[derive(Clone, Copy)]
struct JoinRestrictionScope<'a> {
    root: *mut pg_sys::PlannerInfo,
    relation: *mut pg_sys::RelOptInfo,
    left: &'a RelationNode,
    right: &'a RelationNode,
    join_type: JoinType,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum JoinRestrictionPlacement {
    Match,
    PostJoin,
}

impl JoinRestrictionScope<'_> {
    /// Classify a restriction using PostgreSQL's join-qual boundary.
    ///
    /// PostgreSQL treats SEMI like INNER because SQL cannot place a pushed-down
    /// qual above a semijoin that still references its hidden inner relation.
    /// ANTI is an outer join: pushed-down quals are post-join filters and must
    /// not decide whether the preserved row has a match.
    const fn restriction_placement(
        join_type: JoinType,
        pushed_down: bool,
    ) -> JoinRestrictionPlacement {
        if pushed_down && !matches!(join_type, JoinType::Inner | JoinType::LeftSemi) {
            JoinRestrictionPlacement::PostJoin
        } else {
            JoinRestrictionPlacement::Match
        }
    }
}

impl JoinLevelPredicates {
    fn new() -> Self {
        Self {
            applied: Vec::new(),
            keys: Vec::new(),
            key_restrictions: Vec::new(),
            join_restrictions: Vec::new(),
            on_filters: Vec::new(),
            post_filters: Vec::new(),
        }
    }
}

impl RelationTreePlanner {
    /// Build the exact join alternative presented to `set_join_pathlist_hook`.
    /// Child JOINRELs are reconstructed from their cheapest standard PostgreSQL
    /// join path, so ordinary Join offload and aggregate-over-join materialize
    /// the same provider-neutral relation-tree representation.
    pub(in crate::query_host::planning) unsafe fn build_join(
        root: *mut pg_sys::PlannerInfo,
        join_rel: *mut pg_sys::RelOptInfo,
        outer_rel: *mut pg_sys::RelOptInfo,
        inner_rel: *mut pg_sys::RelOptInfo,
        join_type: JoinType,
        restrictions: *mut pg_sys::List,
        estimated_rows: f64,
    ) -> Option<PlannedRelationTree> {
        let relation = unsafe { &*join_rel };
        if relation.reloptkind != pg_sys::RelOptKind::RELOPT_JOINREL {
            return None;
        }
        let rtis = unsafe { Self::collect_base_relids(root, relation.relids) }?;
        if rtis.len() < 2 {
            return None;
        }
        let mut planner = unsafe {
            Self::for_rtis(root, rtis, relation.relids, relation.lateral_relids)
        }?;
        let left = unsafe { planner.build_selected_subtree(root, outer_rel, true) }?;
        let right = unsafe { planner.build_selected_subtree(root, inner_rel, true) }?;
        let relation_root = unsafe {
            planner.build_join_level(JoinLevel {
                root,
                relation: join_rel,
                left,
                right,
                join_type,
                restrictions,
                parameterized: ptr::null_mut(),
                estimated_rows: Some(estimated_rows),
            })
        }?;
        Some(planner.finish(relation_root))
    }

    /// Collect only scan-bearing base RTIs from a PostgreSQL relation identity.
    /// PG17 joinrel identities also contain synthetic outer-join relids; those
    /// participate in join ordering and qual placement but have no scan RTE.
    ///
    /// # Safety
    ///
    /// `root` must be the live `PlannerInfo` for the current planning callback.
    /// A non-null `relids` must be a live PostgreSQL bitmapset owned by that
    /// planner context.
    pub(super) unsafe fn collect_base_relids(
        root: *mut pg_sys::PlannerInfo,
        relids: *mut pg_sys::Bitmapset,
    ) -> Option<Vec<pg_sys::Index>> {
        if relids.is_null() {
            return None;
        }
        let base_relids = unsafe { (*root).all_baserels };
        let mut rtis =
            Vec::with_capacity(
                unsafe { pg_sys::bms_num_members(base_relids) } as usize
            );
        let mut member = -1;
        loop {
            member = unsafe { pg_sys::bms_next_member(relids, member) };
            if member < 0 {
                break;
            }
            if member > 0 && unsafe { pg_sys::bms_is_member(member, base_relids) } {
                rtis.push(member as pg_sys::Index);
            }
        }
        (!rtis.is_empty()).then_some(rtis)
    }

    pub(super) unsafe fn build_selected_subtree(
        &mut self,
        root: *mut pg_sys::PlannerInfo,
        relation: *mut pg_sys::RelOptInfo,
        allow_mark: bool,
    ) -> Option<RelationNode> {
        let relation_ref = unsafe { &*relation };
        if relation_ref.reloptkind == pg_sys::RelOptKind::RELOPT_BASEREL {
            return unsafe {
                self.build_base_subtree(
                    root,
                    relation,
                    relation_ref.relid,
                    allow_mark,
                )
            };
        }
        if relation_ref.reloptkind != pg_sys::RelOptKind::RELOPT_JOINREL {
            return None;
        }
        let path = unsafe { Self::selected_standard_join_path(relation) }?;
        let join_path = unsafe { &*path.cast::<pg_sys::JoinPath>() };
        // RIGHT_ANTI is PostgreSQL's physical orientation alternative. The IR
        // keeps one invariant: semi/anti always emit their left input.
        let (left_path, right_path, join_type) =
            if join_path.jointype == pg_sys::JoinType::JOIN_RIGHT_ANTI {
                (
                    join_path.innerjoinpath,
                    join_path.outerjoinpath,
                    JoinType::LeftAnti,
                )
            } else {
                (
                    join_path.outerjoinpath,
                    join_path.innerjoinpath,
                    Self::join_type(join_path.jointype)?,
                )
            };
        let left = unsafe {
            self.build_selected_subtree(root, (*left_path).parent, allow_mark)
        }?;
        let right = unsafe {
            self.build_selected_subtree(root, (*right_path).parent, allow_mark)
        }?;
        let inner_param = unsafe { (*right_path).param_info };
        let parameterized = if inner_param.is_null() {
            ptr::null_mut()
        } else {
            unsafe { (*inner_param).ppi_clauses }
        };
        unsafe {
            self.build_join_level(JoinLevel {
                root,
                relation,
                left,
                right,
                join_type,
                restrictions: join_path.joinrestrictinfo,
                parameterized,
                estimated_rows: None,
            })
        }
    }

    unsafe fn build_join_level(&mut self, level: JoinLevel) -> Option<RelationNode> {
        let JoinLevel {
            root,
            relation,
            left,
            right,
            join_type,
            restrictions,
            parameterized,
            estimated_rows,
        } = level;
        let scope = JoinRestrictionScope {
            root,
            relation,
            left: &left,
            right: &right,
            join_type,
        };
        let mut predicates = JoinLevelPredicates::new();
        unsafe {
            self.classify_join_restrictions(scope, restrictions, &mut predicates)
        }?;
        if predicates.keys.is_empty() && !parameterized.is_null() {
            unsafe {
                self.classify_join_restrictions(scope, parameterized, &mut predicates)
            }?;
        }
        if predicates.keys.is_empty()
            && !matches!(join_type, JoinType::LeftSemi | JoinType::LeftAnti)
        {
            return None;
        }
        let final_rows = estimated_rows.unwrap_or(unsafe { (*relation).rows });
        let estimated_key_rows = unsafe {
            self.estimate_inner_join_rows(
                root,
                &left,
                &right,
                &predicates.key_restrictions,
            )
        };
        let join_rows = if predicates.post_filters.is_empty() {
            final_rows
        } else {
            let matched_rows = unsafe {
                self.estimate_inner_join_rows(
                    root,
                    &left,
                    &right,
                    &predicates.join_restrictions,
                )
            };
            Self::pre_filter_rows(join_type, &left, &right, matched_rows, final_rows)
        };
        let join = RelationNode::Join {
            join_type,
            left: Box::new(left),
            right: Box::new(right),
            keys: predicates.keys,
            on_filters: predicates.on_filters,
            null_aware: false,
            mark_filter: None,
            estimated_key_rows,
            estimated_rows: join_rows,
        };
        if predicates.post_filters.is_empty() {
            Some(join)
        } else {
            Some(RelationNode::Filter {
                input: Box::new(join),
                predicates: predicates.post_filters,
                estimated_rows: final_rows,
            })
        }
    }

    unsafe fn classify_join_restrictions(
        &mut self,
        scope: JoinRestrictionScope<'_>,
        restrictions: *mut pg_sys::List,
        predicates: &mut JoinLevelPredicates,
    ) -> Option<()> {
        let JoinRestrictionScope {
            root,
            relation,
            left,
            right,
            join_type,
        } = scope;
        let count = unsafe { pg_sys::list_length(restrictions) };
        for index in 0..count {
            let restriction_ptr = unsafe { pg_sys::list_nth(restrictions, index) }
                .cast::<pg_sys::RestrictInfo>();
            let restriction = unsafe { &*restriction_ptr };
            if predicates.applied.contains(&restriction.rinfo_serial) {
                continue;
            }
            if restriction.pseudoconstant
                || !unsafe {
                    SingleRelationCandidate::expression_is_safe(
                        root,
                        restriction.clause.cast(),
                    )
                }
            {
                return None;
            }
            predicates.applied.push(restriction.rinfo_serial);
            let pushed_down = restriction.is_pushed_down
                || !unsafe {
                    pg_sys::bms_is_subset(
                        restriction.required_relids,
                        (*relation).relids,
                    )
                };
            let placement =
                JoinRestrictionScope::restriction_placement(join_type, pushed_down);
            if placement == JoinRestrictionPlacement::Match
                && let Ok(key) = unsafe {
                    self.expressions
                        .lower_restrictinfo_join_key(root, restriction)
                }
                && let Some(key) = unsafe { self.orient_join_key(left, right, key) }
            {
                predicates.key_restrictions.push(restriction_ptr);
                predicates.join_restrictions.push(restriction_ptr);
                if !predicates.keys.contains(&key) {
                    predicates.keys.push(key);
                }
                continue;
            }
            let predicate = unsafe {
                self.expressions.lower(
                    restriction.clause.cast(),
                    ExpressionScope::predicate(root, PredicateDomain::Exact),
                )
            }
            .ok()?;
            match placement {
                JoinRestrictionPlacement::Match => {
                    predicates.join_restrictions.push(restriction_ptr);
                    predicates.on_filters.push(predicate);
                }
                JoinRestrictionPlacement::PostJoin => {
                    predicates.post_filters.push(predicate);
                }
            }
        }
        Some(())
    }

    fn join_type(join_type: pg_sys::JoinType::Type) -> Option<JoinType> {
        match join_type {
            pg_sys::JoinType::JOIN_INNER => Some(JoinType::Inner),
            pg_sys::JoinType::JOIN_LEFT => Some(JoinType::Left),
            pg_sys::JoinType::JOIN_RIGHT => Some(JoinType::Right),
            pg_sys::JoinType::JOIN_FULL => Some(JoinType::Full),
            pg_sys::JoinType::JOIN_SEMI => Some(JoinType::LeftSemi),
            pg_sys::JoinType::JOIN_ANTI => Some(JoinType::LeftAnti),
            // RIGHT SEMI is introduced after the PG17 target used by LagoDB.
            // When PG18 support is added it must be normalized by swapping
            // inputs, just like PG17's RIGHT_ANTI alternative above.
            _ => None,
        }
    }

    unsafe fn selected_standard_join_path(
        relation: *mut pg_sys::RelOptInfo,
    ) -> Option<*mut pg_sys::Path> {
        let paths = unsafe { (*relation).pathlist };
        let count = unsafe { pg_sys::list_length(paths) };
        let mut selected = None;
        for index in 0..count {
            let candidate =
                unsafe { pg_sys::list_nth(paths, index) }.cast::<pg_sys::Path>();
            if candidate.is_null() {
                continue;
            }
            let Some(join) = (unsafe { Self::unwrap_path(candidate) }) else {
                continue;
            };
            let parameter_info = unsafe { (*join).param_info };
            let required_outer = if parameter_info.is_null() {
                ptr::null_mut()
            } else {
                unsafe { (*parameter_info).ppi_req_outer }
            };
            if !unsafe {
                pg_sys::bms_equal(required_outer, (*relation).lateral_relids)
            } || !matches!(
                unsafe { (*join).type_ },
                pg_sys::NodeTag::T_NestPath
                    | pg_sys::NodeTag::T_MergePath
                    | pg_sys::NodeTag::T_HashPath
            ) {
                continue;
            }
            let total_cost = unsafe { (*candidate).total_cost };
            if selected.is_none_or(|(_, best_cost)| total_cost < best_cost) {
                selected = Some((join, total_cost));
            }
        }
        selected.map(|(path, _)| path)
    }

    unsafe fn unwrap_path(mut path: *mut pg_sys::Path) -> Option<*mut pg_sys::Path> {
        loop {
            if path.is_null() {
                return None;
            }
            path = match unsafe { (*path).type_ } {
                pg_sys::NodeTag::T_GatherPath => unsafe {
                    (*path.cast::<pg_sys::GatherPath>()).subpath
                },
                pg_sys::NodeTag::T_GatherMergePath => unsafe {
                    (*path.cast::<pg_sys::GatherMergePath>()).subpath
                },
                pg_sys::NodeTag::T_SortPath => unsafe {
                    (*path.cast::<pg_sys::SortPath>()).subpath
                },
                pg_sys::NodeTag::T_MaterialPath => unsafe {
                    (*path.cast::<pg_sys::MaterialPath>()).subpath
                },
                pg_sys::NodeTag::T_ProjectionPath => unsafe {
                    (*path.cast::<pg_sys::ProjectionPath>()).subpath
                },
                _ => return Some(path),
            };
        }
    }
}
