//! PostgreSQL planner-shape recognition for liftable SubPlans.

use pgrx::pg_sys;

use super::LiftedMarkSubPlan;
use crate::query_host::planning::expression::QueryExpressionPlanner;
use crate::query_host::planning::query_shape::QueryShape;
use crate::query_host::planning::relation_tree::RelationTreePlanner;

impl RelationTreePlanner {
    pub(super) unsafe fn extract_subplan(
        root: *mut pg_sys::PlannerInfo,
        node: *mut pg_sys::Node,
    ) -> Option<(*mut pg_sys::SubPlan, bool, *mut pg_sys::PlannerInfo)> {
        if node.is_null() {
            return None;
        }
        let mut current = node;
        let mut anti = false;
        if unsafe { (*current).type_ } == pg_sys::NodeTag::T_BoolExpr {
            let boolean = current.cast::<pg_sys::BoolExpr>();
            if unsafe { (*boolean).boolop } == pg_sys::BoolExprType::NOT_EXPR
                && unsafe { pg_sys::list_length((*boolean).args) } == 1
            {
                current = unsafe { pg_sys::list_nth((*boolean).args, 0) }.cast();
                anti = true;
            }
        }
        if unsafe { (*current).type_ } == pg_sys::NodeTag::T_AlternativeSubPlan {
            let alternatives =
                unsafe { (*current.cast::<pg_sys::AlternativeSubPlan>()).subplans };
            if unsafe { pg_sys::list_length(alternatives) } == 0 {
                return None;
            }
            current = unsafe { pg_sys::list_nth(alternatives, 0) }.cast();
        }
        if unsafe { (*current).type_ } != pg_sys::NodeTag::T_SubPlan {
            return None;
        }
        let subplan = current.cast::<pg_sys::SubPlan>();
        if !matches!(
            unsafe { (*subplan).subLinkType },
            pg_sys::SubLinkType::EXISTS_SUBLINK | pg_sys::SubLinkType::ANY_SUBLINK
        ) {
            return None;
        }
        let plan_id = unsafe { (*subplan).plan_id };
        let planner = unsafe { &*root };
        if plan_id <= 0
            || planner.glob.is_null()
            || plan_id > unsafe { pg_sys::list_length((*planner.glob).subroots) }
        {
            return None;
        }
        let inner_root = unsafe {
            pg_sys::list_nth((*planner.glob).subroots, plan_id - 1)
                .cast::<pg_sys::PlannerInfo>()
        };
        (!inner_root.is_null()).then_some((subplan, anti, inner_root))
    }

    pub(super) unsafe fn extract_mark_subplan(
        root: *mut pg_sys::PlannerInfo,
        restriction: *mut pg_sys::RestrictInfo,
        node: *mut pg_sys::Node,
    ) -> Option<LiftedMarkSubPlan> {
        if node.is_null() || unsafe { (*node).type_ } != pg_sys::NodeTag::T_BoolExpr {
            return None;
        }
        let boolean = node.cast::<pg_sys::BoolExpr>();
        if unsafe { (*boolean).boolop } != pg_sys::BoolExprType::OR_EXPR
            || unsafe { pg_sys::list_length((*boolean).args) } != 2
        {
            return None;
        }
        let first = unsafe {
            Self::unwrap_restriction(pg_sys::list_nth((*boolean).args, 0).cast())
        };
        let second = unsafe {
            Self::unwrap_restriction(pg_sys::list_nth((*boolean).args, 1).cast())
        };
        unsafe { Self::match_mark_branches(root, restriction, first, second) }
            .or_else(|| unsafe {
                Self::match_mark_branches(root, restriction, second, first)
            })
    }

    unsafe fn match_mark_branches(
        root: *mut pg_sys::PlannerInfo,
        restriction: *mut pg_sys::RestrictInfo,
        null_branch: *mut pg_sys::Node,
        subplan_branch: *mut pg_sys::Node,
    ) -> Option<LiftedMarkSubPlan> {
        if null_branch.is_null()
            || unsafe { (*null_branch).type_ } != pg_sys::NodeTag::T_NullTest
        {
            return None;
        }
        let null_test = null_branch.cast::<pg_sys::NullTest>();
        if unsafe { (*null_test).nulltesttype } != pg_sys::NullTestType::IS_NULL
            || unsafe { (*null_test).arg.is_null() }
        {
            return None;
        }
        let null_expression =
            unsafe { QueryExpressionPlanner::unwrap_relabel((*null_test).arg) };
        if unsafe { (*null_expression).type_ } != pg_sys::NodeTag::T_Var {
            return None;
        }
        let null_var = null_expression.cast::<pg_sys::Var>();
        if unsafe { (*null_var).varlevelsup } != 0
            || unsafe { (*null_var).varattno } <= 0
        {
            return None;
        }
        let (subplan, anti, inner_root) =
            unsafe { Self::extract_subplan(root, subplan_branch) }?;
        // Keep only the concrete OR shape consumed immediately after LeftMark.
        // For NOT IN, lowering also treats an inner-key NULL as a Mark match;
        // this preserves UNKNOWN for non-null outer keys even though DataFusion's
        // synthetic Mark column itself is a non-null boolean.
        if unsafe { (*subplan).subLinkType } != pg_sys::SubLinkType::ANY_SUBLINK
            || unsafe { (*subplan).testexpr.is_null() }
            || !unsafe {
                Self::testexpr_references_var(
                    (*subplan).testexpr,
                    (*null_var).varno as pg_sys::Index,
                    (*null_var).varattno,
                )
            }
        {
            return None;
        }
        Some(LiftedMarkSubPlan {
            restriction,
            subplan,
            inner_root,
            null_varno: unsafe { (*null_var).varno as pg_sys::Index },
            null_attno: unsafe { (*null_var).varattno },
            anti,
        })
    }

    unsafe fn testexpr_references_var(
        expression: *mut pg_sys::Node,
        varno: pg_sys::Index,
        attno: pg_sys::AttrNumber,
    ) -> bool {
        if expression.is_null()
            || unsafe { (*expression).type_ } != pg_sys::NodeTag::T_OpExpr
        {
            return false;
        }
        let operator = expression.cast::<pg_sys::OpExpr>();
        if unsafe { pg_sys::list_length((*operator).args) } != 2 {
            return false;
        }
        (0..2).any(|index| {
            let expression = unsafe {
                pg_sys::list_nth((*operator).args, index).cast::<pg_sys::Expr>()
            };
            let expression =
                unsafe { QueryExpressionPlanner::unwrap_relabel(expression) };
            unsafe {
                (*expression).type_ == pg_sys::NodeTag::T_Var
                    && (*expression.cast::<pg_sys::Var>()).varlevelsup == 0
                    && (*expression.cast::<pg_sys::Var>()).varno as pg_sys::Index
                        == varno
                    && (*expression.cast::<pg_sys::Var>()).varattno == attno
            }
        })
    }

    unsafe fn unwrap_restriction(node: *mut pg_sys::Node) -> *mut pg_sys::Node {
        if !node.is_null()
            && unsafe { (*node).type_ } == pg_sys::NodeTag::T_RestrictInfo
        {
            unsafe { (*node.cast::<pg_sys::RestrictInfo>()).clause.cast() }
        } else {
            node
        }
    }

    pub(super) unsafe fn subquery_shape_is_supported(
        root: *mut pg_sys::PlannerInfo,
    ) -> bool {
        let parse = unsafe { &*(*root).parse };
        QueryShape::new(parse).supports_lifted_subquery()
    }

    pub(super) unsafe fn find_final_relation(
        root: *mut pg_sys::PlannerInfo,
    ) -> Option<*mut pg_sys::RelOptInfo> {
        let count = unsafe { pg_sys::list_length((*root).join_rel_list) };
        for index in 0..count {
            let relation = unsafe {
                pg_sys::list_nth((*root).join_rel_list, index)
                    .cast::<pg_sys::RelOptInfo>()
            };
            if !relation.is_null()
                && unsafe {
                    pg_sys::bms_equal((*relation).relids, (*root).all_query_rels)
                }
            {
                return Some(relation);
            }
        }
        let rtis =
            unsafe { Self::collect_base_relids(root, (*root).all_query_rels) }?;
        let [rti] = rtis.as_slice() else {
            return None;
        };
        let relation = unsafe { *(*root).simple_rel_array.add(*rti as usize) };
        (!relation.is_null()).then_some(relation)
    }

    pub(super) unsafe fn subplan_output(
        inner_root: *mut pg_sys::PlannerInfo,
    ) -> Option<*mut pg_sys::Expr> {
        let target_list = unsafe { (*(*inner_root).parse).targetList };
        let count = unsafe { pg_sys::list_length(target_list) };
        for index in 0..count {
            let target = unsafe { pg_sys::list_nth(target_list, index) }
                .cast::<pg_sys::TargetEntry>();
            if !unsafe { (*target).resjunk } {
                let expression =
                    unsafe { QueryExpressionPlanner::unwrap_relabel((*target).expr) };
                return (unsafe { (*expression).type_ } == pg_sys::NodeTag::T_Var)
                    .then_some(unsafe { (*target).expr });
            }
        }
        None
    }
}
