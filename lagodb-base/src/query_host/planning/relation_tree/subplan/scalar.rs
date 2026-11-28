//! Correlated scalar subqueries decorrelated into a LEFT join to an aggregate.
//!
//! PostgreSQL never pulls up an EXPR_SUBLINK. It plans the subquery once and
//! runs it per outer row, after setting PARAM_EXEC `parParam[i]` to the value
//! of `args[i]` (`build_subplan`, subselect.c). When each such Param occurs
//! only in inner equalities `inner_column = Param`, the subquery's value for
//! an outer row is its aggregate over the inner rows whose columns equal that
//! row's arguments. Grouping the aggregate by those inner columns and
//! LEFT-joining it on `argument = group` yields that value for every outer
//! row with a matching group. An outer row without one reads NULL, which is
//! the subquery's value over no rows only when the target is NULL on empty
//! input: an aggregate other than COUNT, alone or in strict decimal
//! arithmetic with constants. COUNT and every target that maps NULL to a
//! value decline.

use std::ffi::c_int;
use std::ptr;

use lagodb_core::expr::{ColumnRef, ExprType, PgComparisonOp};
use lagodb_query::plan::{
    AggCall, ExecutionExpr, JoinKey, JoinType, OutputId, RelationValueRef,
};
use pgrx::pg_sys;

use super::super::{RelationNode, RelationTreePlanner};
use crate::query_host::planning::aggregate_block::AggregateBlockBuilder;
use crate::query_host::planning::expression::{
    ExpressionScope, OutputCatalog, PredicateDomain, QueryExpressionPlanner,
};
use crate::query_host::planning::planner_info::PlannerInfoExt;
use crate::query_host::planning::query_shape::QueryShape;

/// A restriction comparing a value with a correlated scalar subquery.
#[derive(Clone, Copy)]
pub(in crate::query_host::planning::relation_tree) struct LiftedScalarSubPlan {
    restriction: *mut pg_sys::RestrictInfo,
    subplan: *mut pg_sys::SubPlan,
    inner_root: *mut pg_sys::PlannerInfo,
}

impl LiftedScalarSubPlan {
    pub(in crate::query_host::planning::relation_tree) const fn restriction(
        self,
    ) -> *mut pg_sys::RestrictInfo {
        self.restriction
    }
}

/// One inner equality `inner = Param` and the outer column setting Param.
struct Correlation {
    inner: *mut pg_sys::Expr,
    operator: *mut pg_sys::OpExpr,
    outer: ColumnRef,
}

/// The SubPlan of the lowered restriction, read as the subquery's value over
/// the joined aggregate's outputs.
struct ScalarSubqueryCatalog {
    subplan: *mut pg_sys::SubPlan,
    value: ExecutionExpr,
}

impl OutputCatalog for ScalarSubqueryCatalog {
    unsafe fn resolve_output(
        &self,
        expression: *mut pg_sys::Expr,
    ) -> Option<&ExecutionExpr> {
        // Lowering walks the restriction's own tree, so the SubPlan is this
        // exact node.
        ptr::eq(expression.cast::<pg_sys::SubPlan>(), self.subplan)
            .then_some(&self.value)
    }

    /// The value is compared in the exact domain, where a NUMERIC aggregate
    /// output compares only as a fixed decimal, whose `DecimalOutput` carries
    /// its type; an output of any other representation declines there.
    fn output_execution_type(&self, _output: OutputId) -> Option<ExprType> {
        None
    }
}

impl RelationTreePlanner {
    /// An operator clause one of whose two arguments is an EXPR_SUBLINK
    /// SubPlan, the other containing no SubPlan.
    pub(in crate::query_host::planning::relation_tree) unsafe fn extract_scalar_subplan(
        root: *mut pg_sys::PlannerInfo,
        restriction: *mut pg_sys::RestrictInfo,
    ) -> Option<LiftedScalarSubPlan> {
        let clause = unsafe { (*restriction).clause };
        if unsafe { (*clause).type_ } != pg_sys::NodeTag::T_OpExpr {
            return None;
        }
        let arguments = unsafe { (*clause.cast::<pg_sys::OpExpr>()).args };
        if unsafe { pg_sys::list_length(arguments) } != 2 {
            return None;
        }
        let [first, second] = [0, 1].map(|index| unsafe {
            pg_sys::list_nth(arguments, index).cast::<pg_sys::Node>()
        });
        let is_scalar_subplan = |node: *mut pg_sys::Node| unsafe {
            (*node).type_ == pg_sys::NodeTag::T_SubPlan
                && (*node.cast::<pg_sys::SubPlan>()).subLinkType
                    == pg_sys::SubLinkType::EXPR_SUBLINK
        };
        let (subplan, other) = if is_scalar_subplan(first) {
            (first, second)
        } else if is_scalar_subplan(second) {
            (second, first)
        } else {
            return None;
        };
        if unsafe { pg_sys::contain_subplans(other) } {
            return None;
        }
        let subplan = subplan.cast::<pg_sys::SubPlan>();
        let inner_root = unsafe { Self::subplan_root(root, subplan) }?;
        Some(LiftedScalarSubPlan {
            restriction,
            subplan,
            inner_root,
        })
    }

    /// Replace `left`'s restriction over the subquery with a LEFT join to the
    /// subquery's grouped aggregate and a Filter over the joined value.
    pub(in crate::query_host::planning::relation_tree) unsafe fn wrap_scalar_subplan(
        &mut self,
        outer_root: *mut pg_sys::PlannerInfo,
        left: RelationNode,
        lifted: LiftedScalarSubPlan,
        estimated_rows: f64,
    ) -> Option<RelationNode> {
        let LiftedScalarSubPlan {
            restriction,
            subplan,
            inner_root,
        } = lifted;
        let parameters = unsafe { (*subplan).parParam };
        // PostgreSQL re-executes the subquery per outer row, while the join
        // evaluates it once for every outer row of a group; like its sublink
        // pull-up (`convert_EXISTS_sublink_to_join`), decline volatile
        // subqueries. The walker descends the whole Query.
        if parameters.is_null()
            || !unsafe { (*subplan).testexpr.is_null() }
            || !QueryShape::new(unsafe { &*(*inner_root).parse })
                .supports_scalar_aggregate_subquery()
            || unsafe {
                pg_sys::contain_volatile_functions((*inner_root).parse.cast())
            }
        {
            return None;
        }
        let rtis = unsafe {
            Self::collect_base_relids(inner_root, (*inner_root).final_join_relids())
        }?;
        let &[rti] = rtis.as_slice() else {
            return None;
        };
        let target = unsafe { Self::scalar_target(inner_root) }?;
        if unsafe { pg_sys::contain_exec_param(target.cast(), parameters) } {
            return None;
        }
        unsafe { self.add_scope(inner_root, rtis) }?;
        let binding = self.binding(inner_root, rti)?;
        let (correlations, uncorrelated) = unsafe {
            self.correlations(
                outer_root,
                subplan,
                (*binding.relation.input_rel).baserestrictinfo,
            )
        }?;
        if correlations
            .iter()
            .any(|correlation| !left.emits(correlation.outer.scan))
        {
            return None;
        }
        // The subquery has no SubLinks, so no restriction lifts a SubPlan;
        // removing the correlations leaves the scan's row estimate stale.
        let input = unsafe {
            self.build_scan_from_restrictions(
                inner_root,
                binding,
                uncorrelated,
                false,
                true,
            )
        }?;

        let first_output = self.next_output;
        let mut block = AggregateBlockBuilder::new(
            inner_root,
            &mut self.expressions,
            first_output,
        );
        let mut keys = Vec::with_capacity(correlations.len());
        let mut group_expressions = ptr::null_mut();
        for correlation in &correlations {
            let opno = unsafe { (*correlation.operator).opno };
            let output = match block.group_output(correlation.inner) {
                Some(output) => output,
                None => {
                    let hashable = unsafe {
                        pg_sys::op_hashjoinable(
                            opno,
                            pg_sys::exprType(correlation.inner.cast()),
                        )
                    };
                    group_expressions = unsafe {
                        pg_sys::lappend(group_expressions, correlation.inner.cast())
                    };
                    unsafe { block.add_group(correlation.inner, opno, hashable) }?
                }
            };
            let value_type = block.group(output)?.result_type();
            keys.push(
                JoinKey::try_new(
                    RelationValueRef::Column(correlation.outer),
                    RelationValueRef::Output { output, value_type },
                    unsafe { Self::comparison_operator(correlation.operator) },
                )
                .ok()?,
            );
        }
        let references = unsafe {
            pg_sys::pull_var_clause(
                target.cast(),
                pg_sys::PVC_INCLUDE_AGGREGATES as c_int,
            )
        };
        for index in 0..unsafe { pg_sys::list_length(references) } {
            let reference =
                unsafe { pg_sys::list_nth(references, index) }.cast::<pg_sys::Expr>();
            if unsafe { (*reference).type_ } != pg_sys::NodeTag::T_Aggref {
                return None;
            }
            unsafe { block.aggregate_output(reference) }?;
        }
        let value = unsafe { block.lower_over_outputs(target.cast(), None) }?;
        if !Self::is_null_on_empty(&value, &block) {
            return None;
        }
        let (groups, aggregates) = block.into_parts();
        self.next_output = first_output + groups.len() + aggregates.len();
        for key in &keys {
            self.expressions.record_join_key(*key).ok()?;
        }

        let input_rows = input.estimated_rows();
        let estimated_width = groups
            .iter()
            .map(|group| group.result_type())
            .chain(aggregates.iter().map(|aggregate| aggregate.result_type()))
            .map(|value_type| unsafe {
                pg_sys::get_typavgwidth(value_type.type_oid, value_type.typmod)
            })
            .sum();
        let aggregate = RelationNode::Aggregate {
            input: Box::new(input),
            groups,
            aggregates,
            estimated_rows: unsafe {
                pg_sys::estimate_num_groups(
                    inner_root,
                    group_expressions,
                    input_rows,
                    ptr::null_mut(),
                    ptr::null_mut(),
                )
            },
            estimated_width,
        };
        let catalog = ScalarSubqueryCatalog { subplan, value };
        let predicate = unsafe {
            self.expressions.lower(
                (*restriction).clause.cast(),
                ExpressionScope::output_predicate(
                    outer_root,
                    &catalog,
                    PredicateDomain::Exact,
                ),
            )
        }
        .ok()?;
        // Groups are distinct, so each outer row matches at most one.
        let outer_rows = left.estimated_rows();
        Some(RelationNode::Filter {
            input: Box::new(RelationNode::Join {
                join_type: JoinType::Left,
                left: Box::new(left),
                right: Box::new(aggregate),
                keys,
                on_filters: Vec::new(),
                null_aware: false,
                mark_filter: None,
                estimated_key_rows: outer_rows,
                estimated_rows: outer_rows,
            }),
            predicates: vec![predicate],
            estimated_rows,
        })
    }

    /// Split the inner restrictions into correlation equalities, one or more
    /// per Param, and the remaining restrictions, which must not read any
    /// Param.
    unsafe fn correlations(
        &self,
        outer_root: *mut pg_sys::PlannerInfo,
        subplan: *mut pg_sys::SubPlan,
        restrictions: *mut pg_sys::List,
    ) -> Option<(Vec<Correlation>, *mut pg_sys::List)> {
        let parameters = unsafe { (*subplan).parParam };
        let parameter_count = unsafe { pg_sys::list_length(parameters) };
        let mut correlated = vec![false; parameter_count as usize];
        let mut correlations = Vec::new();
        let mut uncorrelated = ptr::null_mut();
        for index in 0..unsafe { pg_sys::list_length(restrictions) } {
            let restriction = unsafe { pg_sys::list_nth(restrictions, index) }
                .cast::<pg_sys::RestrictInfo>();
            let clause = unsafe { (*restriction).clause };
            if !unsafe { pg_sys::contain_exec_param(clause.cast(), parameters) } {
                uncorrelated =
                    unsafe { pg_sys::lappend(uncorrelated, restriction.cast()) };
                continue;
            }
            let (inner, parameter) = unsafe { Self::correlation_operands(clause) }?;
            let position = (0..parameter_count).find(|&position| unsafe {
                pg_sys::list_nth_int(parameters, position) == parameter
            })?;
            let argument = unsafe { pg_sys::list_nth((*subplan).args, position) }
                .cast::<pg_sys::Expr>();
            let outer =
                unsafe { self.expressions.inspect_join_column(outer_root, argument) }
                    .ok()?;
            correlated[position as usize] = true;
            correlations.push(Correlation {
                inner,
                operator: clause.cast(),
                outer,
            });
        }
        correlated
            .into_iter()
            .all(|correlated| correlated)
            .then_some((correlations, uncorrelated))
    }

    /// `inner_column = Param` in either argument order: the inner column
    /// expression and the PARAM_EXEC id.
    unsafe fn correlation_operands(
        clause: *mut pg_sys::Expr,
    ) -> Option<(*mut pg_sys::Expr, i32)> {
        if unsafe { (*clause).type_ } != pg_sys::NodeTag::T_OpExpr {
            return None;
        }
        let arguments = unsafe { (*clause.cast::<pg_sys::OpExpr>()).args };
        if unsafe { pg_sys::list_length(arguments) } != 2 {
            return None;
        }
        let [first, second] = [0, 1].map(|index| unsafe {
            pg_sys::list_nth(arguments, index).cast::<pg_sys::Expr>()
        });
        let parameter = |expression: *mut pg_sys::Expr| {
            let direct =
                unsafe { QueryExpressionPlanner::unwrap_relabel(expression) };
            (unsafe { (*direct).type_ } == pg_sys::NodeTag::T_Param)
                .then(|| direct.cast::<pg_sys::Param>())
                .filter(|&parameter| unsafe {
                    (*parameter).paramkind == pg_sys::ParamKind::PARAM_EXEC
                })
                .map(|parameter| unsafe { (*parameter).paramid })
        };
        let is_column = |expression: *mut pg_sys::Expr| unsafe {
            (*QueryExpressionPlanner::unwrap_relabel(expression)).type_
                == pg_sys::NodeTag::T_Var
        };
        match (parameter(first), parameter(second)) {
            (None, Some(parameter)) if is_column(first) => Some((first, parameter)),
            (Some(parameter), None) if is_column(second) => Some((second, parameter)),
            _ => None,
        }
    }

    /// The subquery's one output expression.
    unsafe fn scalar_target(
        inner_root: *mut pg_sys::PlannerInfo,
    ) -> Option<*mut pg_sys::Expr> {
        let target_list = unsafe { (*(*inner_root).parse).targetList };
        let mut targets = (0..unsafe { pg_sys::list_length(target_list) })
            .map(|index| unsafe {
                pg_sys::list_nth(target_list, index).cast::<pg_sys::TargetEntry>()
            })
            .filter(|&target| unsafe { !(*target).resjunk });
        let target = targets.next()?;
        targets.next().is_none().then(|| unsafe { (*target).expr })
    }

    /// Whether `value` is NULL whenever the aggregate reads no row: an
    /// aggregate output NULL on empty input, or strict decimal arithmetic of
    /// such an output with a constant. The target then reads a single
    /// aggregate.
    fn is_null_on_empty(
        value: &ExecutionExpr,
        block: &AggregateBlockBuilder<'_>,
    ) -> bool {
        match value {
            ExecutionExpr::Output(output)
            | ExecutionExpr::DecimalOutput { output, .. } => block
                .aggregate(*output)
                .is_some_and(AggCall::is_null_on_empty),
            ExecutionExpr::DecimalArithmetic { left, right, .. } => {
                let constant = |operand: &ExecutionExpr| {
                    matches!(operand, ExecutionExpr::DecimalValue { .. })
                };
                (Self::is_null_on_empty(left, block) && constant(right))
                    || (constant(left) && Self::is_null_on_empty(right, block))
            }
            _ => false,
        }
    }

    unsafe fn comparison_operator(operator: *mut pg_sys::OpExpr) -> PgComparisonOp {
        let operator = unsafe { &*operator };
        PgComparisonOp {
            opno: operator.opno,
            opfuncid: operator.opfuncid,
            opresulttype: operator.opresulttype,
            opcollid: operator.opcollid,
            inputcollid: operator.inputcollid,
        }
    }
}
