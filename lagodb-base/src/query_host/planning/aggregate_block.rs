//! The grouping and aggregate outputs of one aggregate node.
//!
//! The top-level aggregate of a query block and the grouped aggregate of a
//! decorrelated scalar subquery both allocate their outputs here: each group
//! and distinct Aggref is numbered in the order added, from the block's first
//! output.
//! Outputs are scoped to the node that defines them, so two aggregate nodes
//! in one relation tree start from disjoint first outputs.

use std::ffi::{CStr, CString, c_void};

use lagodb_core::expr::{ExprType, PgOrderingDirection};
use lagodb_core::query_contract::OutputId;
use lagodb_query::plan::{
    AggCall, AggregateArguments, AggregateOrderExpr, ExecutionExpr, FixedDecimalType,
    GroupExpr, ScalarSemantics,
};
use pgrx::pg_sys;

use super::expression::{
    ExpressionScope, OutputCatalog, PredicateDomain, QueryExpressionPlanner,
};

pub(super) struct AggregateBlockBuilder<'a> {
    root: *mut pg_sys::PlannerInfo,
    expressions: &'a mut QueryExpressionPlanner,
    first_output: usize,
    groups: Vec<(*mut pg_sys::Expr, GroupExpr)>,
    aggregates: Vec<(*mut pg_sys::Expr, AggCall)>,
}

impl<'a> AggregateBlockBuilder<'a> {
    pub(super) fn new(
        root: *mut pg_sys::PlannerInfo,
        expressions: &'a mut QueryExpressionPlanner,
        first_output: usize,
    ) -> Self {
        Self {
            root,
            expressions,
            first_output,
            groups: Vec::new(),
            aggregates: Vec::new(),
        }
    }

    /// Add a grouping expression compared with `equality_operator`.
    ///
    /// # Safety
    ///
    /// `expression` must be a live expression of `root`'s planner tree.
    pub(super) unsafe fn add_group(
        &mut self,
        expression: *mut pg_sys::Expr,
        equality_operator: pg_sys::Oid,
        hashable: bool,
    ) -> Option<OutputId> {
        let result_type = QueryExpressionPlanner::expr_type(expression);
        if !ScalarSemantics::Exact.supports_group_equality(
            result_type,
            equality_operator,
            hashable,
        ) {
            return None;
        }
        let scalar = unsafe {
            self.expressions
                .lower(expression.cast(), ExpressionScope::scalar(self.root))
        }
        .ok()?;
        let output = self.next_output();
        self.groups.push((
            expression,
            GroupExpr::try_new(scalar, result_type, output).ok()?,
        ));
        Some(output)
    }

    /// The grouping output of an expression equal to a grouping expression.
    pub(super) fn group_output(
        &self,
        expression: *mut pg_sys::Expr,
    ) -> Option<OutputId> {
        self.groups.iter().find_map(|(candidate, group)| {
            unsafe {
                pg_sys::equal(candidate.cast::<c_void>(), expression.cast::<c_void>())
            }
            .then(|| group.output())
        })
    }

    pub(super) fn group(&self, output: OutputId) -> Option<&GroupExpr> {
        self.groups
            .iter()
            .map(|(_, group)| group)
            .find(|group| group.output() == output)
    }

    pub(super) fn aggregate(&self, output: OutputId) -> Option<&AggCall> {
        self.aggregates
            .iter()
            .map(|(_, aggregate)| aggregate)
            .find(|aggregate| aggregate.output() == output)
    }

    /// The output of an Aggref, built on first use. Equal Aggrefs share one
    /// aggregate.
    ///
    /// # Safety
    ///
    /// `expression` must be a live Aggref of `root`'s planner tree.
    pub(super) unsafe fn aggregate_output(
        &mut self,
        expression: *mut pg_sys::Expr,
    ) -> Option<OutputId> {
        if let Some((_, aggregate)) =
            self.aggregates.iter().find(|(existing, _)| unsafe {
                pg_sys::equal(existing.cast::<c_void>(), expression.cast::<c_void>())
            })
        {
            return Some(aggregate.output());
        }
        let output = self.next_output();
        let aggregate = unsafe {
            self.build_aggregate(expression.cast::<pg_sys::Aggref>(), output)
        }?;
        self.aggregates.push((expression, aggregate));
        Some(output)
    }

    /// Lower an expression over this block's outputs: HAVING, or a scalar
    /// subquery's target. Every Aggref it reads must already have an output.
    ///
    /// # Safety
    ///
    /// `expression` must be a live expression of `root`'s planner tree.
    pub(super) unsafe fn lower_over_outputs(
        &mut self,
        expression: *mut pg_sys::Node,
        domain: Option<PredicateDomain>,
    ) -> Option<ExecutionExpr> {
        let catalog = self.catalog();
        let scope = match domain {
            Some(domain) => {
                ExpressionScope::output_predicate(self.root, &catalog, domain)
            }
            None => ExpressionScope::output_scalar(self.root, &catalog),
        };
        unsafe { self.expressions.lower(expression, scope) }.ok()
    }

    pub(super) fn into_parts(self) -> (Box<[GroupExpr]>, Box<[AggCall]>) {
        (
            self.groups.into_iter().map(|(_, group)| group).collect(),
            self.aggregates
                .into_iter()
                .map(|(_, aggregate)| aggregate)
                .collect(),
        )
    }

    fn next_output(&self) -> OutputId {
        OutputId::from_index(
            self.first_output + self.groups.len() + self.aggregates.len(),
        )
    }

    fn catalog(&self) -> AggregateOutputCatalog {
        let entries = self
            .groups
            .iter()
            .map(|(expression, group)| {
                (*expression, group.output(), group.execution_type())
            })
            .chain(self.aggregates.iter().map(|(expression, aggregate)| {
                (
                    *expression,
                    aggregate.output(),
                    aggregate.execution_result_type(),
                )
            }))
            .map(|(expression, output, execution_type)| {
                let value = match FixedDecimalType::for_type(execution_type) {
                    Some(decimal_type) => ExecutionExpr::DecimalOutput {
                        output,
                        decimal_type,
                    },
                    None => ExecutionExpr::Output(output),
                };
                CatalogEntry {
                    expression,
                    output,
                    execution_type,
                    value,
                }
            })
            .collect();
        AggregateOutputCatalog { entries }
    }

    unsafe fn build_aggregate(
        &mut self,
        aggregate: *mut pg_sys::Aggref,
        output: OutputId,
    ) -> Option<AggCall> {
        let aggregate = unsafe { &*aggregate };
        if !aggregate.aggdirectargs.is_null()
            || aggregate.aggvariadic
            || aggregate.agglevelsup != 0
            || aggregate.aggsplit != pg_sys::AggSplit::AGGSPLIT_SIMPLE
        {
            return None;
        }
        let semantic_arguments = unsafe { Self::semantic_arguments(aggregate.args) };
        let arguments = if aggregate.aggstar {
            if !semantic_arguments.is_empty() {
                return None;
            }
            AggregateArguments::None
        } else if u32::from(aggregate.aggfnoid) == pg_sys::F_STRING_AGG_TEXT_TEXT {
            let &[value, delimiter] = semantic_arguments.as_slice() else {
                return None;
            };
            let value = unsafe {
                self.expressions
                    .lower((*value).expr.cast(), ExpressionScope::scalar(self.root))
            }
            .ok()?;
            let delimiter = unsafe { Self::string_agg_delimiter((*delimiter).expr) }?;
            AggregateArguments::StringAgg { value, delimiter }
        } else {
            let &[target] = semantic_arguments.as_slice() else {
                return None;
            };
            AggregateArguments::Unary(
                unsafe {
                    self.expressions.lower(
                        (*target).expr.cast(),
                        ExpressionScope::scalar(self.root),
                    )
                }
                .ok()?,
            )
        };
        let distinct = !aggregate.aggdistinct.is_null();
        let filter = if aggregate.aggfilter.is_null() {
            None
        } else {
            Some(
                unsafe {
                    self.expressions.lower(
                        aggregate.aggfilter.cast(),
                        ExpressionScope::predicate(self.root, PredicateDomain::Exact),
                    )
                }
                .ok()?,
            )
        };
        let call = AggCall::try_new(
            aggregate.aggfnoid,
            arguments,
            distinct,
            filter,
            ExprType {
                type_oid: aggregate.aggtype,
                typmod: unsafe {
                    pg_sys::exprTypmod(aggregate as *const _ as *const pg_sys::Node)
                },
                collation: aggregate.aggcollid,
            },
            output,
        )
        .ok()?;
        let order_by = unsafe { self.build_aggregate_order_by(aggregate, &call) }?;
        call.try_with_order_by(order_by.into_boxed_slice()).ok()
    }

    unsafe fn semantic_arguments(
        arguments: *mut pg_sys::List,
    ) -> Vec<*mut pg_sys::TargetEntry> {
        let count = unsafe { pg_sys::list_length(arguments) };
        let mut semantic = Vec::with_capacity(count as usize);
        for index in 0..count {
            let target = unsafe { pg_sys::list_nth(arguments, index) }
                .cast::<pg_sys::TargetEntry>();
            if !unsafe { (*target).resjunk } {
                semantic.push(target);
            }
        }
        semantic
    }

    unsafe fn build_aggregate_order_by(
        &mut self,
        aggregate: &pg_sys::Aggref,
        call: &AggCall,
    ) -> Option<Vec<AggregateOrderExpr>> {
        let count = unsafe { pg_sys::list_length(aggregate.aggorder) };
        let honors_order_by = call.kind().honors_order_by();
        if count > 0 && call.result_depends_on_input_order() && !honors_order_by {
            return None;
        }
        let mut order_by = if honors_order_by {
            Vec::with_capacity(count as usize)
        } else {
            Vec::new()
        };
        // An order-insensitive transition can omit sorting only after proving
        // that the skipped expression is a direct column and the comparator is
        // a side-effect-free built-in operator. Implementations that honor
        // ORDER BY retain the same proof before lowering the key into the IR.
        for index in 0..count {
            let clause = unsafe { pg_sys::list_nth(aggregate.aggorder, index) }
                .cast::<pg_sys::SortGroupClause>();
            let target = unsafe {
                pg_sys::get_sortgroupref_tle(
                    (*clause).tleSortGroupRef,
                    aggregate.args,
                )
            };
            let target_expression = unsafe { (*target).expr };
            let direct =
                unsafe { QueryExpressionPlanner::unwrap_relabel(target_expression) };
            if unsafe { (*direct).type_ } != pg_sys::NodeTag::T_Var {
                return None;
            }
            let direction =
                PgOrderingDirection::from_builtin_sort_operator(unsafe {
                    (*clause).sortop
                })?;
            if !honors_order_by {
                continue;
            }
            let expression = unsafe {
                self.expressions.lower(
                    target_expression.cast(),
                    ExpressionScope::scalar(self.root),
                )
            }
            .ok()?;
            order_by.push(
                AggregateOrderExpr::try_new(expression, direction, unsafe {
                    (*clause).nulls_first
                })
                .ok()?,
            );
        }
        Some(order_by)
    }

    unsafe fn string_agg_delimiter(expression: *mut pg_sys::Expr) -> Option<CString> {
        let expression =
            unsafe { QueryExpressionPlanner::unwrap_relabel(expression) };
        if unsafe { (*expression).type_ } != pg_sys::NodeTag::T_Const {
            return None;
        }
        let constant = expression.cast::<pg_sys::Const>();
        if unsafe { (*constant).consttype != pg_sys::TEXTOID } {
            return None;
        }
        // PostgreSQL treats a NULL STRING_AGG delimiter as no separator.
        // DataFusion requires a non-null literal delimiter, so the empty
        // literal is the equivalent representation at this plan boundary.
        if unsafe { (*constant).constisnull } {
            return Some(c"".to_owned());
        }
        let text =
            unsafe { (*constant).constvalue.cast_mut_ptr::<pg_sys::varlena>() };
        let delimiter = unsafe { pg_sys::text_to_cstring(text) };
        (!delimiter.is_null())
            .then(|| unsafe { CStr::from_ptr(delimiter).to_owned() })
    }
}

/// The grouping expressions and Aggrefs of one aggregate block, read as its
/// outputs.
struct AggregateOutputCatalog {
    entries: Vec<CatalogEntry>,
}

struct CatalogEntry {
    expression: *mut pg_sys::Expr,
    output: OutputId,
    execution_type: ExprType,
    value: ExecutionExpr,
}

impl OutputCatalog for AggregateOutputCatalog {
    unsafe fn resolve_output(
        &self,
        expression: *mut pg_sys::Expr,
    ) -> Option<&ExecutionExpr> {
        self.entries.iter().find_map(|entry| {
            unsafe {
                pg_sys::equal(
                    entry.expression.cast::<c_void>(),
                    expression.cast::<c_void>(),
                )
            }
            .then_some(&entry.value)
        })
    }

    fn output_execution_type(&self, output: OutputId) -> Option<ExprType> {
        self.entries
            .iter()
            .find(|entry| entry.output == output)
            .map(|entry| entry.execution_type)
    }
}
