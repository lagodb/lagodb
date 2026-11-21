//! Relation-scoped `PredicateFragment` negotiation for Parquet foreign scans.

use lagodb_core::datum::Utf8ServerEncoding;
use lagodb_core::diag::PgReportError;
use lagodb_core::expr::pushdown::{
    FilterPlan, FilterPlanningContext, PredicateFragment, PredicatePlan,
    PredicatePlanner, ScalarExpr,
};
use lagodb_core::expr::{
    BooleanTestKind, ColumnRef, PgComparisonOp, PushdownCosting, RuntimeValueSource,
};
use lagodb_core::handles::RelationGuard;
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::format::FormatKind;

use super::ParquetPlannedPredicate;
use super::operator::ComparisonOperator;
use super::plan::{PlannedColumn, PlannedNode};
use super::policy::{Capability, ParquetPushdownPolicy};
use super::value::ValueType;

pub(crate) struct ParquetFilterPlanner {
    columns: Box<[Option<PlannedColumn>]>,
    utf8: Option<Utf8ServerEncoding>,
}

impl ParquetFilterPlanner {
    pub(crate) fn begin(
        context: &FilterPlanningContext,
    ) -> Result<Self, ConnectorError> {
        let relation = RelationGuard::open_table(
            context.relation_oid(),
            pg_sys::NoLock as pg_sys::LOCKMODE,
        )
        .map_err(|error| {
            ConnectorError::Postgres(PgReportError::from_pg_error(error))
        })?;
        let handle = relation.as_handle();
        let mut columns = vec![None; handle.natts()];
        for column in handle.live_columns().iter() {
            let name = column.name().to_str().map_err(|_| {
                ConnectorError::invalid_object_schema(
                    FormatKind::Parquet,
                    "PostgreSQL column names must be valid UTF-8 for Parquet filters",
                )
            })?;
            columns[(column.attno() - 1) as usize] = Some(PlannedColumn {
                attno: column.attno(),
                name: name.into(),
            });
        }
        Ok(Self {
            columns: columns.into_boxed_slice(),
            utf8: Utf8ServerEncoding::resolve().ok(),
        })
    }

    /// Negotiate one fragment through the shared predicate-tree walker.
    pub(crate) fn plan_filter(
        &self,
        fragment: &PredicateFragment,
    ) -> Result<FilterPlan<ParquetPlannedPredicate>, ConnectorError> {
        let adapter = RelationPredicateAdapter {
            planner: self,
            fragment,
        };
        Ok(match fragment.root().plan_with(&adapter)? {
            PredicatePlan::Unsupported => FilterPlan::Unsupported,
            PredicatePlan::Partial(planned) => FilterPlan::partial(
                ParquetPlannedPredicate::new(planned.node),
                planned.costing,
            ),
            PredicatePlan::Exact(planned)
            | PredicatePlan::ExactNoComplement(planned) => FilterPlan::exact(
                ParquetPlannedPredicate::new(planned.node),
                planned.costing,
            ),
            // A set plans conservatively, so no exact tree holds one.
            PredicatePlan::Conservative(planned) => {
                let evaluation = planned.node.evaluation();
                FilterPlan::conservative(
                    ParquetPlannedPredicate::new(planned.node),
                    planned.costing,
                    evaluation,
                )
            }
        })
    }

    fn column(&self, column: &ColumnRef) -> Option<PlannedColumn> {
        let index = usize::try_from(column.attno - 1).ok()?;
        self.columns.get(index)?.clone()
    }

    /// Row-group statistics prune `prunable` leaves. A Conservative leaf bound
    /// from a non-constant value may be unrepresentable at bind time.
    fn costing(
        prunable: bool,
        capability: Capability,
        source: RuntimeValueSource,
    ) -> PushdownCosting {
        if !prunable
            || (capability == Capability::Conservative
                && source != RuntimeValueSource::Constant)
        {
            PushdownCosting::UncostedBestEffort
        } else {
            PushdownCosting::StatisticsPruning
        }
    }

    fn plan_comparison(
        &self,
        fragment: &PredicateFragment,
        operator: PgComparisonOp,
        left: &ScalarExpr,
        right: &ScalarExpr,
    ) -> PredicatePlan<CostedNode> {
        let (column, value, mirrored) = match (left, right) {
            (ScalarExpr::Column(column), ScalarExpr::Value(value)) => {
                (column, *value, false)
            }
            (ScalarExpr::Value(value), ScalarExpr::Column(column)) => {
                (column, *value, true)
            }
            _ => return PredicatePlan::Unsupported,
        };
        let slot = fragment.value(value);
        let Some(supported) = ParquetPushdownPolicy::comparison(
            column,
            slot,
            operator.identity(),
            self.utf8,
        ) else {
            return PredicatePlan::Unsupported;
        };
        let Some(planned_column) = self.column(column) else {
            return PredicatePlan::Unsupported;
        };
        let operator = if mirrored {
            supported.operator.mirrored()
        } else {
            supported.operator
        };
        // Floats and decimals are not pruned by row-group statistics, and
        // min/max never exclude `<>`.
        let prunable = !matches!(
            supported.value_type,
            ValueType::Float4
                | ValueType::Float8
                | ValueType::Float4OnFloat8
                | ValueType::Float8OnFloat4
                | ValueType::Decimal128(_)
        ) && operator != ComparisonOperator::NotEq;
        let planned = CostedNode {
            node: PlannedNode::Comparison {
                operator,
                column: planned_column,
                value,
                value_type: supported.value_type,
            },
            costing: Self::costing(prunable, supported.capability, slot.source_kind),
        };
        match supported.capability {
            Capability::Exact => PredicatePlan::Exact(planned),
            Capability::Conservative => PredicatePlan::Conservative(planned),
        }
    }

    fn plan_in_set(
        &self,
        fragment: &PredicateFragment,
        operator: PgComparisonOp,
        value: &ScalarExpr,
        set: &ScalarExpr,
    ) -> PredicatePlan<CostedNode> {
        let (ScalarExpr::Column(column), ScalarExpr::Value(set)) = (value, set)
        else {
            return PredicatePlan::Unsupported;
        };
        let slot = fragment.value(*set);
        let Some(element) = slot.array_element() else {
            return PredicatePlan::Unsupported;
        };
        let Some(supported) = ParquetPushdownPolicy::comparison(
            column,
            &element,
            operator.identity(),
            self.utf8,
        ) else {
            return PredicatePlan::Unsupported;
        };
        if supported.operator != ComparisonOperator::Eq
            || !ParquetPushdownPolicy::supports_set_membership(supported.value_type)
        {
            return PredicatePlan::Unsupported;
        }
        let Some(planned_column) = self.column(column) else {
            return PredicatePlan::Unsupported;
        };
        // A set prunes row groups as the OR of its equalities; the rows of
        // every selected group reach PostgreSQL, which the filter's
        // evaluation, not its costing, prices.
        PredicatePlan::Conservative(CostedNode {
            node: PlannedNode::InSet {
                column: planned_column,
                set: *set,
                value_type: supported.value_type,
            },
            costing: Self::costing(true, Capability::Conservative, slot.source_kind),
        })
    }

    fn plan_column_test(
        &self,
        value: &ScalarExpr,
        admitted: impl FnOnce(&ColumnRef) -> bool,
        prunable: bool,
        build: impl FnOnce(PlannedColumn) -> PlannedNode,
        accepted: fn(CostedNode) -> PredicatePlan<CostedNode>,
    ) -> PredicatePlan<CostedNode> {
        let ScalarExpr::Column(column) = value else {
            return PredicatePlan::Unsupported;
        };
        if !admitted(column) {
            return PredicatePlan::Unsupported;
        }
        let Some(column) = self.column(column) else {
            return PredicatePlan::Unsupported;
        };
        accepted(CostedNode {
            node: build(column),
            costing: Self::costing(
                prunable,
                Capability::Exact,
                RuntimeValueSource::Constant,
            ),
        })
    }
}

/// A planned node and its scan-volume costing.
struct CostedNode {
    node: PlannedNode,
    costing: PushdownCosting,
}

struct RelationPredicateAdapter<'a> {
    planner: &'a ParquetFilterPlanner,
    fragment: &'a PredicateFragment,
}

impl RelationPredicateAdapter<'_> {
    fn logical(
        children: Vec<CostedNode>,
        combine: fn(PushdownCosting, PushdownCosting) -> PushdownCosting,
        build: fn(Box<[PlannedNode]>) -> PlannedNode,
    ) -> CostedNode {
        let costing = children
            .iter()
            .map(|child| child.costing)
            .reduce(combine)
            .unwrap_or(PushdownCosting::StatisticsPruning);
        CostedNode {
            node: build(children.into_iter().map(|child| child.node).collect()),
            costing,
        }
    }

    fn constant(node: PlannedNode) -> PredicatePlan<CostedNode> {
        PredicatePlan::Exact(CostedNode {
            node,
            costing: PushdownCosting::StatisticsPruning,
        })
    }
}

impl PredicatePlanner<ScalarExpr, PgComparisonOp> for RelationPredicateAdapter<'_> {
    type Predicate = CostedNode;
    type Error = ConnectorError;

    fn always_true(&self) -> Result<PredicatePlan<CostedNode>, Self::Error> {
        Ok(Self::constant(PlannedNode::AlwaysTrue))
    }

    fn always_false(&self) -> Result<PredicatePlan<CostedNode>, Self::Error> {
        Ok(Self::constant(PlannedNode::AlwaysFalse))
    }

    fn strict_true(
        &self,
        value: &ScalarExpr,
    ) -> Result<PredicatePlan<CostedNode>, Self::Error> {
        Ok(self.planner.plan_column_test(
            value,
            |_| true,
            true,
            PlannedNode::IsNotNull,
            PredicatePlan::ExactNoComplement,
        ))
    }

    fn strict_false(
        &self,
        value: &ScalarExpr,
    ) -> Result<PredicatePlan<CostedNode>, Self::Error> {
        Ok(self.planner.plan_column_test(
            value,
            |_| true,
            true,
            |_| PlannedNode::AlwaysFalse,
            PredicatePlan::ExactNoComplement,
        ))
    }

    fn comparison(
        &self,
        operator: &PgComparisonOp,
        left: &ScalarExpr,
        right: &ScalarExpr,
    ) -> Result<PredicatePlan<CostedNode>, Self::Error> {
        Ok(self
            .planner
            .plan_comparison(self.fragment, *operator, left, right))
    }

    fn boolean_test(
        &self,
        kind: BooleanTestKind,
        value: &ScalarExpr,
    ) -> Result<PredicatePlan<CostedNode>, Self::Error> {
        // NOT TRUE / NOT FALSE accept NULL rows, which statistics cannot exclude.
        let prunable = !matches!(
            kind,
            BooleanTestKind::IsNotTrue | BooleanTestKind::IsNotFalse
        );
        Ok(self.planner.plan_column_test(
            value,
            ParquetPushdownPolicy::supports_boolean_test,
            prunable,
            |column| PlannedNode::BooleanTest { kind, column },
            PredicatePlan::Exact,
        ))
    }

    fn is_null(
        &self,
        value: &ScalarExpr,
    ) -> Result<PredicatePlan<CostedNode>, Self::Error> {
        Ok(self.planner.plan_column_test(
            value,
            |_| true,
            true,
            PlannedNode::IsNull,
            PredicatePlan::Exact,
        ))
    }

    fn is_not_null(
        &self,
        value: &ScalarExpr,
    ) -> Result<PredicatePlan<CostedNode>, Self::Error> {
        Ok(self.planner.plan_column_test(
            value,
            |_| true,
            true,
            PlannedNode::IsNotNull,
            PredicatePlan::Exact,
        ))
    }

    fn is_nan(
        &self,
        value: &ScalarExpr,
    ) -> Result<PredicatePlan<CostedNode>, Self::Error> {
        // Binding swaps NaN tests under NOT rather than complementing them.
        Ok(self.planner.plan_column_test(
            value,
            ParquetPushdownPolicy::supports_nan_test,
            false,
            PlannedNode::IsNan,
            PredicatePlan::Exact,
        ))
    }

    fn is_not_nan(
        &self,
        value: &ScalarExpr,
    ) -> Result<PredicatePlan<CostedNode>, Self::Error> {
        Ok(self.planner.plan_column_test(
            value,
            ParquetPushdownPolicy::supports_nan_test,
            false,
            PlannedNode::IsNotNan,
            PredicatePlan::Exact,
        ))
    }

    fn starts_with(
        &self,
        value: &ScalarExpr,
        prefix: &ScalarExpr,
    ) -> Result<PredicatePlan<CostedNode>, Self::Error> {
        let ScalarExpr::Value(prefix) = prefix else {
            return Ok(PredicatePlan::Unsupported);
        };
        let slot = self.fragment.value(*prefix);
        let utf8 = self.planner.utf8;
        Ok(self.planner.plan_column_test(
            value,
            |column| ParquetPushdownPolicy::supports_starts_with(column, slot, utf8),
            true,
            |column| PlannedNode::StartsWith {
                column,
                prefix: *prefix,
            },
            PredicatePlan::ExactNoComplement,
        ))
    }

    fn in_set(
        &self,
        operator: &PgComparisonOp,
        value: &ScalarExpr,
        set: &ScalarExpr,
    ) -> Result<PredicatePlan<CostedNode>, Self::Error> {
        Ok(self
            .planner
            .plan_in_set(self.fragment, *operator, value, set))
    }

    fn conjunction(&self, children: Vec<CostedNode>) -> CostedNode {
        Self::logical(children, PushdownCosting::conjunction, PlannedNode::And)
    }

    fn disjunction(&self, children: Vec<CostedNode>) -> CostedNode {
        Self::logical(children, PushdownCosting::disjunction, PlannedNode::Or)
    }

    fn negate(&self, child: CostedNode) -> CostedNode {
        CostedNode {
            node: PlannedNode::Not(Box::new(child.node)),
            costing: child.costing,
        }
    }
}
