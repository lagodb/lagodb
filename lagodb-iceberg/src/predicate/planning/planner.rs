//! Relation-scoped `PredicateFragment` to Iceberg planned-predicate conversion.

use std::sync::Arc;

use iceberg_lite::spec::{
    NestedField, PrimitiveType, Schema as IcebergSchema, TableMetadata, Type,
};
use lagodb_core::datum::{Utf8ServerEncoding, numeric_typmod};
use lagodb_core::expr::pushdown::{
    FilterPlan, FilterPlanningContext, FilterPushdownPlanner, PredicateFragment,
    PredicatePlan, PredicatePlanner, ScalarExpr,
};
use lagodb_core::expr::{
    BooleanTestKind, ColumnRef, PgComparisonKind, PgComparisonOp, PushdownCosting,
    RuntimeValueSpec,
};
use lagodb_core::handles::RelationGuard;
use pgrx::pg_sys;

use crate::error::{IcebergError, IcebergResult};
use crate::predicate::ir::{
    IcebergPlannedPredicate, PlannedColumn, PlannedNode, PlannedValueType,
};
use crate::predicate::semantics::PredicateCapability;
use crate::schema::relation::{LiveColumn, RelationLayout};

use super::PredicateStorageContract;
use super::admission::{PgPredicateAdmission, SupportedComparison};
use super::costing::PredicateCostModel;

/// Borrowed local/remote column binding resolved once per predicate leaf.
#[derive(Clone, Copy)]
struct ResolvedColumn<'a> {
    live: &'a LiveColumn,
    field: &'a NestedField,
}

impl ResolvedColumn<'_> {
    fn to_planned(self) -> PlannedColumn {
        PlannedColumn {
            field_id: self.field.id,
            display_name: self.live.name.clone(),
        }
    }
}

/// One admission-accepted comparison resolved against the planner schema.
struct ResolvedComparison<'a> {
    supported: SupportedComparison,
    column: ResolvedColumn<'a>,
    value_type: PlannedValueType,
}

pub(crate) struct IcebergFilterPlanner {
    schema: Arc<IcebergSchema>,
    layout: RelationLayout,
    utf8: Option<Utf8ServerEncoding>,
    cost_model: PredicateCostModel,
    contract: PredicateStorageContract,
}

impl IcebergFilterPlanner {
    /// Bind predicate planning to an adapter-supplied Iceberg schema, the
    /// partition specs of the table it reads, and that adapter's storage
    /// contract.
    pub(crate) fn new(
        context: &FilterPlanningContext,
        schema: &Arc<IcebergSchema>,
        metadata: &TableMetadata,
        contract: PredicateStorageContract,
    ) -> IcebergResult<Self> {
        let relation = RelationGuard::open_table(
            context.relation_oid(),
            pg_sys::NoLock as pg_sys::LOCKMODE,
        )?;
        let layout = RelationLayout::from_relation(&relation.as_handle())?;
        Ok(Self {
            schema: Arc::clone(schema),
            layout,
            utf8: Utf8ServerEncoding::resolve().ok(),
            cost_model: PredicateCostModel::from_metadata(metadata),
            contract,
        })
    }

    fn resolve_comparison(
        &self,
        column: &ColumnRef,
        value: &RuntimeValueSpec,
        operator: PgComparisonOp,
    ) -> IcebergResult<Option<ResolvedComparison<'_>>> {
        let Some(resolved_column) = self.resolve_column(column.attno)? else {
            return Ok(None);
        };
        Ok(PgPredicateAdmission::plan_comparison(
            column,
            value,
            operator.identity(),
            &resolved_column.field.field_type,
            self.utf8,
            self.contract,
        )
        .map(|(supported, value_type)| ResolvedComparison {
            supported,
            column: resolved_column,
            value_type,
        }))
    }

    fn plan_comparison(
        &self,
        fragment: &PredicateFragment,
        operator: PgComparisonOp,
        left: &ScalarExpr,
        right: &ScalarExpr,
    ) -> IcebergResult<PredicatePlan<CostedNode>> {
        let (column, value, mirrored) = match (left, right) {
            (ScalarExpr::Column(column), ScalarExpr::Value(value)) => {
                (column, *value, false)
            }
            (ScalarExpr::Value(value), ScalarExpr::Column(column)) => {
                (column, *value, true)
            }
            _ => return Ok(PredicatePlan::Unsupported),
        };
        let value_slot = fragment.value(value);
        let Some(ResolvedComparison {
            supported,
            column: resolved_column,
            value_type,
        }) = self.resolve_comparison(column, value_slot, operator)?
        else {
            return Ok(PredicatePlan::Unsupported);
        };
        let planned_operator = if mirrored {
            supported.operator.commuted()
        } else {
            supported.operator
        };
        let costing = self.cost_model.comparison(
            supported,
            planned_operator,
            value_slot.source_kind,
            value_type,
            resolved_column.field.id,
        );
        let planned = CostedNode {
            node: PlannedNode::Comparison {
                operator: planned_operator,
                column: resolved_column.to_planned(),
                value,
                value_type,
            },
            costing,
        };
        Ok(match supported.capability {
            PredicateCapability::Exact => PredicatePlan::Exact(planned),
            PredicateCapability::Conservative => PredicatePlan::Conservative(planned),
        })
    }

    fn plan_in_set(
        &self,
        fragment: &PredicateFragment,
        operator: PgComparisonOp,
        value: &ScalarExpr,
        set: &ScalarExpr,
    ) -> IcebergResult<PredicatePlan<CostedNode>> {
        let (ScalarExpr::Column(column), ScalarExpr::Value(set)) = (value, set)
        else {
            return Ok(PredicatePlan::Unsupported);
        };
        let set_slot = fragment.value(*set);
        // A set only prunes files, and file planning sees only rescan-stable
        // values.
        if !set_slot.source_kind.is_rescan_stable() {
            return Ok(PredicatePlan::Unsupported);
        }
        let Some(element) = set_slot.array_element() else {
            return Ok(PredicatePlan::Unsupported);
        };
        let Some(resolved) = self.resolve_comparison(column, &element, operator)?
        else {
            return Ok(PredicatePlan::Unsupported);
        };
        if resolved.supported.operator != PgComparisonKind::Equal
            || !PgPredicateAdmission::supports_set_membership(resolved.value_type)
        {
            return Ok(PredicatePlan::Unsupported);
        }
        let costing = self.cost_model.in_set(
            resolved.supported,
            set_slot.source_kind,
            resolved.value_type,
            resolved.column.field.id,
        );
        Ok(PredicatePlan::Conservative(CostedNode {
            node: PlannedNode::InSet {
                column: resolved.column.to_planned(),
                set: *set,
                value_type: resolved.value_type,
            },
            costing,
        }))
    }

    fn plan_null_test(
        &self,
        value: &ScalarExpr,
        is_not_null: bool,
    ) -> IcebergResult<PredicatePlan<CostedNode>> {
        let ScalarExpr::Column(column) = value else {
            return Ok(PredicatePlan::Unsupported);
        };
        let Some(resolved) = self.resolve_column(column.attno)? else {
            return Ok(PredicatePlan::Unsupported);
        };
        if !PgPredicateAdmission::supports_null_test(
            column.declared_type.type_oid,
            &resolved.field.field_type,
        ) {
            return Ok(PredicatePlan::Unsupported);
        }
        let costing = self.cost_model.value_pruning(resolved.field.id);
        let planned = resolved.to_planned();
        Ok(PredicatePlan::Exact(CostedNode {
            node: if is_not_null {
                PlannedNode::IsNotNull(planned)
            } else {
                PlannedNode::IsNull(planned)
            },
            costing,
        }))
    }

    fn plan_nan_test(
        &self,
        value: &ScalarExpr,
        is_not_nan: bool,
    ) -> IcebergResult<PredicatePlan<CostedNode>> {
        let ScalarExpr::Column(column) = value else {
            return Ok(PredicatePlan::Unsupported);
        };
        let Some(resolved) = self.resolve_column(column.attno)? else {
            return Ok(PredicatePlan::Unsupported);
        };
        if !PgPredicateAdmission::supports_nan_test(
            column,
            &resolved.field.field_type,
        ) {
            return Ok(PredicatePlan::Unsupported);
        }
        let costing = self.cost_model.value_pruning(resolved.field.id);
        let planned = resolved.to_planned();
        let node = CostedNode {
            node: if is_not_nan {
                PlannedNode::IsNotNan(planned)
            } else {
                PlannedNode::IsNan(planned)
            },
            costing,
        };
        // Negation remains semantic until binding. The binder therefore swaps
        // these nodes directly and adds the NotNull guard required by NotNan,
        // instead of complementing an already-built native predicate.
        Ok(PredicatePlan::Exact(node))
    }

    fn plan_starts_with(
        &self,
        fragment: &PredicateFragment,
        value: &ScalarExpr,
        prefix: &ScalarExpr,
    ) -> IcebergResult<PredicatePlan<CostedNode>> {
        let (ScalarExpr::Column(column), ScalarExpr::Value(prefix)) = (value, prefix)
        else {
            return Ok(PredicatePlan::Unsupported);
        };
        let Some(resolved) = self.resolve_column(column.attno)? else {
            return Ok(PredicatePlan::Unsupported);
        };
        if !PgPredicateAdmission::supports_starts_with(
            column,
            fragment.value(*prefix),
            &resolved.field.field_type,
            self.utf8,
        ) {
            return Ok(PredicatePlan::Unsupported);
        }
        let costing = self.cost_model.order_pruning(resolved.field.id);
        Ok(PredicatePlan::ExactNoComplement(CostedNode {
            node: PlannedNode::StartsWith {
                column: resolved.to_planned(),
                prefix: *prefix,
            },
            costing,
        }))
    }

    fn resolve_column(
        &self,
        attno: pg_sys::AttrNumber,
    ) -> IcebergResult<Option<ResolvedColumn<'_>>> {
        let live = self.layout.column(attno)?;
        let field = match (
            self.contract,
            self.schema.as_struct().field_by_name(&live.name),
        ) {
            (_, Some(field)) => field.as_ref(),
            (PredicateStorageContract::Managed { .. }, None) => {
                return Err(IcebergError::ColumnNotFound(live.name.clone()));
            }
            (PredicateStorageContract::Foreign, None) => return Ok(None),
        };
        if let Type::Primitive(PrimitiveType::Decimal { precision, scale }) =
            field.field_type.as_ref()
            && self.layout.attr_types()[(attno - 1) as usize].1
                != numeric_typmod(*precision, *scale as i32)
        {
            // Decimal predicate pushdown deliberately requires an exact typmod
            // match. Users are responsible for matching the local declaration
            // to the remote decimal type to retain pushdown; mismatches accept
            // the cost of local predicate evaluation, including safe precision
            // widening and unconstrained numeric. We do not special-case those
            // here. For narrowing, this also prevents pruning from bypassing
            // numeric conversion errors before local predicate evaluation.
            return Ok(None);
        }
        Ok(Some(ResolvedColumn { live, field }))
    }
}

/// A planned node paired with its scan-volume costing.
struct CostedNode {
    node: PlannedNode,
    costing: PushdownCosting,
}

struct RelationPredicateAdapter<'a> {
    planner: &'a IcebergFilterPlanner,
    fragment: &'a PredicateFragment,
}

impl RelationPredicateAdapter<'_> {
    fn logical(
        &self,
        children: Vec<CostedNode>,
        combine: fn(PushdownCosting, PushdownCosting) -> PushdownCosting,
        build: impl FnOnce(Box<[PlannedNode]>) -> PlannedNode,
    ) -> CostedNode {
        let costing = children
            .iter()
            .map(|child| child.costing)
            .reduce(combine)
            .unwrap_or(PushdownCosting::StatisticsPruning);
        let children = children
            .into_iter()
            .map(|child| child.node)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        CostedNode {
            node: build(children),
            costing,
        }
    }
}

impl PredicatePlanner<ScalarExpr, PgComparisonOp> for RelationPredicateAdapter<'_> {
    type Predicate = CostedNode;
    type Error = IcebergError;

    fn always_true(&self) -> IcebergResult<PredicatePlan<CostedNode>> {
        Ok(PredicatePlan::Exact(CostedNode {
            node: PlannedNode::AlwaysTrue,
            costing: PushdownCosting::StatisticsPruning,
        }))
    }

    fn always_false(&self) -> IcebergResult<PredicatePlan<CostedNode>> {
        Ok(PredicatePlan::Exact(CostedNode {
            node: PlannedNode::AlwaysFalse,
            costing: PushdownCosting::StatisticsPruning,
        }))
    }

    fn strict_true(
        &self,
        value: &ScalarExpr,
    ) -> IcebergResult<PredicatePlan<CostedNode>> {
        let ScalarExpr::Column(column) = value else {
            return Ok(PredicatePlan::Unsupported);
        };
        let Some(resolved) = self.planner.resolve_column(column.attno)? else {
            return Ok(PredicatePlan::Unsupported);
        };
        if !PgPredicateAdmission::supports_null_test(
            column.declared_type.type_oid,
            &resolved.field.field_type,
        ) {
            return Ok(PredicatePlan::Unsupported);
        }
        let costing = self.planner.cost_model.value_pruning(resolved.field.id);
        Ok(PredicatePlan::ExactNoComplement(CostedNode {
            node: PlannedNode::IsNotNull(resolved.to_planned()),
            costing,
        }))
    }

    fn strict_false(
        &self,
        value: &ScalarExpr,
    ) -> IcebergResult<PredicatePlan<CostedNode>> {
        let ScalarExpr::Column(column) = value else {
            return Ok(PredicatePlan::Unsupported);
        };
        let Some(resolved) = self.planner.resolve_column(column.attno)? else {
            return Ok(PredicatePlan::Unsupported);
        };
        if !PgPredicateAdmission::supports_null_test(
            column.declared_type.type_oid,
            &resolved.field.field_type,
        ) {
            return Ok(PredicatePlan::Unsupported);
        }
        Ok(PredicatePlan::ExactNoComplement(CostedNode {
            node: PlannedNode::AlwaysFalse,
            costing: self.planner.cost_model.value_pruning(resolved.field.id),
        }))
    }

    fn comparison(
        &self,
        operator: &PgComparisonOp,
        left: &ScalarExpr,
        right: &ScalarExpr,
    ) -> IcebergResult<PredicatePlan<CostedNode>> {
        self.planner
            .plan_comparison(self.fragment, *operator, left, right)
    }

    fn boolean_test(
        &self,
        kind: BooleanTestKind,
        value: &ScalarExpr,
    ) -> IcebergResult<PredicatePlan<CostedNode>> {
        let ScalarExpr::Column(column) = value else {
            return Ok(PredicatePlan::Unsupported);
        };
        let Some(resolved) = self.planner.resolve_column(column.attno)? else {
            return Ok(PredicatePlan::Unsupported);
        };
        if !PgPredicateAdmission::supports_boolean_test(
            column,
            &resolved.field.field_type,
        ) {
            return Ok(PredicatePlan::Unsupported);
        }
        let costing = self.planner.cost_model.value_pruning(resolved.field.id);
        Ok(PredicatePlan::Exact(CostedNode {
            node: PlannedNode::BooleanTest {
                kind,
                column: resolved.to_planned(),
            },
            costing,
        }))
    }

    fn is_null(
        &self,
        value: &ScalarExpr,
    ) -> IcebergResult<PredicatePlan<CostedNode>> {
        self.planner.plan_null_test(value, false)
    }

    fn is_not_null(
        &self,
        value: &ScalarExpr,
    ) -> IcebergResult<PredicatePlan<CostedNode>> {
        self.planner.plan_null_test(value, true)
    }

    fn is_nan(&self, value: &ScalarExpr) -> IcebergResult<PredicatePlan<CostedNode>> {
        self.planner.plan_nan_test(value, false)
    }

    fn is_not_nan(
        &self,
        value: &ScalarExpr,
    ) -> IcebergResult<PredicatePlan<CostedNode>> {
        self.planner.plan_nan_test(value, true)
    }

    fn starts_with(
        &self,
        value: &ScalarExpr,
        prefix: &ScalarExpr,
    ) -> IcebergResult<PredicatePlan<CostedNode>> {
        self.planner.plan_starts_with(self.fragment, value, prefix)
    }

    fn in_set(
        &self,
        operator: &PgComparisonOp,
        value: &ScalarExpr,
        set: &ScalarExpr,
    ) -> IcebergResult<PredicatePlan<CostedNode>> {
        self.planner
            .plan_in_set(self.fragment, *operator, value, set)
    }

    fn conjunction(&self, children: Vec<CostedNode>) -> CostedNode {
        self.logical(children, PushdownCosting::conjunction, PlannedNode::And)
    }

    fn disjunction(&self, children: Vec<CostedNode>) -> CostedNode {
        self.logical(children, PushdownCosting::disjunction, PlannedNode::Or)
    }

    fn negate(&self, child: CostedNode) -> CostedNode {
        CostedNode {
            node: PlannedNode::Not(Box::new(child.node)),
            costing: child.costing,
        }
    }
}

impl FilterPushdownPlanner for IcebergFilterPlanner {
    type PlannedPredicate = IcebergPlannedPredicate;
    type Error = IcebergError;

    fn try_plan_filter(
        &mut self,
        fragment: &PredicateFragment,
    ) -> Result<FilterPlan<Self::PlannedPredicate>, Self::Error> {
        let adapter = RelationPredicateAdapter {
            planner: self,
            fragment,
        };
        Ok(match fragment.root().plan_with(&adapter)? {
            PredicatePlan::Unsupported => FilterPlan::Unsupported,
            PredicatePlan::Partial(planned) => FilterPlan::partial(
                IcebergPlannedPredicate::new(self.schema.schema_id(), planned.node),
                planned.costing,
            ),
            PredicatePlan::Exact(planned)
            | PredicatePlan::ExactNoComplement(planned) => FilterPlan::exact(
                IcebergPlannedPredicate::new(self.schema.schema_id(), planned.node),
                planned.costing,
            ),
            // A set plans conservatively, so no exact tree holds one.
            PredicatePlan::Conservative(planned) => {
                let evaluation = planned.node.evaluation();
                let predicate = IcebergPlannedPredicate::new(
                    self.schema.schema_id(),
                    planned.node,
                );
                FilterPlan::conservative(predicate, planned.costing, evaluation)
            }
        })
    }

    fn pruning_units(&self) -> Option<f64> {
        self.contract.pruning_units()
    }
}
