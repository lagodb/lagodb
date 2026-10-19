//! Relation-scoped `PredicateFragment` to Iceberg planned-predicate conversion.

use lagodb_core::datum::{Utf8ServerEncoding, numeric_typmod};
use lagodb_core::expr::pushdown::{
    FilterPlan, FilterPlanningContext, FilterPushdownPlanner, PredicateFragment,
    PredicatePlan, PredicatePlanner, ScalarExpr,
};
use lagodb_core::expr::{BooleanTestKind, RuntimeValueSource};
use lagodb_core::expr::{PgComparisonOp, PushdownCosting};
use lagodb_core::handles::RelationGuard;
use pgrx::pg_sys;
use std::sync::Arc;

use iceberg_lite::spec::{PrimitiveType, Schema as IcebergSchema, Type};

use crate::error::IcebergError;
use crate::schema::relation::RelationLayout;

use super::error::IcebergFilterError;
use super::plan::{
    PlannedComparisonOperator, PlannedIcebergColumn, PlannedIcebergNode,
    PlannedIcebergPredicate, PlannedValueType,
};
use super::policy::{
    ComparisonOpClass, PgPredicatePushdownPolicy, PredicatePushdownPolicy,
    SupportedPredicateCapability,
};

pub(crate) struct IcebergFilterPlanner {
    schema: Arc<IcebergSchema>,
    layout: RelationLayout,
    utf8: Option<Utf8ServerEncoding>,
}

impl IcebergFilterPlanner {
    /// Bind predicate planning to an adapter-supplied Iceberg schema.
    pub(crate) fn from_schema(
        context: &FilterPlanningContext,
        schema: &Arc<IcebergSchema>,
    ) -> Result<Self, IcebergFilterError> {
        let relation = RelationGuard::open_table(
            context.relation_oid(),
            pg_sys::NoLock as pg_sys::LOCKMODE,
        )
        .map_err(IcebergError::from)?;
        let layout = RelationLayout::from_relation(&relation.as_handle())?;
        Ok(Self {
            schema: Arc::clone(schema),
            layout,
            utf8: Utf8ServerEncoding::resolve().ok(),
        })
    }

    fn plan_comparison(
        &self,
        fragment: &PredicateFragment,
        operator: PgComparisonOp,
        left: &ScalarExpr,
        right: &ScalarExpr,
    ) -> Result<PredicatePlan<PlannedNode>, IcebergFilterError> {
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
        let Some((mut supported, value_type)) =
            PgPredicatePushdownPolicy::plan_comparison(
                column,
                value_slot,
                operator.identity(),
                self.utf8,
            )
        else {
            return Ok(PredicatePlan::Unsupported);
        };
        let Some(planned_column) = self.column(column.attno)? else {
            return Ok(PredicatePlan::Unsupported);
        };
        let field = self
            .schema
            .field_by_id(planned_column.field_id)
            .expect("relation field binding belongs to the planner schema");
        let Some(value_type) = value_type.with_source_type(&field.field_type) else {
            return Ok(PredicatePlan::Unsupported);
        };
        if matches!(value_type, PlannedValueType::FixedBinary(_)) {
            // PostgreSQL bytea values do not carry Iceberg's fixed width.
            // A different-width value stays with the residual at bind time.
            supported.capability = SupportedPredicateCapability::Conservative;
        }
        let mut planned_operator =
            PlannedComparisonOperator::from(supported.operator);
        if mirrored {
            planned_operator = planned_operator.mirrored();
        }
        let source_kind = value_slot.source_kind;
        let costing = if supported.capability
            == SupportedPredicateCapability::Conservative
            && (source_kind != RuntimeValueSource::Constant
                || matches!(value_type, PlannedValueType::FixedBinary(_))
                || PredicatePushdownPolicy::is_value_sensitive_type(
                    column.declared_type.type_oid,
                )) {
            PushdownCosting::UncostedBestEffort
        } else {
            PushdownCosting::CostedPruning
        };
        let planned = PlannedNode {
            node: PlannedIcebergNode::Comparison {
                operator: planned_operator,
                column: planned_column,
                value,
                value_type,
            },
            costing,
        };
        Ok(match supported.capability {
            SupportedPredicateCapability::Exact => PredicatePlan::Exact(planned),
            SupportedPredicateCapability::Conservative => {
                PredicatePlan::Conservative(planned)
            }
        })
    }

    fn plan_null_test(
        &self,
        value: &ScalarExpr,
        is_not_null: bool,
    ) -> Result<PredicatePlan<PlannedNode>, IcebergFilterError> {
        let ScalarExpr::Column(column) = value else {
            return Ok(PredicatePlan::Unsupported);
        };
        if !PgPredicatePushdownPolicy::supports_null_test(
            column.declared_type.type_oid,
        ) {
            return Ok(PredicatePlan::Unsupported);
        }
        let Some(column) = self.column(column.attno)? else {
            return Ok(PredicatePlan::Unsupported);
        };
        Ok(PredicatePlan::Exact(PlannedNode {
            node: if is_not_null {
                PlannedIcebergNode::IsNotNull(column)
            } else {
                PlannedIcebergNode::IsNull(column)
            },
            costing: PushdownCosting::CostedPruning,
        }))
    }

    fn plan_nan_test(
        &self,
        value: &ScalarExpr,
        is_not_nan: bool,
    ) -> Result<PredicatePlan<PlannedNode>, IcebergFilterError> {
        let ScalarExpr::Column(column) = value else {
            return Ok(PredicatePlan::Unsupported);
        };
        if !PgPredicatePushdownPolicy::supports_nan_test(column) {
            return Ok(PredicatePlan::Unsupported);
        }
        let Some(column) = self.column(column.attno)? else {
            return Ok(PredicatePlan::Unsupported);
        };
        let planned = PlannedNode {
            node: if is_not_nan {
                PlannedIcebergNode::IsNotNan(column)
            } else {
                PlannedIcebergNode::IsNan(column)
            },
            costing: PushdownCosting::CostedPruning,
        };
        // Negation remains semantic until binding. The binder therefore swaps
        // these nodes directly and adds the NotNull guard required by NotNan,
        // instead of complementing an already-built native predicate.
        Ok(PredicatePlan::Exact(planned))
    }

    fn plan_starts_with(
        &self,
        fragment: &PredicateFragment,
        value: &ScalarExpr,
        prefix: &ScalarExpr,
    ) -> Result<PredicatePlan<PlannedNode>, IcebergFilterError> {
        let (ScalarExpr::Column(column), ScalarExpr::Value(prefix)) = (value, prefix)
        else {
            return Ok(PredicatePlan::Unsupported);
        };
        if PgPredicatePushdownPolicy::plan_starts_with(
            column,
            fragment.value(*prefix),
            self.utf8,
        )
        .is_none()
        {
            return Ok(PredicatePlan::Unsupported);
        }
        let Some(column) = self.column(column.attno)? else {
            return Ok(PredicatePlan::Unsupported);
        };
        Ok(PredicatePlan::ExactNoComplement(PlannedNode {
            node: PlannedIcebergNode::StartsWith {
                column,
                prefix: *prefix,
            },
            costing: PushdownCosting::CostedPruning,
        }))
    }

    fn column(
        &self,
        attno: pg_sys::AttrNumber,
    ) -> Result<Option<PlannedIcebergColumn>, IcebergFilterError> {
        let column = self.layout.column(attno)?;
        let field = self
            .schema
            .as_struct()
            .field_by_name(&column.name)
            .ok_or_else(|| IcebergError::ColumnNotFound(column.name.clone()))?;
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
        Ok(Some(PlannedIcebergColumn {
            field_id: field.id,
            debug_name: column.name.clone(),
        }))
    }
}

struct RelationPredicateAdapter<'a> {
    planner: &'a IcebergFilterPlanner,
    fragment: &'a PredicateFragment,
}

impl RelationPredicateAdapter<'_> {
    fn logical(
        &self,
        children: Vec<PlannedNode>,
        build: impl FnOnce(Box<[PlannedIcebergNode]>) -> PlannedIcebergNode,
    ) -> PlannedNode {
        let costing = if children.iter().all(|child| child.costing.is_costed()) {
            PushdownCosting::CostedPruning
        } else {
            PushdownCosting::UncostedBestEffort
        };
        let children = children
            .into_iter()
            .map(|child| child.node)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        PlannedNode {
            node: build(children),
            costing,
        }
    }
}

impl PredicatePlanner<ScalarExpr, PgComparisonOp> for RelationPredicateAdapter<'_> {
    type Predicate = PlannedNode;
    type Error = IcebergFilterError;

    fn always_true(&self) -> Result<PredicatePlan<PlannedNode>, Self::Error> {
        Ok(PredicatePlan::Exact(PlannedNode {
            node: PlannedIcebergNode::AlwaysTrue,
            costing: PushdownCosting::CostedPruning,
        }))
    }

    fn always_false(&self) -> Result<PredicatePlan<PlannedNode>, Self::Error> {
        Ok(PredicatePlan::Exact(PlannedNode {
            node: PlannedIcebergNode::AlwaysFalse,
            costing: PushdownCosting::CostedPruning,
        }))
    }

    fn strict_true(
        &self,
        value: &ScalarExpr,
    ) -> Result<PredicatePlan<PlannedNode>, Self::Error> {
        let ScalarExpr::Column(column) = value else {
            return Ok(PredicatePlan::Unsupported);
        };
        let Some(column) = self.planner.column(column.attno)? else {
            return Ok(PredicatePlan::Unsupported);
        };
        Ok(PredicatePlan::ExactNoComplement(PlannedNode {
            node: PlannedIcebergNode::IsNotNull(column),
            costing: PushdownCosting::CostedPruning,
        }))
    }

    fn strict_false(
        &self,
        value: &ScalarExpr,
    ) -> Result<PredicatePlan<PlannedNode>, Self::Error> {
        let ScalarExpr::Column(column) = value else {
            return Ok(PredicatePlan::Unsupported);
        };
        if self.planner.column(column.attno)?.is_none() {
            return Ok(PredicatePlan::Unsupported);
        }
        Ok(PredicatePlan::ExactNoComplement(PlannedNode {
            node: PlannedIcebergNode::AlwaysFalse,
            costing: PushdownCosting::CostedPruning,
        }))
    }

    fn comparison(
        &self,
        operator: &PgComparisonOp,
        left: &ScalarExpr,
        right: &ScalarExpr,
    ) -> Result<PredicatePlan<PlannedNode>, Self::Error> {
        self.planner
            .plan_comparison(self.fragment, *operator, left, right)
    }

    fn boolean_test(
        &self,
        kind: BooleanTestKind,
        value: &ScalarExpr,
    ) -> Result<PredicatePlan<PlannedNode>, Self::Error> {
        let ScalarExpr::Column(column) = value else {
            return Ok(PredicatePlan::Unsupported);
        };
        let Some(column) = self.planner.column(column.attno)? else {
            return Ok(PredicatePlan::Unsupported);
        };
        Ok(PredicatePlan::Exact(PlannedNode {
            node: PlannedIcebergNode::BooleanTest { kind, column },
            costing: PushdownCosting::CostedPruning,
        }))
    }

    fn is_null(
        &self,
        value: &ScalarExpr,
    ) -> Result<PredicatePlan<PlannedNode>, Self::Error> {
        self.planner.plan_null_test(value, false)
    }

    fn is_not_null(
        &self,
        value: &ScalarExpr,
    ) -> Result<PredicatePlan<PlannedNode>, Self::Error> {
        self.planner.plan_null_test(value, true)
    }

    fn is_nan(
        &self,
        value: &ScalarExpr,
    ) -> Result<PredicatePlan<PlannedNode>, Self::Error> {
        self.planner.plan_nan_test(value, false)
    }

    fn is_not_nan(
        &self,
        value: &ScalarExpr,
    ) -> Result<PredicatePlan<PlannedNode>, Self::Error> {
        self.planner.plan_nan_test(value, true)
    }

    fn starts_with(
        &self,
        value: &ScalarExpr,
        prefix: &ScalarExpr,
    ) -> Result<PredicatePlan<PlannedNode>, Self::Error> {
        self.planner.plan_starts_with(self.fragment, value, prefix)
    }

    fn conjunction(&self, children: Vec<PlannedNode>) -> PlannedNode {
        self.logical(children, PlannedIcebergNode::And)
    }

    fn disjunction(&self, children: Vec<PlannedNode>) -> PlannedNode {
        self.logical(children, PlannedIcebergNode::Or)
    }

    fn negate(&self, child: PlannedNode) -> PlannedNode {
        PlannedNode {
            node: PlannedIcebergNode::Not(Box::new(child.node)),
            costing: child.costing,
        }
    }
}

impl FilterPushdownPlanner for IcebergFilterPlanner {
    type PlannedPredicate = PlannedIcebergPredicate;
    type Error = IcebergFilterError;

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
                PlannedIcebergPredicate::new(self.schema.schema_id(), planned.node),
                planned.costing,
            ),
            PredicatePlan::Exact(planned)
            | PredicatePlan::ExactNoComplement(planned) => FilterPlan::exact(
                PlannedIcebergPredicate::new(self.schema.schema_id(), planned.node),
                planned.costing,
            ),
            PredicatePlan::Conservative(planned) => FilterPlan::conservative(
                PlannedIcebergPredicate::new(self.schema.schema_id(), planned.node),
                planned.costing,
            ),
        })
    }
}

struct PlannedNode {
    node: PlannedIcebergNode,
    costing: PushdownCosting,
}

impl From<ComparisonOpClass> for PlannedComparisonOperator {
    fn from(value: ComparisonOpClass) -> Self {
        match value {
            ComparisonOpClass::Equal => Self::Eq,
            ComparisonOpClass::NotEqual => Self::NotEq,
            ComparisonOpClass::Less => Self::Lt,
            ComparisonOpClass::LessEqual => Self::Le,
            ComparisonOpClass::Greater => Self::Gt,
            ComparisonOpClass::GreaterEqual => Self::Ge,
        }
    }
}
