//! Relation-scoped `PredicateFragment` to Iceberg planned-predicate conversion.

use lagodb_core::expr::RuntimeValueSource;
use lagodb_core::expr::pushdown::{
    FilterPlan, FilterPlanningContext, FilterPushdownPlanner, PredicateExpr,
    PredicateFragment, ScalarExpr,
};
use lagodb_core::expr::{PgComparisonOp, PushdownCosting};
use lagodb_core::handles::RelationGuard;
use pgrx::pg_sys;
use std::sync::Arc;

use iceberg_lite::spec::Schema as IcebergSchema;

use crate::engine::schema::relation::{
    RelationFieldIndex, RelationFieldMap, RelationShape,
};
use crate::error::IcebergError;

use super::error::IcebergFilterError;
use super::plan::{
    PlannedComparisonOperator, PlannedIcebergColumn, PlannedIcebergNode,
    PlannedIcebergPredicate,
};
use super::policy::{
    ComparisonOpClass, PgPredicatePushdownPolicy, PredicatePushdownPolicy,
    SupportedPredicateCapability,
};

pub(crate) struct IcebergFilterPlanner {
    schema_id: i32,
    fields: RelationFieldIndex,
}

impl IcebergFilterPlanner {
    /// Bind predicate planning to an adapter-supplied Iceberg schema.
    pub(crate) fn from_schema(
        context: &FilterPlanningContext,
        schema: &Arc<IcebergSchema>,
    ) -> Result<Self, IcebergFilterError> {
        let relation = RelationGuard::open(
            context.relation_oid(),
            pg_sys::NoLock as pg_sys::LOCKMODE,
        )
        .map_err(IcebergError::from)?;
        let shape = RelationShape::from_relation(&relation.as_handle())?;
        let fields = RelationFieldMap::from_shape(schema, &shape)?.into_indexed();
        Ok(Self {
            schema_id: schema.schema_id(),
            fields,
        })
    }

    fn plan_node(
        &self,
        fragment: &PredicateFragment,
        node: &PredicateExpr,
    ) -> Result<Option<PlannedNode>, IcebergFilterError> {
        match node {
            PredicateExpr::Comparison {
                operator,
                left,
                right,
            } => self.plan_comparison(fragment, *operator, left, right),
            PredicateExpr::IsNull(value) => self.plan_null_test(value, false),
            PredicateExpr::IsNotNull(value) => self.plan_null_test(value, true),
            PredicateExpr::And(children) => {
                self.plan_logical(fragment, children, LogicalKind::And)
            }
            PredicateExpr::Or(children) => {
                self.plan_logical(fragment, children, LogicalKind::Or)
            }
            PredicateExpr::Not(child) => {
                let Some(child) = self.plan_node(fragment, child)? else {
                    return Ok(None);
                };
                if child.contract != PlannedContract::Exact {
                    return Ok(None);
                }
                Ok(Some(PlannedNode {
                    node: PlannedIcebergNode::Not(Box::new(child.node)),
                    contract: child.contract,
                    costing: child.costing,
                }))
            }
        }
    }

    fn plan_comparison(
        &self,
        fragment: &PredicateFragment,
        operator: PgComparisonOp,
        left: &ScalarExpr,
        right: &ScalarExpr,
    ) -> Result<Option<PlannedNode>, IcebergFilterError> {
        let (column, value, mirrored) = match (left, right) {
            (ScalarExpr::Column(column), ScalarExpr::Value(value)) => {
                (column, *value, false)
            }
            (ScalarExpr::Value(value), ScalarExpr::Column(column)) => {
                (column, *value, true)
            }
            _ => return Ok(None),
        };
        let value_slot = fragment.value(value);
        let Some((supported, value_type)) =
            PgPredicatePushdownPolicy::plan_comparison(
                column,
                value_slot,
                operator.identity(),
            )
        else {
            return Ok(None);
        };
        let mut planned_operator =
            PlannedComparisonOperator::from(supported.operator);
        if mirrored {
            planned_operator = planned_operator.mirrored();
        }
        let source_kind = value_slot.source_kind;
        let costing = if supported.capability
            == SupportedPredicateCapability::Conservative
            && (source_kind != RuntimeValueSource::Constant
                || PredicatePushdownPolicy::is_value_sensitive_type(
                    column.declared_type.type_oid,
                )) {
            PushdownCosting::UncostedBestEffort
        } else {
            PushdownCosting::CostedPruning
        };
        let contract = match supported.capability {
            SupportedPredicateCapability::Exact => PlannedContract::Exact,
            SupportedPredicateCapability::Conservative => {
                PlannedContract::Conservative
            }
        };
        Ok(Some(PlannedNode {
            node: PlannedIcebergNode::Comparison {
                operator: planned_operator,
                column: self.column(column.attno)?,
                value,
                value_type,
            },
            contract,
            costing,
        }))
    }

    fn plan_null_test(
        &self,
        value: &ScalarExpr,
        is_not_null: bool,
    ) -> Result<Option<PlannedNode>, IcebergFilterError> {
        let ScalarExpr::Column(column) = value else {
            return Ok(None);
        };
        if !PredicatePushdownPolicy::supports_null_test(column.declared_type.type_oid)
        {
            return Ok(None);
        }
        let column = self.column(column.attno)?;
        Ok(Some(PlannedNode {
            node: if is_not_null {
                PlannedIcebergNode::IsNotNull(column)
            } else {
                PlannedIcebergNode::IsNull(column)
            },
            contract: PlannedContract::Exact,
            costing: PushdownCosting::CostedPruning,
        }))
    }

    fn plan_logical(
        &self,
        fragment: &PredicateFragment,
        children: &[PredicateExpr],
        kind: LogicalKind,
    ) -> Result<Option<PlannedNode>, IcebergFilterError> {
        let mut planned = Vec::with_capacity(children.len());
        let mut contract = PlannedContract::Exact;
        let mut costing = PushdownCosting::CostedPruning;
        for child in children {
            let Some(child) = self.plan_node(fragment, child)? else {
                return Ok(None);
            };
            if child.contract == PlannedContract::Conservative {
                contract = PlannedContract::Conservative;
            }
            if !child.costing.is_costed() {
                costing = PushdownCosting::UncostedBestEffort;
            }
            planned.push(child.node);
        }
        let planned = planned.into_boxed_slice();
        Ok(Some(PlannedNode {
            node: match kind {
                LogicalKind::And => PlannedIcebergNode::And(planned),
                LogicalKind::Or => PlannedIcebergNode::Or(planned),
            },
            contract,
            costing,
        }))
    }

    fn column(
        &self,
        attno: pg_sys::AttrNumber,
    ) -> Result<PlannedIcebergColumn, IcebergFilterError> {
        let binding = self
            .fields
            .binding_for_attno(attno)
            .ok_or(IcebergFilterError::MissingFieldBinding(attno))?;
        Ok(PlannedIcebergColumn {
            field_id: binding.field_id,
            debug_name: binding.debug_name.clone(),
        })
    }
}

impl FilterPushdownPlanner for IcebergFilterPlanner {
    type PlannedPredicate = PlannedIcebergPredicate;
    type Error = IcebergFilterError;

    fn try_plan_filter(
        &mut self,
        fragment: &PredicateFragment,
    ) -> Result<FilterPlan<Self::PlannedPredicate>, Self::Error> {
        let Some(planned) = self.plan_node(fragment, fragment.root())? else {
            return Ok(FilterPlan::Unsupported);
        };
        let predicate = PlannedIcebergPredicate::new(self.schema_id, planned.node);
        Ok(match planned.contract {
            PlannedContract::Exact => FilterPlan::exact(predicate, planned.costing),
            PlannedContract::Conservative => {
                FilterPlan::conservative(predicate, planned.costing)
            }
        })
    }
}

struct PlannedNode {
    node: PlannedIcebergNode,
    contract: PlannedContract,
    costing: PushdownCosting,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PlannedContract {
    Exact,
    Conservative,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LogicalKind {
    And,
    Or,
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
