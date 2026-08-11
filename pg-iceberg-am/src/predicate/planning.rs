//! Relation-scoped `FilterFragment` to Iceberg planned-predicate conversion.

use pg_lakebase_core::expr::pushdown::{
    FilterFragment, FilterNode, FilterPlan, FilterPlanningContext,
    FilterPushdownPlanner, FilterScalar, FilterValueSourceKind,
};
use pg_lakebase_core::expr::{PgComparisonOp, PushdownCosting};
use pg_lakebase_core::handles::RelationGuard;
use pgrx::pg_sys;

use crate::access::scan::LoadedScanMetadata;
use crate::error::IcebergError;
use crate::relation_binding::{RelationFieldIndex, RelationFieldMap, RelationShape};

use super::error::IcebergFilterError;
use super::planned::{
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
    pub(crate) fn begin(
        context: &FilterPlanningContext,
    ) -> Result<Self, IcebergFilterError> {
        let relation = RelationGuard::open(
            context.relation_oid(),
            pg_sys::NoLock as pg_sys::LOCKMODE,
        )
        .map_err(IcebergError::from)?;
        let shape = RelationShape::from_relation(&relation.as_handle());
        let metadata = LoadedScanMetadata::load_query(
            context.relation_oid(),
            context.tablespace_oid(),
        )?;
        let fields =
            RelationFieldMap::from_shape(metadata.schema(), &shape)?.into_indexed();
        Ok(Self {
            schema_id: metadata.schema().schema_id(),
            fields,
        })
    }

    fn plan_node(
        &self,
        fragment: &FilterFragment,
        node: &FilterNode,
    ) -> Result<Option<PlannedNode>, IcebergFilterError> {
        match node {
            FilterNode::Comparison {
                operator,
                left,
                right,
            } => self.plan_comparison(fragment, *operator, left, right),
            FilterNode::IsNull(value) => self.plan_null_test(value, false),
            FilterNode::IsNotNull(value) => self.plan_null_test(value, true),
            FilterNode::And(children) => {
                self.plan_logical(fragment, children, LogicalKind::And)
            }
            FilterNode::Or(children) => {
                self.plan_logical(fragment, children, LogicalKind::Or)
            }
            FilterNode::Not(child) => {
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
        fragment: &FilterFragment,
        operator: PgComparisonOp,
        left: &FilterScalar,
        right: &FilterScalar,
    ) -> Result<Option<PlannedNode>, IcebergFilterError> {
        let (column, value, mirrored) = match (left, right) {
            (FilterScalar::Column(column), FilterScalar::Value(value)) => {
                (column, *value, false)
            }
            (FilterScalar::Value(value), FilterScalar::Column(column)) => {
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
            && (source_kind != FilterValueSourceKind::Constant
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
        value: &FilterScalar,
        is_not_null: bool,
    ) -> Result<Option<PlannedNode>, IcebergFilterError> {
        let FilterScalar::Column(column) = value else {
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
        fragment: &FilterFragment,
        children: &[FilterNode],
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
        fragment: &FilterFragment,
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

#[derive(Clone, Copy)]
enum LogicalKind {
    And,
    Or,
}

impl From<ComparisonOpClass> for PlannedComparisonOperator {
    fn from(value: ComparisonOpClass) -> Self {
        match value {
            ComparisonOpClass::Eq => Self::Eq,
            ComparisonOpClass::NotEq => Self::NotEq,
            ComparisonOpClass::Lt => Self::Lt,
            ComparisonOpClass::Le => Self::Le,
            ComparisonOpClass::Gt => Self::Gt,
            ComparisonOpClass::Ge => Self::Ge,
        }
    }
}
