//! Relation-scoped `PredicateFragment` to Iceberg planned-predicate conversion.

use lagodb_core::datum::{Utf8ServerEncoding, numeric_typmod};
use lagodb_core::expr::pushdown::{
    FilterPlan, FilterPlanningContext, FilterPushdownPlanner, PredicateFragment,
    PredicatePlan, PredicatePlanner, ScalarExpr,
};
use lagodb_core::expr::{
    BooleanTestKind, ColumnRef, RuntimeValueSource, RuntimeValueSpec,
};
use lagodb_core::expr::{PgComparisonOp, PushdownCosting};
use lagodb_core::handles::RelationGuard;
use pgrx::pg_sys;
use std::sync::Arc;

use iceberg_lite::spec::{
    PrimitiveType, Schema as IcebergSchema, TableMetadata, Transform, Type,
};

use crate::error::IcebergError;
use crate::schema::relation::RelationLayout;

use super::error::IcebergFilterError;
use super::plan::{
    PlannedComparisonOperator, PlannedIcebergColumn, PlannedIcebergNode,
    PlannedIcebergPredicate, PlannedValueType,
};
use super::policy::{
    ComparisonOpClass, PgPredicatePushdownPolicy, SupportedComparison,
    SupportedPredicateCapability,
};

/// What a pruning filter needs from a partition transform to keep only the
/// files whose partition values can match it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PartitionMatch {
    /// Equality and null tests, which every transform projects.
    Value,
    /// Ranges and prefixes, which a bucket hash does not project.
    Order,
}

impl PartitionMatch {
    /// The filters `transform` projects onto partition values, `None` for a
    /// transform that projects none.
    fn of(transform: Transform) -> Option<Self> {
        match transform {
            Transform::Void | Transform::Unknown => None,
            Transform::Bucket(_) => Some(Self::Value),
            Transform::Identity
            | Transform::Truncate(_)
            | Transform::Year
            | Transform::Month
            | Transform::Day
            | Transform::Hour => Some(Self::Order),
        }
    }
}

/// One policy-accepted comparison resolved against the planner schema.
struct ResolvedComparison {
    supported: SupportedComparison,
    column: PlannedIcebergColumn,
    value_type: PlannedValueType,
}

pub(crate) struct IcebergFilterPlanner {
    schema: Arc<IcebergSchema>,
    layout: RelationLayout,
    utf8: Option<Utf8ServerEncoding>,
    /// Field ids that every partition spec derives a partition value from,
    /// each with the filters all of those specs project.
    layout_sources: Vec<(i32, PartitionMatch)>,
    pruning_units: Option<f64>,
}

impl IcebergFilterPlanner {
    /// Bind predicate planning to an adapter-supplied Iceberg schema, the
    /// partition specs of the table it reads and that read's
    /// [`FilterPushdownPlanner::pruning_units`].
    pub(crate) fn new(
        context: &FilterPlanningContext,
        schema: &Arc<IcebergSchema>,
        metadata: &TableMetadata,
        pruning_units: Option<f64>,
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
            layout_sources: Self::layout_sources(metadata),
            pruning_units,
        })
    }

    /// Source fields whose partition values prune every file. A scan projects
    /// a filter by each file's own spec, so a field missing from one spec,
    /// such as that of files written before the table was partitioned, leaves
    /// those files to file metrics. The specs of the table metadata include
    /// every spec its files can carry.
    fn layout_sources(metadata: &TableMetadata) -> Vec<(i32, PartitionMatch)> {
        // A spec holds a few fields, so lists searched in order suffice, as
        // partprune.c matches clauses to partition keys.
        let mut specs = metadata.partition_specs_iter().map(|spec| {
            let mut sources: Vec<(i32, PartitionMatch)> = Vec::new();
            for field in spec.fields() {
                let Some(matched) = PartitionMatch::of(field.transform) else {
                    continue;
                };
                match sources.iter_mut().find(|(id, _)| *id == field.source_id) {
                    Some((_, best)) => *best = (*best).max(matched),
                    None => sources.push((field.source_id, matched)),
                }
            }
            sources
        });
        let Some(mut common) = specs.next() else {
            return Vec::new();
        };
        for sources in specs {
            common.retain_mut(|(source_id, matched)| {
                Self::layout_match(&sources, *source_id).is_some_and(|other| {
                    *matched = (*matched).min(other);
                    true
                })
            });
        }
        common
    }

    fn layout_match(
        sources: &[(i32, PartitionMatch)],
        field_id: i32,
    ) -> Option<PartitionMatch> {
        sources.iter().find_map(|&(source_id, matched)| {
            (source_id == field_id).then_some(matched)
        })
    }

    /// Costing of a pruning filter on `column` that needs `needed` from a
    /// partition transform. Partition values prune whole files when every spec
    /// projects the filter; file metrics prune it otherwise.
    fn pruning_costing(
        &self,
        column: &PlannedIcebergColumn,
        needed: PartitionMatch,
    ) -> PushdownCosting {
        match Self::layout_match(&self.layout_sources, column.field_id) {
            Some(matched) if matched >= needed => PushdownCosting::LayoutPruning,
            _ => PushdownCosting::StatisticsPruning,
        }
    }

    /// Costing of a comparison of `column` with a value from `source`.
    fn comparison_costing(
        &self,
        supported: SupportedComparison,
        source: RuntimeValueSource,
        value_type: PlannedValueType,
        column: &PlannedIcebergColumn,
    ) -> PushdownCosting {
        // A constant temporal literal prunes at its PostgreSQL selectivity.
        // An infinite one is not representable in Iceberg and leaves its rows
        // unpruned at bind time, a documented costing limit.
        if supported.capability == SupportedPredicateCapability::Conservative
            && (source != RuntimeValueSource::Constant
                || matches!(value_type, PlannedValueType::FixedBinary(_)))
        {
            return PushdownCosting::UncostedBestEffort;
        }
        match supported.operator {
            // A bucket hash projects only equality onto partition values.
            ComparisonOpClass::Equal => {
                self.pruning_costing(column, PartitionMatch::Value)
            }
            // Truncate and temporal transforms do not project `<>`.
            ComparisonOpClass::NotEqual => PushdownCosting::StatisticsPruning,
            ComparisonOpClass::Less
            | ComparisonOpClass::LessEqual
            | ComparisonOpClass::Greater
            | ComparisonOpClass::GreaterEqual => {
                self.pruning_costing(column, PartitionMatch::Order)
            }
        }
    }

    fn resolve_comparison(
        &self,
        column: &ColumnRef,
        value: &RuntimeValueSpec,
        operator: PgComparisonOp,
    ) -> Result<Option<ResolvedComparison>, IcebergFilterError> {
        let Some((supported, value_type)) =
            PgPredicatePushdownPolicy::plan_comparison(
                column,
                value,
                operator.identity(),
                self.utf8,
            )
        else {
            return Ok(None);
        };
        let Some(planned_column) = self.column(column.attno)? else {
            return Ok(None);
        };
        let field = self
            .schema
            .field_by_id(planned_column.field_id)
            .expect("relation field binding belongs to the planner schema");
        Ok(value_type
            .with_source_type(&field.field_type)
            .map(|value_type| ResolvedComparison {
                supported,
                column: planned_column,
                value_type,
            }))
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
        let Some(ResolvedComparison {
            mut supported,
            column: planned_column,
            value_type,
        }) = self.resolve_comparison(column, value_slot, operator)?
        else {
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
        let costing = self.comparison_costing(
            supported,
            value_slot.source_kind,
            value_type,
            &planned_column,
        );
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

    fn plan_in_set(
        &self,
        fragment: &PredicateFragment,
        operator: PgComparisonOp,
        value: &ScalarExpr,
        set: &ScalarExpr,
    ) -> Result<PredicatePlan<PlannedNode>, IcebergFilterError> {
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
        if resolved.supported.operator != ComparisonOpClass::Equal
            || !PgPredicatePushdownPolicy::supports_set_membership(
                resolved.value_type,
            )
        {
            return Ok(PredicatePlan::Unsupported);
        }
        // A set prunes as the OR of its equalities, as `partprune.c` expands
        // a ScalarArrayOpExpr. Partition values match a set of any size, but
        // file metrics skip one of more than 200 values (iceberg-lite's
        // `IN_PREDICATE_LIMIT`), a size planning does not know, so only
        // layout pruning carries over. That every row of a selected file
        // reaches PostgreSQL is priced by the filter's evaluation
        // (`PlannedIcebergNode::evaluation`), not by its costing.
        let costing = match self.comparison_costing(
            resolved.supported,
            set_slot.source_kind,
            resolved.value_type,
            &resolved.column,
        ) {
            PushdownCosting::LayoutPruning => PushdownCosting::LayoutPruning,
            PushdownCosting::StatisticsPruning
            | PushdownCosting::UncostedBestEffort => {
                PushdownCosting::UncostedBestEffort
            }
        };
        Ok(PredicatePlan::Conservative(PlannedNode {
            node: PlannedIcebergNode::InSet {
                column: resolved.column,
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
        let costing = self.pruning_costing(&column, PartitionMatch::Value);
        Ok(PredicatePlan::Exact(PlannedNode {
            node: if is_not_null {
                PlannedIcebergNode::IsNotNull(column)
            } else {
                PlannedIcebergNode::IsNull(column)
            },
            costing,
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
        let costing = self.pruning_costing(&column, PartitionMatch::Value);
        let planned = PlannedNode {
            node: if is_not_nan {
                PlannedIcebergNode::IsNotNan(column)
            } else {
                PlannedIcebergNode::IsNan(column)
            },
            costing,
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
        let costing = self.pruning_costing(&column, PartitionMatch::Order);
        Ok(PredicatePlan::ExactNoComplement(PlannedNode {
            node: PlannedIcebergNode::StartsWith {
                column,
                prefix: *prefix,
            },
            costing,
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
        combine: fn(PushdownCosting, PushdownCosting) -> PushdownCosting,
        build: impl FnOnce(Box<[PlannedIcebergNode]>) -> PlannedIcebergNode,
    ) -> PlannedNode {
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
            costing: PushdownCosting::StatisticsPruning,
        }))
    }

    fn always_false(&self) -> Result<PredicatePlan<PlannedNode>, Self::Error> {
        Ok(PredicatePlan::Exact(PlannedNode {
            node: PlannedIcebergNode::AlwaysFalse,
            costing: PushdownCosting::StatisticsPruning,
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
        let costing = self.planner.pruning_costing(&column, PartitionMatch::Value);
        Ok(PredicatePlan::ExactNoComplement(PlannedNode {
            node: PlannedIcebergNode::IsNotNull(column),
            costing,
        }))
    }

    fn strict_false(
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
            node: PlannedIcebergNode::AlwaysFalse,
            costing: self.planner.pruning_costing(&column, PartitionMatch::Value),
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
        let costing = self.planner.pruning_costing(&column, PartitionMatch::Value);
        Ok(PredicatePlan::Exact(PlannedNode {
            node: PlannedIcebergNode::BooleanTest { kind, column },
            costing,
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

    fn in_set(
        &self,
        operator: &PgComparisonOp,
        value: &ScalarExpr,
        set: &ScalarExpr,
    ) -> Result<PredicatePlan<PlannedNode>, Self::Error> {
        self.planner
            .plan_in_set(self.fragment, *operator, value, set)
    }

    fn conjunction(&self, children: Vec<PlannedNode>) -> PlannedNode {
        self.logical(
            children,
            PushdownCosting::conjunction,
            PlannedIcebergNode::And,
        )
    }

    fn disjunction(&self, children: Vec<PlannedNode>) -> PlannedNode {
        self.logical(
            children,
            PushdownCosting::disjunction,
            PlannedIcebergNode::Or,
        )
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
            // A set plans conservatively, so no exact tree holds one.
            PredicatePlan::Conservative(planned) => {
                let evaluation = planned.node.evaluation();
                let predicate = PlannedIcebergPredicate::new(
                    self.schema.schema_id(),
                    planned.node,
                );
                FilterPlan::conservative(predicate, planned.costing, evaluation)
            }
        })
    }

    fn pruning_units(&self) -> Option<f64> {
        self.pruning_units
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
