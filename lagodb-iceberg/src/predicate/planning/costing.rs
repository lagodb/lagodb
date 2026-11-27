//! Partition-spec and file-metrics pruning cost model for Iceberg filters.

use iceberg_lite::spec::{TableMetadata, Transform};
use lagodb_core::expr::{PgComparisonKind, PushdownCosting, RuntimeValueSource};

use crate::predicate::ir::PlannedValueType;
use crate::predicate::semantics::{FloatComparison, PredicateCapability};

use super::admission::SupportedComparison;

/// What a pruning filter needs from a partition transform to keep only the
/// files whose partition values can match it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PartitionMatch {
    /// Equality and null tests, which every non-void transform projects.
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

/// Partition-layout pruning capabilities shared across all specs of a table.
pub(super) struct PredicateCostModel {
    /// Field ids that every partition spec derives a partition value from,
    /// each with the filters all of those specs project.
    layout_sources: Vec<(i32, PartitionMatch)>,
}

impl PredicateCostModel {
    /// Source fields whose partition values prune every file. A scan projects
    /// a filter by each file's own spec, so a field missing from one spec,
    /// such as that of files written before the table was partitioned, leaves
    /// those files to file metrics. The specs of the table metadata include
    /// every spec its files can carry.
    pub(super) fn from_metadata(metadata: &TableMetadata) -> Self {
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
            return Self {
                layout_sources: Vec::new(),
            };
        };
        for sources in specs {
            common.retain_mut(|(source_id, matched)| {
                Self::layout_match(&sources, *source_id).is_some_and(|other| {
                    *matched = (*matched).min(other);
                    true
                })
            });
        }
        Self {
            layout_sources: common,
        }
    }

    pub(super) fn value_pruning(&self, field_id: i32) -> PushdownCosting {
        self.pruning_costing(field_id, PartitionMatch::Value)
    }

    pub(super) fn order_pruning(&self, field_id: i32) -> PushdownCosting {
        self.pruning_costing(field_id, PartitionMatch::Order)
    }

    /// Costing of a comparison of `field_id` with a value from `source`.
    pub(super) fn comparison(
        &self,
        supported: SupportedComparison,
        planned_operator: PgComparisonKind,
        source: RuntimeValueSource,
        value_type: PlannedValueType,
        field_id: i32,
    ) -> PushdownCosting {
        let mut costing =
            self.base_comparison(supported, source, value_type, field_id);
        // A float `>` / `>=` binds as `x > c OR isnan(x)`. Row-group metrics
        // never exclude `isnan`, and file metrics do only where the NaN count
        // is recorded as zero, so the bound shape is costed as that OR. A
        // runtime value bound as NaN takes another shape, which this covers
        // conservatively.
        if matches!(
            value_type,
            PlannedValueType::Float
                | PlannedValueType::Double
                | PlannedValueType::FloatToDouble
                | PlannedValueType::DoubleToFloat
        ) && FloatComparison::may_add_nan_disjunction(planned_operator)
        {
            costing = costing.disjunction(self.value_pruning(field_id));
        }
        costing
    }

    /// Costing of an `= ANY` set predicate.
    ///
    /// A set prunes as the OR of its equalities, as `partprune.c` expands a
    /// `ScalarArrayOpExpr`. Partition values match a set of any size, but file
    /// metrics skip one of more than 200 values (iceberg-lite's
    /// `IN_PREDICATE_LIMIT`), a size planning does not know, so only layout
    /// pruning carries over. That every row of a selected file reaches
    /// PostgreSQL is priced by the filter's evaluation
    /// (`PlannedNode::evaluation`), not by its costing.
    pub(super) fn in_set(
        &self,
        supported: SupportedComparison,
        source: RuntimeValueSource,
        value_type: PlannedValueType,
        field_id: i32,
    ) -> PushdownCosting {
        match self.base_comparison(supported, source, value_type, field_id) {
            PushdownCosting::LayoutPruning => PushdownCosting::LayoutPruning,
            PushdownCosting::StatisticsPruning
            | PushdownCosting::UncostedBestEffort => {
                PushdownCosting::UncostedBestEffort
            }
        }
    }

    fn base_comparison(
        &self,
        supported: SupportedComparison,
        source: RuntimeValueSource,
        value_type: PlannedValueType,
        field_id: i32,
    ) -> PushdownCosting {
        // A constant temporal literal prunes at its PostgreSQL selectivity.
        // An infinite one is not representable in Iceberg and leaves its rows
        // unpruned at bind time, a documented costing limit.
        if supported.capability == PredicateCapability::Conservative
            && (source != RuntimeValueSource::Constant
                || matches!(value_type, PlannedValueType::FixedBinary(_)))
        {
            return PushdownCosting::UncostedBestEffort;
        }
        match supported.operator {
            // A bucket hash projects only equality onto partition values.
            PgComparisonKind::Equal => self.value_pruning(field_id),
            // Truncate and temporal transforms do not project `<>`.
            PgComparisonKind::NotEqual => PushdownCosting::StatisticsPruning,
            PgComparisonKind::Less
            | PgComparisonKind::LessEqual
            | PgComparisonKind::Greater
            | PgComparisonKind::GreaterEqual => self.order_pruning(field_id),
        }
    }

    /// Costing of a pruning filter on `field_id` that needs `needed` from a
    /// partition transform. Partition values prune whole files when every spec
    /// projects the filter; file metrics prune it otherwise.
    fn pruning_costing(
        &self,
        field_id: i32,
        needed: PartitionMatch,
    ) -> PushdownCosting {
        match Self::layout_match(&self.layout_sources, field_id) {
            Some(matched) if matched >= needed => PushdownCosting::LayoutPruning,
            _ => PushdownCosting::StatisticsPruning,
        }
    }

    fn layout_match(
        sources: &[(i32, PartitionMatch)],
        field_id: i32,
    ) -> Option<PartitionMatch> {
        sources.iter().find_map(|&(source_id, matched)| {
            (source_id == field_id).then_some(matched)
        })
    }
}
