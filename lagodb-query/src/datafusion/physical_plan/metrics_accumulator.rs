//! Statement-level physical metrics retained across plan recompilation.

use std::any::{Any, TypeId};
use std::mem;
use std::sync::Arc;
use std::time::Duration;

use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};

use super::CompiledPhysicalPlan;
use crate::plan::{PlanExplainNode, PlanExplainProperty};

/// Metrics from every executed physical-plan instance in one PostgreSQL
/// statement. A topology variant is keyed by concrete operator type and child
/// position, so metrics are never merged merely because display names match.
/// Node details remain visible only while every merged instance reports the
/// same value; a topology aggregate never presents one instance as universal.
#[derive(Debug, Default)]
pub(in crate::datafusion) struct PhysicalPlanMetricsAccumulator {
    variants: Vec<PhysicalPlanMetricsVariant>,
}

impl PhysicalPlanMetricsAccumulator {
    /// Retain one executed plan before the execution owner replaces it.
    /// This runs only at a statement rescan/recompile boundary.
    pub(in crate::datafusion) fn record(&mut self, plan: &CompiledPhysicalPlan) {
        Self::record_in(&mut self.variants, plan.plan.as_ref());
    }

    /// Merge the current plan with every retained predecessor and build the
    /// typed EXPLAIN tree without mutating the live accumulator.
    pub(in crate::datafusion) fn analyze_tree(
        &self,
        current: &CompiledPhysicalPlan,
        include_timing: bool,
    ) -> PlanExplainNode {
        let mut variants = self.variants.clone();
        Self::record_in(&mut variants, current.plan.as_ref());

        if variants.len() == 1 {
            let variant = variants.swap_remove(0);
            return variant
                .tree
                .into_explain(variant.instances, include_timing)
                .with_property("Metrics Scope", "all physical plan instances")
                .with_uinteger_property(
                    "Physical Plan Instances",
                    variant.instances,
                );
        }

        let properties = vec![
            PlanExplainNode::property(
                "Metrics Scope",
                "all physical plan instances, grouped by topology",
            ),
            PlanExplainNode::uinteger_property(
                "Physical Plan Variants",
                u64::try_from(variants.len())
                    .expect("physical plan variant count fits in u64"),
            ),
        ];
        let children = variants
            .into_iter()
            .enumerate()
            .map(|(index, variant)| {
                variant
                    .tree
                    .into_explain(variant.instances, include_timing)
                    .with_uinteger_property(
                        "Plan Variant",
                        u64::try_from(index + 1)
                            .expect("physical plan variant index fits in u64"),
                    )
                    .with_uinteger_property(
                        "Physical Plan Instances",
                        variant.instances,
                    )
            })
            .collect();
        PlanExplainNode::new("Physical Plan Metrics", properties, children)
    }

    fn record_in(
        variants: &mut Vec<PhysicalPlanMetricsVariant>,
        plan: &dyn ExecutionPlan,
    ) {
        if let Some(variant) = variants
            .iter_mut()
            .find(|variant| variant.tree.same_topology(plan))
        {
            variant.tree.merge_metrics(plan);
            variant.instances += 1;
        } else {
            variants.push(PhysicalPlanMetricsVariant {
                tree: PhysicalPlanMetricsNode::capture(plan),
                instances: 1,
            });
        }
    }
}

#[derive(Debug, Clone)]
struct PhysicalPlanMetricsVariant {
    tree: PhysicalPlanMetricsNode,
    instances: u64,
}

#[derive(Debug, Clone)]
struct PhysicalPlanMetricsNode {
    operator_type: TypeId,
    node_type: String,
    details: PhysicalPlanDetails,
    metrics: MetricsSet,
    children: Vec<Self>,
}

#[derive(Debug, Clone)]
enum PhysicalPlanDetails {
    Uniform(String),
    Varies,
}

impl PhysicalPlanDetails {
    fn observe(&mut self, details: String) {
        let Self::Uniform(current) = self else {
            return;
        };
        if current != &details {
            *self = Self::Varies;
        }
    }

    fn into_explain_value(self, instances: u64) -> String {
        match self {
            Self::Uniform(details) => details,
            Self::Varies => {
                debug_assert!(instances > 1);
                format!("[Varies across {instances} physical plan instances]")
            }
        }
    }
}

impl PhysicalPlanMetricsNode {
    fn capture(plan: &dyn ExecutionPlan) -> Self {
        let details = PhysicalPlanDetails::Uniform(Self::display_details(plan));
        let children = plan
            .children()
            .into_iter()
            .map(|child| Self::capture(child.as_ref()))
            .collect();
        Self {
            operator_type: (plan as &dyn Any).type_id(),
            node_type: plan.name().to_owned(),
            details,
            metrics: plan.metrics().unwrap_or_default().aggregate_by_name(),
            children,
        }
    }

    fn same_topology(&self, plan: &dyn ExecutionPlan) -> bool {
        let children = plan.children();
        self.operator_type == (plan as &dyn Any).type_id()
            && self.children.len() == children.len()
            && self
                .children
                .iter()
                .zip(children)
                .all(|(current, next)| current.same_topology(next.as_ref()))
    }

    fn merge_metrics(&mut self, plan: &dyn ExecutionPlan) {
        debug_assert_eq!(self.operator_type, (plan as &dyn Any).type_id());
        self.details.observe(Self::display_details(plan));
        let metrics = plan.metrics().unwrap_or_default().aggregate_by_name();
        for metric in metrics.iter() {
            self.metrics.push(Arc::clone(metric));
        }
        self.metrics = mem::take(&mut self.metrics).aggregate_by_name();
        let children = plan.children();
        debug_assert_eq!(self.children.len(), children.len());
        for (current, next) in self.children.iter_mut().zip(children) {
            current.merge_metrics(next.as_ref());
        }
    }

    fn display_details(plan: &dyn ExecutionPlan) -> String {
        let mut details = DisplayableExecutionPlan::new(plan).one_line().to_string();
        details.truncate(details.trim_end().len());
        details
    }

    fn into_explain(self, instances: u64, include_timing: bool) -> PlanExplainNode {
        let mut properties = vec![PlanExplainNode::property(
            "Details",
            self.details.into_explain_value(instances),
        )];
        let metrics = self
            .metrics
            .aggregate_by_name()
            .sorted_for_display()
            .timestamps_removed();
        for metric in metrics.iter() {
            if !include_timing
                && matches!(
                    metric.value(),
                    MetricValue::ElapsedCompute(_) | MetricValue::Time { .. }
                )
            {
                continue;
            }
            properties.push(Self::metric_property(metric.value()));
        }
        let children = self
            .children
            .into_iter()
            .map(|child| child.into_explain(instances, include_timing))
            .collect();
        PlanExplainNode::new(self.node_type, properties, children)
    }

    fn metric_property(metric: &MetricValue) -> PlanExplainProperty {
        let name = format!("Metric: {}", metric.name());
        match metric {
            MetricValue::OutputRows(value)
            | MetricValue::SpillCount(value)
            | MetricValue::SpilledBytes(value)
            | MetricValue::OutputBytes(value)
            | MetricValue::OutputBatches(value)
            | MetricValue::SpilledRows(value)
            | MetricValue::Count { count: value, .. } => {
                PlanExplainNode::uinteger_property(
                    name,
                    Self::metric_integer(value.value()),
                )
            }
            MetricValue::CurrentMemoryUsage(value)
            | MetricValue::Gauge { gauge: value, .. }
            | MetricValue::PeakMemoryUsage { gauge: value, .. } => {
                PlanExplainNode::uinteger_property(
                    name,
                    Self::metric_integer(value.value()),
                )
            }
            MetricValue::ElapsedCompute(value)
            | MetricValue::Time { time: value, .. } => {
                let nanoseconds = Self::metric_integer(value.value());
                PlanExplainNode::float_property(
                    name,
                    Duration::from_nanos(nanoseconds).as_secs_f64() * 1_000.0,
                    3,
                    Some("ms"),
                )
            }
            MetricValue::StartTimestamp(_)
            | MetricValue::EndTimestamp(_)
            | MetricValue::PruningMetrics { .. }
            | MetricValue::Ratio { .. }
            | MetricValue::Custom { .. } => {
                PlanExplainNode::property(name, metric.to_string())
            }
        }
    }

    fn metric_integer(value: usize) -> u64 {
        u64::try_from(value).expect("DataFusion metric value fits in u64")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uniform_details_remain_visible_for_one_or_many_instances() {
        let mut details =
            PhysicalPlanDetails::Uniform("FilterExec: value = 1".to_owned());

        assert_eq!(
            details.clone().into_explain_value(1),
            "FilterExec: value = 1"
        );

        details.observe("FilterExec: value = 1".to_owned());

        assert_eq!(details.into_explain_value(2), "FilterExec: value = 1");
    }

    #[test]
    fn varying_details_hide_instance_specific_value() {
        let mut details =
            PhysicalPlanDetails::Uniform("FilterExec: value = 1".to_owned());

        details.observe("FilterExec: value = 2".to_owned());
        details.observe("FilterExec: value = 1".to_owned());

        assert_eq!(
            details.into_explain_value(2),
            "[Varies across 2 physical plan instances]"
        );
    }
}
