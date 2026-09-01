//! Central operator-tree cost estimation for the current DataFusion engine.

use std::mem::size_of;

use lagodb_core::query_contract::SourceId;

use crate::ExecutionProfile;

use super::ir::{
    AggregateExpression, AggregateNode, ProjectNode, QueryFragment, QueryNode,
    SourceNode,
};
use super::planning::SourceEstimateTable;

const ENGINE_SETUP_TUPLE_EQUIVALENTS: f64 = 4_096.0;

/// PostgreSQL `Cost` pair for one complete offload path or operator subtree.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PlanCost {
    startup: f64,
    total: f64,
}

impl PlanCost {
    fn try_new(startup: f64, total: f64) -> Result<Self, QueryCostError> {
        if !startup.is_finite() || startup < 0.0 {
            return Err(QueryCostError::InvalidCost {
                field: "startup",
                value: startup,
            });
        }
        if !total.is_finite() || total < startup {
            return Err(QueryCostError::InvalidCost {
                field: "total",
                value: total,
            });
        }
        Ok(Self { startup, total })
    }

    /// Planner policy override applied only after every semantic/source gate.
    #[inline]
    pub const fn forced() -> Self {
        Self {
            startup: 0.0,
            total: 1.0,
        }
    }

    #[inline]
    pub const fn startup(self) -> f64 {
        self.startup
    }

    #[inline]
    pub const fn total(self) -> f64 {
        self.total
    }
}

/// Engine execution facts and PostgreSQL cost scale for one planning event.
#[derive(Debug, Clone, Copy)]
pub struct CostingContext {
    execution: ExecutionProfile,
    cpu_tuple_cost: f64,
    cpu_operator_cost: f64,
    engine_setup_cost: f64,
}

impl CostingContext {
    pub fn try_new(
        execution: ExecutionProfile,
        cpu_tuple_cost: f64,
        cpu_operator_cost: f64,
    ) -> Result<Self, QueryCostError> {
        Self::validate_component("cpu_tuple_cost", cpu_tuple_cost)?;
        Self::validate_component("cpu_operator_cost", cpu_operator_cost)?;
        let engine_setup_cost = ENGINE_SETUP_TUPLE_EQUIVALENTS * cpu_tuple_cost;
        Self::validate_component("engine_setup_cost", engine_setup_cost)?;
        Ok(Self {
            execution,
            cpu_tuple_cost,
            cpu_operator_cost,
            engine_setup_cost,
        })
    }

    fn validate_component(
        field: &'static str,
        value: f64,
    ) -> Result<(), QueryCostError> {
        if value.is_finite() && value >= 0.0 {
            Ok(())
        } else {
            Err(QueryCostError::InvalidCost { field, value })
        }
    }
}

/// Estimated physical shape and cumulative cost of one operator subtree.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PlanEstimate {
    rows: f64,
    batches: f64,
    output_bytes: f64,
    cost: PlanCost,
}

impl PlanEstimate {
    fn try_new(
        rows: f64,
        batches: f64,
        output_bytes: f64,
        cost: PlanCost,
    ) -> Result<Self, QueryCostError> {
        for (field, value) in [
            ("rows", rows),
            ("batches", batches),
            ("output_bytes", output_bytes),
        ] {
            if !value.is_finite() || value < 0.0 {
                return Err(QueryCostError::InvalidEstimate { field, value });
            }
        }
        Ok(Self {
            rows,
            batches,
            output_bytes,
            cost,
        })
    }

    #[inline]
    pub const fn rows(self) -> f64 {
        self.rows
    }

    #[inline]
    pub const fn batches(self) -> f64 {
        self.batches
    }

    #[inline]
    pub const fn output_bytes(self) -> f64 {
        self.output_bytes
    }

    #[inline]
    pub const fn cost(self) -> PlanCost {
        self.cost
    }
}

/// Concrete recursive estimator for every currently executable IR node.
pub struct QueryCostEstimator<'a> {
    context: CostingContext,
    sources: &'a SourceEstimateTable,
}

impl<'a> QueryCostEstimator<'a> {
    #[inline]
    pub const fn new(
        context: CostingContext,
        sources: &'a SourceEstimateTable,
    ) -> Self {
        Self { context, sources }
    }

    pub fn estimate(
        &self,
        fragment: &QueryFragment,
    ) -> Result<PlanEstimate, QueryCostError> {
        let estimate = self.estimate_node(fragment.root())?;
        let cost = PlanCost::try_new(
            estimate.cost.startup + self.context.engine_setup_cost,
            estimate.cost.total + self.context.engine_setup_cost,
        )?;
        PlanEstimate::try_new(
            estimate.rows,
            estimate.batches,
            estimate.output_bytes,
            cost,
        )
    }

    fn estimate_node(
        &self,
        node: &QueryNode,
    ) -> Result<PlanEstimate, QueryCostError> {
        match node {
            QueryNode::Source(source) => self.estimate_source(source),
            QueryNode::Aggregate(aggregate) => self.estimate_aggregate(aggregate),
            QueryNode::Project(project) => self.estimate_project(project),
        }
    }

    fn estimate_source(
        &self,
        source: &SourceNode,
    ) -> Result<PlanEstimate, QueryCostError> {
        let estimate = self.sources.estimate(source.source()).ok_or(
            QueryCostError::MissingSource {
                source_id: source.source(),
            },
        )?;
        let rows = estimate.estimated_rows();
        let maximum_batch_rows =
            self.context.execution.maximum_batch_rows().get() as f64;
        let batches = if rows == 0.0 {
            0.0
        } else {
            (rows / maximum_batch_rows).ceil()
        };
        let startup = self.context.cpu_tuple_cost;
        let total = startup + batches * self.context.cpu_tuple_cost;
        PlanEstimate::try_new(rows, batches, 0.0, PlanCost::try_new(startup, total)?)
    }

    fn estimate_aggregate(
        &self,
        aggregate: &AggregateNode,
    ) -> Result<PlanEstimate, QueryCostError> {
        let input = self.estimate_node(aggregate.input())?;
        let [expression] = aggregate.aggregates() else {
            return Err(QueryCostError::InvalidScalarAggregate);
        };
        let count_work = match expression {
            AggregateExpression::CountStar(_) => {
                (input.batches + 1.0) * self.context.cpu_operator_cost
            }
        };
        let startup = input.cost.total + count_work;
        PlanEstimate::try_new(
            1.0,
            1.0,
            size_of::<i64>() as f64,
            PlanCost::try_new(startup, startup)?,
        )
    }

    fn estimate_project(
        &self,
        project: &ProjectNode,
    ) -> Result<PlanEstimate, QueryCostError> {
        let input = self.estimate_node(project.input())?;
        let cost = PlanCost::try_new(
            input.cost.startup,
            input.cost.total + self.context.cpu_tuple_cost,
        )?;
        PlanEstimate::try_new(input.rows, input.batches, input.output_bytes, cost)
    }
}

/// Invalid inputs or arithmetic while estimating one query fragment.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum QueryCostError {
    #[error("query cost {field} is invalid: {value}")]
    InvalidCost { field: &'static str, value: f64 },
    #[error("query estimate {field} is invalid: {value}")]
    InvalidEstimate { field: &'static str, value: f64 },
    #[error(
        "query cost source {source_id:?} is absent from the source estimate table"
    )]
    MissingSource { source_id: SourceId },
    #[error("the current query cost model requires exactly one CountStar aggregate")]
    InvalidScalarAggregate,
}
