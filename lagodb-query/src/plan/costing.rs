//! Central operator-tree cost estimation for the current DataFusion engine.

use std::mem::size_of;

use lagodb_core::query_contract::ScanId;

use crate::ExecutionProfile;

use super::ir::{FilterNode, ProjectNode, QueryFragment, QueryNode, ScanNode};
use super::scan_catalog::ScanEstimateTable;
use super::{AggregateNode, DistinctNode};

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

    /// Planner policy override applied only after every semantic/scan gate.
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
    scans: &'a ScanEstimateTable,
    scan_output_rows: &'a [f64],
    aggregate_rows: f64,
    output_rows: f64,
}

impl<'a> QueryCostEstimator<'a> {
    #[inline]
    pub const fn new(
        context: CostingContext,
        scans: &'a ScanEstimateTable,
        scan_output_rows: &'a [f64],
        aggregate_rows: f64,
        output_rows: f64,
    ) -> Self {
        Self {
            context,
            scans,
            scan_output_rows,
            aggregate_rows,
            output_rows,
        }
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
            QueryNode::Scan(scan) => self.estimate_scan(scan),
            QueryNode::Aggregate(aggregate) => self.estimate_aggregate(aggregate),
            QueryNode::Distinct(distinct) => self.estimate_distinct(distinct),
            QueryNode::Filter(filter) => self.estimate_filter(filter),
            QueryNode::Project(project) => self.estimate_project(project),
        }
    }

    fn estimate_scan(&self, scan: &ScanNode) -> Result<PlanEstimate, QueryCostError> {
        let estimate =
            self.scans
                .estimate(scan.scan())
                .ok_or(QueryCostError::MissingScan {
                    scan_id: scan.scan(),
                })?;
        let source_rows = estimate.estimated_rows();
        let rows = *self.scan_output_rows.get(scan.scan().index()).ok_or(
            QueryCostError::MissingScan {
                scan_id: scan.scan(),
            },
        )?;
        let maximum_batch_rows =
            self.context.execution.maximum_batch_rows().get() as f64;
        let source_batches = if source_rows == 0.0 {
            0.0
        } else {
            (source_rows / maximum_batch_rows).ceil()
        };
        let batches = if rows == 0.0 {
            0.0
        } else {
            (rows / maximum_batch_rows).ceil()
        };
        let startup = self.context.cpu_tuple_cost;
        let filter_cost = if scan.filter().is_some() {
            source_rows * self.context.cpu_operator_cost
        } else {
            0.0
        };
        let total =
            startup + source_batches * self.context.cpu_tuple_cost + filter_cost;
        PlanEstimate::try_new(rows, batches, 0.0, PlanCost::try_new(startup, total)?)
    }

    fn estimate_aggregate(
        &self,
        aggregate: &AggregateNode,
    ) -> Result<PlanEstimate, QueryCostError> {
        let input = self.estimate_node(aggregate.input())?;
        let aggregate_work = input.rows
            * (aggregate.groups().len() + aggregate.aggregates().len()) as f64
            * self.context.cpu_operator_cost;
        let distinct_work = input.rows
            * aggregate
                .aggregates()
                .iter()
                .filter(|aggregate| aggregate.uses_distinct_state())
                .count() as f64
            * self.context.cpu_operator_cost;
        let startup = input.cost.total + aggregate_work + distinct_work;
        let rows = if aggregate.groups().is_empty() {
            1.0
        } else {
            self.aggregate_rows
        };
        let total = startup + rows * self.context.cpu_tuple_cost;
        let output_columns = aggregate.groups().len() + aggregate.aggregates().len();
        let output_bytes = rows * (output_columns * size_of::<i64>()) as f64;
        let batches = if rows == 0.0 {
            0.0
        } else {
            (rows / self.context.execution.maximum_batch_rows().get() as f64).ceil()
        };
        PlanEstimate::try_new(
            rows,
            batches,
            output_bytes,
            PlanCost::try_new(startup, total)?,
        )
    }

    fn estimate_filter(
        &self,
        filter: &FilterNode,
    ) -> Result<PlanEstimate, QueryCostError> {
        let input = self.estimate_node(filter.input())?;
        let cost = PlanCost::try_new(
            input.cost.startup,
            input.cost.total + input.rows * self.context.cpu_operator_cost,
        )?;
        let batches = if self.output_rows == 0.0 {
            0.0
        } else {
            (self.output_rows
                / self.context.execution.maximum_batch_rows().get() as f64)
                .ceil()
        };
        let output_bytes = if input.rows == 0.0 {
            0.0
        } else {
            input.output_bytes * self.output_rows / input.rows
        };
        PlanEstimate::try_new(self.output_rows, batches, output_bytes, cost)
    }

    fn estimate_distinct(
        &self,
        distinct: &DistinctNode,
    ) -> Result<PlanEstimate, QueryCostError> {
        let input = self.estimate_node(distinct.input())?;
        let work = input.rows
            * distinct.keys().len() as f64
            * self.context.cpu_operator_cost;
        let startup = input.cost.total + work;
        let rows = self.aggregate_rows;
        let total = startup + rows * self.context.cpu_tuple_cost;
        let output_bytes = rows * (distinct.keys().len() * size_of::<i64>()) as f64;
        let batches = if rows == 0.0 {
            0.0
        } else {
            (rows / self.context.execution.maximum_batch_rows().get() as f64).ceil()
        };
        PlanEstimate::try_new(
            rows,
            batches,
            output_bytes,
            PlanCost::try_new(startup, total)?,
        )
    }

    fn estimate_project(
        &self,
        project: &ProjectNode,
    ) -> Result<PlanEstimate, QueryCostError> {
        let input = self.estimate_node(project.input())?;
        let cost = PlanCost::try_new(
            input.cost.startup,
            input.cost.total + input.rows * self.context.cpu_tuple_cost,
        )?;
        let output_bytes =
            input.rows * (project.outputs().len() * size_of::<i64>()) as f64;
        PlanEstimate::try_new(input.rows, input.batches, output_bytes, cost)
    }
}

/// Invalid inputs or arithmetic while estimating one query fragment.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum QueryCostError {
    #[error("query cost {field} is invalid: {value}")]
    InvalidCost { field: &'static str, value: f64 },
    #[error("query estimate {field} is invalid: {value}")]
    InvalidEstimate { field: &'static str, value: f64 },
    #[error("query cost scan {scan_id:?} is absent from the scan estimate table")]
    MissingScan { scan_id: ScanId },
}
