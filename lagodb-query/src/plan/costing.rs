//! Central PostgreSQL-scaled cost estimation for the executable operator tree.

mod join;
mod limit;

use lagodb_core::query_contract::ScanId;

use crate::ExecutionProfile;

use super::ir::{FilterNode, QueryFragment, QueryNode, ScanNode};
use super::scan_catalog::ScanCostTable;
use super::{
    AggregateNode, DistinctNode, JoinNode, LimitNode, ProjectNode, SortNode,
};
use join::JoinCostStrategy;
use limit::LimitCostStrategy;

// Charge a fixed PostgreSQL startup-cost unit for constructing the DataFusion
// execution state. Operator work remains estimated separately.
const ENGINE_SETUP_COST: f64 = 10.0;

/// PostgreSQL `Cost` pair for one complete offload path or operator subtree.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PlanCost {
    startup: f64,
    total: f64,
}

impl PlanCost {
    pub fn try_new(startup: f64, total: f64) -> Result<Self, QueryCostError> {
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

/// Existing engine limits and PostgreSQL's cost scale for one planning event.
#[derive(Debug, Clone, Copy)]
pub struct CostingContext {
    execution: ExecutionProfile,
    cpu_tuple_cost: f64,
    cpu_operator_cost: f64,
    sequential_page_cost: f64,
    block_size: usize,
    engine_setup_cost: f64,
}

impl CostingContext {
    pub fn try_new(
        execution: ExecutionProfile,
        cpu_tuple_cost: f64,
        cpu_operator_cost: f64,
        sequential_page_cost: f64,
        block_size: usize,
    ) -> Result<Self, QueryCostError> {
        Self::validate_component("cpu_tuple_cost", cpu_tuple_cost)?;
        Self::validate_component("cpu_operator_cost", cpu_operator_cost)?;
        Self::validate_component("sequential_page_cost", sequential_page_cost)?;
        if block_size == 0 {
            return Err(QueryCostError::InvalidBlockSize);
        }
        let engine_setup_cost = ENGINE_SETUP_COST;
        Self::validate_component("engine_setup_cost", engine_setup_cost)?;
        Ok(Self {
            execution,
            cpu_tuple_cost,
            cpu_operator_cost,
            sequential_page_cost,
            block_size,
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

    fn batches(self, rows: f64) -> f64 {
        if rows == 0.0 {
            0.0
        } else {
            (rows / self.execution.maximum_batch_rows().get() as f64).ceil()
        }
    }

    fn expression_cost(self, rows: f64, units: usize) -> f64 {
        rows * units as f64 * self.cpu_operator_cost
    }
}

/// Estimated cardinality and cumulative cost of one operator subtree.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PlanEstimate {
    rows: f64,
    batches: f64,
    cost: PlanCost,
}

impl PlanEstimate {
    fn try_new(
        rows: f64,
        batches: f64,
        cost: PlanCost,
    ) -> Result<Self, QueryCostError> {
        for (field, value) in [("rows", rows), ("batches", batches)] {
            if !value.is_finite() || value < 0.0 {
                return Err(QueryCostError::InvalidEstimate { field, value });
            }
        }
        Ok(Self {
            rows,
            batches,
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
    pub const fn cost(self) -> PlanCost {
        self.cost
    }
}

/// Recursive estimator for every executable IR node. Scan leaves consume only
/// the provider's existing row/byte estimate and PostgreSQL's BASEREL rows;
/// every upper operator owns its own cardinality and work estimate.
pub struct QueryCostEstimator<'a> {
    context: CostingContext,
    scans: &'a ScanCostTable,
    scan_output_rows: &'a [f64],
}

impl<'a> QueryCostEstimator<'a> {
    #[inline]
    pub const fn new(
        context: CostingContext,
        scans: &'a ScanCostTable,
        scan_output_rows: &'a [f64],
    ) -> Self {
        Self {
            context,
            scans,
            scan_output_rows,
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
        PlanEstimate::try_new(estimate.rows, estimate.batches, cost)
    }

    fn estimate_node(
        &self,
        node: &QueryNode,
    ) -> Result<PlanEstimate, QueryCostError> {
        match node {
            QueryNode::Scan(scan) => self.estimate_scan(scan),
            QueryNode::Join(join) => self.estimate_join(join),
            QueryNode::Aggregate(aggregate) => self.estimate_aggregate(aggregate),
            QueryNode::Distinct(distinct) => self.estimate_distinct(distinct),
            QueryNode::Filter(filter) => self.estimate_filter(filter),
            QueryNode::Project(project) => self.estimate_project(project),
            QueryNode::Sort(sort) => self.estimate_sort(sort),
            QueryNode::Limit(limit) => self.estimate_limit(limit),
        }
    }

    fn estimate_scan(&self, scan: &ScanNode) -> Result<PlanEstimate, QueryCostError> {
        let scan_cost =
            self.scans
                .cost(scan.scan())
                .ok_or(QueryCostError::MissingScan {
                    scan_id: scan.scan(),
                })?;
        let rows = *self.scan_output_rows.get(scan.scan().index()).ok_or(
            QueryCostError::MissingScan {
                scan_id: scan.scan(),
            },
        )?;
        let source_rows = scan_cost.rows_read();
        // Provider bytes are an expected physical-work estimate. Preserve the
        // fractional-page model used by relation CustomScan costing instead
        // of charging every source at least one rounded-up page.
        let pages = scan_cost.bytes_read() / self.context.block_size as f64;
        let io_cost = pages * self.context.sequential_page_cost;
        let filter_units = scan.filter().map_or(0, |filter| filter.cost_units());
        let total = io_cost
            + source_rows * self.context.cpu_tuple_cost
            + self.context.expression_cost(source_rows, filter_units);
        PlanEstimate::try_new(
            rows,
            self.context.batches(rows),
            PlanCost::try_new(
                scan_cost.startup_cost(),
                scan_cost.startup_cost() + total,
            )?,
        )
    }

    fn estimate_join(&self, join: &JoinNode) -> Result<PlanEstimate, QueryCostError> {
        let left = self.estimate_node(join.left())?;
        let right = self.estimate_node(join.right())?;
        JoinCostStrategy::new(self.context, join, left, right).estimate()
    }

    fn estimate_aggregate(
        &self,
        aggregate: &AggregateNode,
    ) -> Result<PlanEstimate, QueryCostError> {
        let input = self.estimate_node(aggregate.input())?;
        let expression_units = aggregate
            .groups()
            .iter()
            .map(|group| group.expression().cost_units())
            .chain(aggregate.aggregates().iter().map(|call| {
                call.argument().map_or(0, |value| value.cost_units())
                    + call.filter().map_or(0, |filter| filter.cost_units())
                    + call
                        .order_by()
                        .iter()
                        .map(|order| order.expression().cost_units())
                        .sum::<usize>()
            }))
            .sum::<usize>();
        let distinct_states = aggregate
            .aggregates()
            .iter()
            .filter(|call| call.uses_distinct_state())
            .count();
        let transition_units =
            aggregate.groups().len() + aggregate.aggregates().len() + distinct_states;
        let aggregate_cost = self
            .context
            .expression_cost(input.rows, expression_units + transition_units);
        let ordered_states = aggregate
            .aggregates()
            .iter()
            .filter(|call| !call.order_by().is_empty())
            .count();
        let ordered_cost = if input.rows <= 1.0 {
            0.0
        } else {
            self.context
                .expression_cost(input.rows * input.rows.log2(), ordered_states)
        };
        let rows = aggregate.estimated_rows();
        let startup = input.cost.total + aggregate_cost + ordered_cost;
        let total = startup + rows * self.context.cpu_tuple_cost;
        PlanEstimate::try_new(
            rows,
            self.context.batches(rows),
            PlanCost::try_new(startup, total)?,
        )
    }

    fn estimate_distinct(
        &self,
        distinct: &DistinctNode,
    ) -> Result<PlanEstimate, QueryCostError> {
        let input = self.estimate_node(distinct.input())?;
        let key_units = distinct
            .keys()
            .iter()
            .map(|key| 1 + key.expression().cost_units())
            .sum::<usize>();
        let work = self.context.expression_cost(input.rows, key_units);
        let rows = distinct.estimated_rows();
        let startup = input.cost.total + work;
        let total = startup + rows * self.context.cpu_tuple_cost;
        PlanEstimate::try_new(
            rows,
            self.context.batches(rows),
            PlanCost::try_new(startup, total)?,
        )
    }

    fn estimate_filter(
        &self,
        filter: &FilterNode,
    ) -> Result<PlanEstimate, QueryCostError> {
        let input = self.estimate_node(filter.input())?;
        let rows = filter.estimated_rows();
        let total = input.cost.total
            + self
                .context
                .expression_cost(input.rows, filter.predicate().cost_units());
        PlanEstimate::try_new(
            rows,
            self.context.batches(rows),
            PlanCost::try_new(input.cost.startup, total)?,
        )
    }

    fn estimate_project(
        &self,
        project: &ProjectNode,
    ) -> Result<PlanEstimate, QueryCostError> {
        let input = self.estimate_node(project.input())?;
        let expression_units = project
            .expressions()
            .iter()
            .map(|expression| expression.expression().cost_units())
            .sum::<usize>();
        let total = input.cost.total
            + input.rows * self.context.cpu_tuple_cost
            + self.context.expression_cost(input.rows, expression_units);
        PlanEstimate::try_new(
            input.rows,
            input.batches,
            PlanCost::try_new(input.cost.startup, total)?,
        )
    }

    fn estimate_sort(&self, sort: &SortNode) -> Result<PlanEstimate, QueryCostError> {
        let input = self.estimate_node(sort.input())?;
        let comparisons = if input.rows <= 1.0 {
            0.0
        } else {
            input.rows * input.rows.log2()
        };
        let sort_cost = self.context.expression_cost(comparisons, sort.keys().len());
        let startup = input.cost.total + sort_cost;
        let total = startup + input.rows * self.context.cpu_tuple_cost;
        PlanEstimate::try_new(
            input.rows,
            input.batches,
            PlanCost::try_new(startup, total)?,
        )
    }

    fn estimate_limit(
        &self,
        limit: &LimitNode,
    ) -> Result<PlanEstimate, QueryCostError> {
        let input = self.estimate_node(limit.input())?;
        LimitCostStrategy::new(self.context, limit, input).estimate()
    }
}

/// Invalid inputs or arithmetic while estimating one query fragment.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum QueryCostError {
    #[error("query cost {field} is invalid: {value}")]
    InvalidCost { field: &'static str, value: f64 },
    #[error("query estimate {field} is invalid: {value}")]
    InvalidEstimate { field: &'static str, value: f64 },
    #[error("query cost block size must be non-zero")]
    InvalidBlockSize,
    #[error("query cost scan {scan_id:?} is absent from the scan estimate table")]
    MissingScan { scan_id: ScanId },
}
