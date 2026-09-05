//! Provider-neutral semantic query plan substrate.

mod aggregate;
mod aggregate_semantics;
mod codec;
mod codec_wire;
mod costing;
mod distinct;
mod expression;
mod ir;
mod layout;
mod scan_catalog;
mod selected_plan;
mod semantics;
mod table_scan_filter;
mod validation;

pub use aggregate::{
    AggCall, AggregateArguments, AggregateKind, AggregateNode, AggregateOrderExpr,
    GroupExpr, SortDirection,
};
pub use codec::{QueryPlanData, QueryPlanDataError};
pub use costing::{
    CostingContext, PlanCost, PlanEstimate, QueryCostError, QueryCostEstimator,
};
pub use distinct::{DistinctExpr, DistinctNode};
pub use expression::{
    BooleanTestKind, CaseWhen, ExecutionExpr, ExecutionScalarRepr, PostgresEvalExpr,
    PostgresEvalInput, PostgresExprVolatility, ScalarFunctionKind,
};
pub use ir::{
    FilterNode, ProjectNode, QueryFragment, QueryNode, QueryPlanError,
    QueryPlanSummary, ScanNode,
};
pub use lagodb_core::query_contract::OutputId;
pub use layout::{QueryTupleLayout, QueryTupleSlot};
pub use scan_catalog::{ScanCatalog, ScanEstimateTable};
pub use selected_plan::{
    PlannedTableScan, SelectedQueryPlan, SelectedQueryPlanError,
    TableScanRuntimeBindings,
};
pub use semantics::{ComparisonKind, ScalarSemantics};
pub use table_scan_filter::TableScanFilterExplain;
