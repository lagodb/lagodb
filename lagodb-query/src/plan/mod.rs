//! Provider-neutral semantic query plan substrate.

mod aggregate;
mod aggregate_semantics;
mod codec;
mod codec_wire;
mod costing;
mod distinct;
mod explain;
mod expression;
mod ir;
mod join;
mod layout;
mod limit;
mod postgres_fallback;
mod project;
mod scan_catalog;
mod selected_plan;
mod semantics;
mod sort;
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
pub use explain::{
    PlanExplainNode, PlanExplainProperty, PlanExplainRelation, PlanExplainValue,
};
pub use expression::{
    BooleanTestKind, CaseWhen, ExecutionExpr, ExecutionScalarRepr, PostgresEvalExpr,
    PostgresEvalInput, PostgresExprVolatility, ScalarFunctionKind,
};
pub use ir::{FilterNode, QueryFragment, QueryNode, QueryPlanError, ScanNode};
pub use join::{JoinKey, JoinNode, JoinType, MarkFilter};
pub use lagodb_core::query_contract::OutputId;
pub use layout::{QueryTupleLayout, QueryTupleSlot};
pub use limit::{LimitEstimate, LimitNode};
pub use project::{ProjectExpr, ProjectNode};
pub use scan_catalog::{ScanCatalog, ScanCostTable};
pub use selected_plan::{
    PlannedTableScan, SelectedQueryPlan, SelectedQueryPlanError,
};
pub use semantics::{ComparisonKind, Decimal128Semantics, ScalarSemantics};
pub use sort::{SortExpr, SortNode};
pub use table_scan_filter::TableScanFilterExplain;
