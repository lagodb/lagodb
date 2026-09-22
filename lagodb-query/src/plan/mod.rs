//! Provider-neutral semantic query plan substrate.

mod codec;
mod costing;
mod explain;
mod ir;
mod layout;
mod selected_plan;
mod table_scan_filter;

pub use codec::{QueryPlanData, QueryPlanDataError};
pub use costing::{
    CostingContext, PlanCost, PlanEstimate, QueryCostError, QueryCostEstimator,
    ScanCostTable,
};
pub use explain::{
    PlanExplainNode, PlanExplainProperty, PlanExplainRelation, PlanExplainValue,
};
pub use ir::{
    AggCall, AggregateArguments, AggregateKind, AggregateNode, AggregateOrderExpr,
    BooleanTestKind, CaseWhen, ComparisonKind, Decimal128Semantics, DistinctExpr,
    DistinctNode, ExecutionExpr, ExecutionScalarRepr, FilterNode, GroupExpr, JoinKey,
    JoinNode, JoinType, LimitEstimate, LimitNode, MarkFilter, PostgresEvalExpr,
    PostgresEvalInput, PostgresExprVolatility, ProjectExpr, ProjectNode,
    QueryFragment, QueryNode, QueryPlanError, ScalarFunctionKind, ScalarSemantics,
    ScanNode, SortDirection, SortExpr, SortNode,
};
pub use lagodb_core::query_contract::OutputId;
pub use layout::{QueryTupleLayout, QueryTupleSlot};
pub use selected_plan::{
    PlannedTableScan, SelectedQueryPlan, SelectedQueryPlanError,
};
pub use table_scan_filter::TableScanFilterExplain;
