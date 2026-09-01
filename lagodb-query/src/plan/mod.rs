//! Provider-neutral semantic query plan substrate.

mod costing;
mod envelope;
mod identity;
mod ir;
mod plan_data;
mod planning;

pub use costing::{
    CostingContext, PlanCost, PlanEstimate, QueryCostError, QueryCostEstimator,
};
pub use envelope::{DecodedQuerySource, QueryPlanEnvelope, QueryPlanEnvelopeError};
pub use identity::OutputId;
pub use ir::{
    AggregateExpression, AggregateNode, CountStar, ProjectNode, QueryFragment,
    QueryNode, QueryPlanError, QueryTupleLayout, QueryTupleSlot, SourceNode,
};
pub use plan_data::{QueryPlanData, QueryPlanDataError};
pub use planning::{SourceCatalog, SourceCatalogError, SourceEstimateTable};
