//! PostgreSQL expression views, relation analysis, and planned filter pushdown.

pub(crate) mod contract;
pub(crate) mod execution;
pub mod pg;
pub(crate) mod planning;
pub mod pushdown;

pub(crate) use planning::{inspect, relation};

pub use contract::{
    ParamKey, PgComparisonIdentity, PgComparisonOp, PushdownContract, PushdownCosting,
};
