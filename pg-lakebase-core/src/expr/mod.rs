//! Typed PG `Expr` views, walkers, classification, and the runtime predicate
//! translator surface for CustomScan.

mod column;
pub(crate) mod contract;
pub(crate) mod execution;
pub mod pg;
pub(crate) mod planning;

// Preserve the provider-facing module paths while keeping plan-stage and
// executor-stage implementation ownership explicit.
pub use execution::builder::PredicateBuilder;
pub use execution::params;
pub use execution::params::{
    ExecParamRef, ExternParamRef, ResolvedParam, RuntimeParamError,
    RuntimeParamResolver,
};
pub use execution::translator;
pub use planning::predicate;

pub use planning::{classify, split};
pub(crate) use planning::{inspect, relation};

pub use contract::{
    ColumnRef, ParamKey, PgComparisonIdentity, PgComparisonOp, PushdownContract,
    PushdownCosting, QualPushdownDecision,
};

pub use predicate::{
    PlanColumnRef, PlanDynamicRef, PlanLiteralRef, PlanOuterVarRef, PlanParamRef,
    PlanPredicate, PlanPredicateContext, PlanScalar, PredicateParseError,
};

/// Shared `(rel_oid, attno) -> attname` lookup for plan-stage `column_refs` and providers.
pub use column::ColumnNameResolver;
