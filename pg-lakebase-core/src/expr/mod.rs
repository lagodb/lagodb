//! Typed PG `Expr` views, walkers, classification, and the runtime predicate
//! translator surface for CustomScan.

mod dml;
mod execution;
pub mod nodes;
mod planning;

// Preserve the provider-facing module paths while keeping plan-stage and
// executor-stage implementation ownership explicit.
pub use execution::{runtime_params, translator};
pub use planning::{predicate, split, walker};

pub(crate) use planning::{inspect, relation, rewrite};

pub use dml::DmlConflictPredicateBuilder;
pub use predicate::{
    PlanColumnRef, PlanDynamicRef, PlanLiteralRef, PlanOuterVarRef, PlanParamRef,
    PlanPredicate, PlanPredicateContext, PlanScalar, PredicateParseError,
};

/// Shared `(rel_oid, attno) -> attname` lookup for plan-stage `column_refs` and providers.
pub use split::ColumnNameResolver;
