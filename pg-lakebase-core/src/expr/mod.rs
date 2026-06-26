//! Typed PG `Expr` views, walkers, classification, and the runtime predicate
//! translator surface for CustomScan.

mod conflict_filter;
pub(crate) mod inspect;
pub mod nodes;
pub mod predicate;
mod relation;
mod rewrite;
pub mod runtime_params;
pub mod split;
pub mod translator;
pub mod walker;

pub use conflict_filter::DmlConflictPredicateBuilder;
pub use predicate::{
    PlanColumnRef, PlanDynamicRef, PlanLiteralRef, PlanOuterVarRef, PlanParamRef,
    PlanPredicate, PlanPredicateContext, PlanScalar, PredicateParseError,
};

/// Shared `(rel_oid, attno) -> attname` lookup for plan-stage `column_refs` and providers.
pub use split::ColumnNameResolver;

/// Property tests: residual/pushed split equivalence (Rust-only model).
#[cfg(test)]
mod pbt_split;

/// Property tests: pseudoconstant skip and security gating (Rust-only model).
#[cfg(test)]
mod pbt_security_gate;
