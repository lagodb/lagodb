//! Planned filter pushdown: stable IR, negotiation, persistence, and binding.

mod codec;
mod contract;
mod expression_codec;
mod ir;
mod negotiate;
mod negotiation;
mod normalize;
mod plan_set;
mod runtime;
mod scope;
mod tree;

pub(crate) use codec::{EncodedFilterData, FilterDataCodec, FilterDataError};
pub(crate) use contract::{BoundFilter, EffectiveFilterContract};
pub use contract::{
    BoundFilterSet, FilterBindResult, FilterPlan, FilterPlanningContext,
    FilterPushdown, FilterPushdownPlanner, PlannedFilter,
};
pub use ir::{PredicateExpr, PredicateFragment, ScalarExpr};
pub(crate) use negotiate::{FilterNegotiator, ScanClauseSource};
pub use negotiation::{QueryPruningPlan, QueryPruningPlanner};
pub(crate) use normalize::RelationExpressionNormalizer;
pub use normalize::{NormalizedPredicate, QueryExpressionNormalizer};
pub use plan_set::FilterPlanSummary;
pub(crate) use plan_set::{
    FilterQualLocation, NegotiatedFilter, NegotiatedFilterSet, PathFilterSet,
    PlannedFilterRecord,
};
pub(crate) use runtime::{RelationFilterBinding, RelationFilterBindingError};
pub use scope::{QueryExpressionScope, SourceEntry};
pub use tree::{PredicatePlan, PredicatePlanner};
