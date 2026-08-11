//! Planned filter pushdown: stable IR, negotiation, persistence, and binding.

mod bindings;
mod codec;
mod contract;
mod ir;
mod negotiate;
mod normalize;
mod plan_set;
mod runtime;

pub(crate) use bindings::FilterBindingExpr;
pub use bindings::{FilterValue, FilterValueBindings};
pub(crate) use codec::{EncodedFilterData, FilterDataCodec, FilterDataError};
pub(crate) use contract::{BoundFilter, EffectiveFilterContract};
pub use contract::{
    BoundFilterSet, FilterBindResult, FilterPlan, FilterPlanningContext,
    FilterPushdown, FilterPushdownPlanner, PlannedFilter,
};
pub use ir::{
    FilterColumn, FilterFragment, FilterNode, FilterScalar, FilterTypeMetadata,
    FilterValueSlot, FilterValueSlotId, FilterValueSourceKind,
};
pub(crate) use negotiate::{FilterNegotiator, ScanClauseSource};
pub(crate) use normalize::{FilterNormalizer, NormalizedFilter};
pub use plan_set::FilterPlanSummary;
pub(crate) use plan_set::{
    FilterQualLocation, NegotiatedFilter, NegotiatedFilterSet, PathFilterSet,
    PlannedFilterRecord,
};
pub(crate) use runtime::{RuntimeFilterError, RuntimeFilterState};
