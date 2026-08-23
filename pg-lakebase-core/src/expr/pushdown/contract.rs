//! Typed provider contract for planning, persisting, and binding filters.

use std::error::Error;

use pgrx::pg_sys;

use crate::diag::SqlStateError;
use crate::expr::contract::{PushdownContract, PushdownCosting};
use crate::plan_data::{PlanDataReader, PlanDataWriter};

use super::{FilterFragment, FilterValueBindings};

/// Owned relation identity available when a provider planning session starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FilterPlanningContext {
    relation_oid: pg_sys::Oid,
    scan_relid: pg_sys::Index,
    tablespace_oid: pg_sys::Oid,
    effective_user_id: pg_sys::Oid,
}

impl FilterPlanningContext {
    pub(crate) const fn new(
        relation_oid: pg_sys::Oid,
        scan_relid: pg_sys::Index,
        tablespace_oid: pg_sys::Oid,
        effective_user_id: pg_sys::Oid,
    ) -> Self {
        Self {
            relation_oid,
            scan_relid,
            tablespace_oid,
            effective_user_id,
        }
    }

    #[inline]
    pub const fn relation_oid(&self) -> pg_sys::Oid {
        self.relation_oid
    }

    #[inline]
    pub const fn scan_relid(&self) -> pg_sys::Index {
        self.scan_relid
    }

    #[inline]
    pub const fn tablespace_oid(&self) -> pg_sys::Oid {
        self.tablespace_oid
    }

    /// Role selected by PostgreSQL for relation access (including view owner
    /// semantics), or the session user when no override is present.
    #[inline]
    pub const fn effective_user_id(&self) -> pg_sys::Oid {
        self.effective_user_id
    }
}

/// Provider result for one complete fragment.
pub enum FilterPlan<P> {
    Unsupported,
    Exact(PlannedFilter<P>),
    Conservative(PlannedFilter<P>),
}

impl<P> FilterPlan<P> {
    pub fn exact(predicate: P, costing: PushdownCosting) -> Self {
        Self::Exact(PlannedFilter { predicate, costing })
    }

    pub fn conservative(predicate: P, costing: PushdownCosting) -> Self {
        Self::Conservative(PlannedFilter { predicate, costing })
    }
}

/// Provider-owned artifact plus its scan-volume costing contract.
pub struct PlannedFilter<P> {
    pub predicate: P,
    pub costing: PushdownCosting,
}

/// Runtime bind result. Structural support is already closed at planning.
pub enum FilterBindResult<B> {
    Bound(B),
    ValueNotRepresentable,
}

/// One complete set of provider predicates bound for the current parameter
/// values. The framework rebuilds this set atomically on a relevant rescan.
pub struct BoundFilterSet<'a, B> {
    entries: &'a [Option<BoundFilter<B>>],
}

impl<'a, B> BoundFilterSet<'a, B> {
    pub(crate) fn new(entries: &'a [Option<BoundFilter<B>>]) -> Self {
        Self { entries }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.iter().all(Option::is_none)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.iter().filter(|entry| entry.is_some()).count()
    }

    pub fn iter(&self) -> impl Iterator<Item = &B> {
        self.entries
            .iter()
            .filter_map(Option::as_ref)
            .map(|entry| &entry.predicate)
    }

    pub fn rescan_stable(&self) -> impl Iterator<Item = &B> {
        self.entries
            .iter()
            .filter_map(Option::as_ref)
            .filter(|entry| entry.rescan_stable)
            .map(|entry| &entry.predicate)
    }

    pub fn static_values(&self) -> impl Iterator<Item = &B> {
        self.entries
            .iter()
            .filter_map(Option::as_ref)
            .filter(|entry| entry.static_values)
            .map(|entry| &entry.predicate)
    }
}

pub(crate) struct BoundFilter<B> {
    pub predicate: B,
    pub rescan_stable: bool,
    pub static_values: bool,
}

/// Relation-scoped provider planner.
pub trait FilterPushdownPlanner {
    type PlannedPredicate;
    type Error: SqlStateError + Error + Send + Sync + 'static;

    fn try_plan_filter(
        &mut self,
        fragment: &FilterFragment,
    ) -> Result<FilterPlan<Self::PlannedPredicate>, Self::Error>;
}

/// Provider filter facet shared by CustomScan and FDW adapters.
pub trait FilterPushdown: 'static {
    type Planner: FilterPushdownPlanner<
            PlannedPredicate = Self::PlannedPredicate,
            Error = Self::Error,
        > + 'static;
    type PlannedPredicate: 'static;
    type BoundPredicate: 'static;
    type Error: SqlStateError + Error + Send + Sync + 'static;

    fn begin_filter_planning(
        context: &FilterPlanningContext,
    ) -> Result<Self::Planner, Self::Error>;

    fn encode_planned(
        predicate: &Self::PlannedPredicate,
        writer: &mut PlanDataWriter,
    ) -> Result<(), Self::Error>;

    fn decode_planned(
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
    ) -> Result<Self::PlannedPredicate, Self::Error>;

    fn bind_filter(
        predicate: &Self::PlannedPredicate,
        values: FilterValueBindings<'_>,
    ) -> Result<FilterBindResult<Self::BoundPredicate>, Self::Error>;
}

/// Effective core-owned contract after direct planning or widening.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EffectiveFilterContract {
    pub contract: PushdownContract,
    pub costing: PushdownCosting,
}
