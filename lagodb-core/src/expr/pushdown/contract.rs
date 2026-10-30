//! Typed provider contract for planning, persisting, and binding filters.

use std::error::Error;

use pgrx::pg_sys;

use crate::diag::SqlStateError;
use crate::expr::RuntimeValueBindings;
use crate::expr::contract::{PushdownContract, PushdownCosting};
use crate::plan_data::{PlanDataReader, PlanDataWriter};

use super::PredicateFragment;

/// Owned relation identity available when a provider planning session starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FilterPlanningContext {
    relation_oid: pg_sys::Oid,
    scan_relid: pg_sys::Index,
    effective_user_id: pg_sys::Oid,
}

impl FilterPlanningContext {
    pub const fn new(
        relation_oid: pg_sys::Oid,
        scan_relid: pg_sys::Index,
        effective_user_id: pg_sys::Oid,
    ) -> Self {
        Self {
            relation_oid,
            scan_relid,
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
    /// The provider safely widened a Boolean tree by omitting children. Core
    /// negotiation must decompose the source tree before accepting it so only
    /// the artifact's actual runtime-value dependencies survive.
    Partial(PlannedFilter<P>),
    Exact(PlannedFilter<P>),
    Conservative(PlannedFilter<P>),
}

impl<P> FilterPlan<P> {
    pub fn exact(predicate: P, costing: PushdownCosting) -> Self {
        Self::Exact(PlannedFilter::new(
            predicate,
            costing,
            FilterEvaluation::PerRow,
        ))
    }

    /// A conservative filter the provider evaluates on each row as
    /// `evaluation` declares.
    pub fn conservative(
        predicate: P,
        costing: PushdownCosting,
        evaluation: FilterEvaluation,
    ) -> Self {
        Self::Conservative(PlannedFilter::new(predicate, costing, evaluation))
    }

    pub fn partial(predicate: P, costing: PushdownCosting) -> Self {
        Self::Partial(PlannedFilter::new(
            predicate,
            costing,
            FilterEvaluation::PerRow,
        ))
    }

    /// The same plan over another provider artifact.
    pub fn map<Q>(self, map: impl FnOnce(P) -> Q) -> FilterPlan<Q> {
        match self {
            Self::Unsupported => FilterPlan::Unsupported,
            Self::Partial(planned) => FilterPlan::Partial(planned.map(map)),
            Self::Exact(planned) => FilterPlan::Exact(planned.map(map)),
            Self::Conservative(planned) => FilterPlan::Conservative(planned.map(map)),
        }
    }
}

/// How much of a planned filter the provider evaluates on each row it reads,
/// as PostgreSQL tells the quals an index handles from the qpquals a scan
/// evaluates. Planning-only: it decides which rows path costing expects the
/// provider to return and which filter cost it charges per read row.
///
/// Which parts of a pushed filter its reader evaluates is the provider's
/// decision, made from that reader's capabilities. Core names no predicate
/// kind here, so a provider whose reader gains one changes only its own
/// declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterEvaluation {
    PerRow,
    /// The provider prunes storage units with the whole filter but evaluates
    /// only part of it on each row, as an index executes the lossy quals
    /// `get_index_clause_from_support` derives from a clause (indxpath.c).
    /// Core derives that part as negotiation widens a filter: it keeps the
    /// sub-filters the provider plans as per-row filters, an AND those of its
    /// children, an OR all of its children or nothing, a NOT only itself.
    /// The provider's row filter keeps parts by the same rules. Only a
    /// conservative filter can be projected.
    Projected,
    /// The provider uses the filter only to prune storage units. An exact
    /// filter drops rows, so only a conservative one can be pruning-only.
    PruningOnly,
}

/// Provider-owned artifact plus its scan-volume costing contract.
pub struct PlannedFilter<P> {
    pub predicate: P,
    pub costing: PushdownCosting,
    /// Set only by the [`FilterPlan`] constructors, which keep exact filters
    /// per-row.
    pub(crate) evaluation: FilterEvaluation,
}

impl<P> PlannedFilter<P> {
    fn new(
        predicate: P,
        costing: PushdownCosting,
        evaluation: FilterEvaluation,
    ) -> Self {
        Self {
            predicate,
            costing,
            evaluation,
        }
    }

    fn map<Q>(self, map: impl FnOnce(P) -> Q) -> PlannedFilter<Q> {
        PlannedFilter {
            predicate: map(self.predicate),
            costing: self.costing,
            evaluation: self.evaluation,
        }
    }
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
        fragment: &PredicateFragment,
    ) -> Result<FilterPlan<Self::PlannedPredicate>, Self::Error>;

    /// Storage units, at least one, that statistics pruning keeps or skips
    /// whole, as BRIN does block ranges. `None` prices statistics pruning
    /// as row-granular. CustomScan and query-offload planning read it; FDW
    /// path costing does not, so a foreign-table planner leaves it `None`.
    fn pruning_units(&self) -> Option<f64> {
        None
    }
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
        values: RuntimeValueBindings<'_>,
    ) -> Result<FilterBindResult<Self::BoundPredicate>, Self::Error>;
}

/// Effective core-owned contract after direct planning or widening.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EffectiveFilterContract {
    pub contract: PushdownContract,
    pub costing: PushdownCosting,
    pub row_filter: RowFilter,
}

/// What of a pushed filter the provider evaluates on each row it reads,
/// resolved from its [`FilterEvaluation`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowFilter {
    Whole,
    /// A part core derived from the pushed expression, sharing its nodes.
    Part(*mut pg_sys::Expr),
    /// Nothing: the provider only prunes storage units with the filter.
    Nothing,
}

impl RowFilter {
    /// The expression evaluated on each row of a filter pushed as
    /// `pushed_expr`.
    #[inline]
    pub(crate) fn expr(
        self,
        pushed_expr: *mut pg_sys::Expr,
    ) -> Option<*mut pg_sys::Expr> {
        match self {
            Self::Whole => Some(pushed_expr),
            Self::Part(expr) => Some(expr),
            Self::Nothing => None,
        }
    }
}
