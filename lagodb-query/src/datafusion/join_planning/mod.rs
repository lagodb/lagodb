//! PostgreSQL planner knowledge applied to one DataFusion planning session.

mod adaptive;
mod estimates;
mod hybrid_hash;
mod null_sensitive;
mod sort_merge;
mod staged_build;
mod statistics;
mod strategy;

pub(in crate::datafusion) use self::adaptive::AdaptiveHashJoinExec;
pub(in crate::datafusion) use self::null_sensitive::NullSensitiveAntiJoinExec;

use std::sync::Arc;

use datafusion::execution::context::QueryPlanner;
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::operator_statistics::StatisticsRegistry;

use self::estimates::PlanEstimates;
use self::null_sensitive::{
    LagoQueryPlanner, NullSensitiveBuildSideRule, NullSensitiveProbeRule,
};
use self::statistics::PlanStatisticsProvider;
use self::strategy::JoinStrategyRule;
use super::memory::QueryExecutionLimits;
use crate::HashJoinFallback;
use crate::plan::QueryFragment;

/// Budgets and overflow policy shared by every adaptive join of one execution.
#[derive(Debug, Clone, Copy)]
pub(in crate::datafusion) struct JoinMemoryPolicy {
    /// `hash_mem` of the whole task, not of one partition.
    pub(in crate::datafusion) hash_memory_bytes: usize,
    /// Bound of each input's in-memory bucket run.
    pub(in crate::datafusion) work_mem_bytes: usize,
    pub(in crate::datafusion) fallback: HashJoinFallback,
}

/// Query-specific session rules shared by serial and parallel planning.
pub(in crate::datafusion) struct QuerySessionRules<'a> {
    fragment: &'a QueryFragment,
    limits: QueryExecutionLimits,
}

impl<'a> QuerySessionRules<'a> {
    pub(in crate::datafusion) const fn new(
        fragment: &'a QueryFragment,
        limits: QueryExecutionLimits,
    ) -> Self {
        Self { fragment, limits }
    }

    pub(in crate::datafusion) fn apply(self, state: SessionState) -> SessionState {
        let estimates = PlanEstimates::for_joins(self.fragment).map(Arc::new);
        let has_having = self.fragment.has_having_filter();
        if estimates.is_none() && !has_having {
            return state;
        }
        // `push_down_filter` can move HAVING below Aggregate. Keep that
        // semantic boundary in LagoDB's validated IR and leave all other
        // DataFusion rewrites enabled.
        let optimizer_rules = has_having.then(|| {
            state
                .optimizers()
                .iter()
                .filter(|rule| rule.name() != "push_down_filter")
                .cloned()
                .collect()
        });
        let physical_rules = estimates.is_some().then(|| self.physical_rules(&state));

        let mut builder = SessionStateBuilder::new_from_existing(state);
        if let Some(rules) = optimizer_rules {
            builder = builder.with_optimizer_rules(rules);
        }
        if let Some(rules) = physical_rules {
            builder = builder.with_physical_optimizer_rules(rules);
        }
        if let Some(estimates) = estimates {
            let mut registry = StatisticsRegistry::default_with_builtin_providers();
            registry.register(Arc::new(PlanStatisticsProvider::new(estimates)));
            builder = builder.with_statistics_registry(registry);
        }
        builder.build()
    }

    /// The physical planner of every LagoDB session. A session installs it
    /// before the distributed planner, which wraps the planner it finds.
    pub(in crate::datafusion) fn query_planner() -> Arc<dyn QueryPlanner + Send + Sync>
    {
        Arc::new(LagoQueryPlanner)
    }

    /// The build-side and strategy rules need JoinSelection's build sides,
    /// and must run before EnsureRequirements adds the distribution each join
    /// mode requires. A NULL-sensitive anti join exists only in a fragment
    /// that joins, and its build side is restored first, so the strategy rule
    /// sees the final one. The probe rule removes a repartition that
    /// EnsureRequirements adds, so it runs right after it.
    fn physical_rules(
        &self,
        state: &SessionState,
    ) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
        let mut rules = state.physical_optimizers().to_vec();
        let join_selection = rules
            .iter()
            .position(|rule| rule.name() == "join_selection")
            .expect("the default physical optimizer includes JoinSelection");
        rules.insert(join_selection + 1, Arc::new(NullSensitiveBuildSideRule));
        if self.limits.spill_enabled() {
            rules.insert(
                join_selection + 2,
                Arc::new(JoinStrategyRule::new(self.limits.join_policy())),
            );
        }
        let ensure_requirements = rules
            .iter()
            .position(|rule| rule.name() == "EnsureRequirements")
            .expect("the default physical optimizer includes EnsureRequirements");
        rules.insert(ensure_requirements + 1, Arc::new(NullSensitiveProbeRule));
        rules
    }
}
