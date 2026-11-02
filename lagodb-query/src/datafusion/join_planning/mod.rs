//! PostgreSQL planner knowledge applied to one DataFusion planning session.

mod adaptive;
mod estimates;
mod sort_merge;
mod staged_build;
mod statistics;
mod strategy;

pub(in crate::datafusion) use self::adaptive::AdaptiveHashJoinExec;

use std::sync::Arc;

use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::operator_statistics::StatisticsRegistry;

use self::estimates::PlanEstimates;
use self::statistics::PlanStatisticsProvider;
use self::strategy::JoinStrategyRule;
use super::memory::QueryExecutionLimits;
use crate::plan::QueryFragment;

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
        let physical_rules = (estimates.is_some() && self.limits.spill_enabled())
            .then(|| self.physical_rules(&state));

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

    /// The strategy rule needs JoinSelection's final build side, and must run
    /// before EnsureRequirements adds the distribution each join mode requires.
    fn physical_rules(
        &self,
        state: &SessionState,
    ) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
        let mut rules = state.physical_optimizers().to_vec();
        let join_selection = rules
            .iter()
            .position(|rule| rule.name() == "join_selection")
            .expect("the default physical optimizer includes JoinSelection");
        rules.insert(
            join_selection + 1,
            Arc::new(JoinStrategyRule::new(self.limits.hash_memory_bytes())),
        );
        rules
    }
}
