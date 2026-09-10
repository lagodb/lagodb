//! PostgreSQL-aligned LIMIT/OFFSET run-cost adjustment.

use super::super::LimitNode;
use super::{CostingContext, PlanCost, PlanEstimate, QueryCostError};

pub(super) struct LimitCostStrategy<'limit> {
    context: CostingContext,
    limit: &'limit LimitNode,
    input: PlanEstimate,
}

impl<'limit> LimitCostStrategy<'limit> {
    pub(super) const fn new(
        context: CostingContext,
        limit: &'limit LimitNode,
        input: PlanEstimate,
    ) -> Self {
        Self {
            context,
            limit,
            input,
        }
    }

    pub(super) fn estimate(self) -> Result<PlanEstimate, QueryCostError> {
        let rows = self.limit.estimated_rows();
        let input_run_cost = self.input.cost.total - self.input.cost.startup;
        let input_rows = self.input.rows;
        let estimate = self.limit.estimate();
        let mut remaining_rows = input_rows;
        let mut startup = self.input.cost.startup;
        if estimate.offset_rows() != 0.0 {
            let offset_rows = estimate.offset_rows().min(remaining_rows);
            if input_rows > 0.0 {
                startup += input_run_cost * offset_rows / input_rows;
            }
            remaining_rows = (remaining_rows - offset_rows).max(1.0);
        }
        let total = match estimate.count_rows() {
            Some(count_rows) => {
                let count_rows = count_rows.min(remaining_rows);
                if input_rows > 0.0 {
                    startup + input_run_cost * count_rows / input_rows
                } else {
                    startup
                }
            }
            None => self.input.cost.total,
        };
        PlanEstimate::try_new(
            rows,
            self.context.batches(rows),
            PlanCost::try_new(startup, total)?,
        )
    }
}
