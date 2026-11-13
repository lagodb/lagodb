//! Producer width derived from the finished distributed plan, before launch.

use std::num::NonZeroU32;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::NetworkBoundaryExt;

/// The producer width the plan implies.
///
/// Upstream routes tasks across the URLs it is given, so the leader only needs
/// this width from the plan: to cap the launch and to size the DSM rings.
pub(in crate::datafusion) struct ParallelStageCatalog {
    maximum_producer_tasks: u32,
}

impl ParallelStageCatalog {
    pub(in crate::datafusion) fn discover(
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let mut maximum_producer_tasks = 0_u32;
        Self::visit(plan, &mut maximum_producer_tasks)?;
        Ok(Self {
            maximum_producer_tasks,
        })
    }

    fn visit(plan: &Arc<dyn ExecutionPlan>, maximum: &mut u32) -> Result<()> {
        if let Some(boundary) = plan.as_network_boundary() {
            let stage = boundary.input_stage();
            if let Some(producer) = stage.local_plan() {
                *maximum = (*maximum).max(stage.task_count() as u32);
                // The boundary's children already contain its local producer
                // plan, so returning here avoids counting it twice.
                Self::visit(producer, maximum)?;
            }
            return Ok(());
        }
        for child in plan.children() {
            Self::visit(child, maximum)?;
        }
        Ok(())
    }

    /// Launch only when the plan and the PG cap permit at least two producers.
    pub(in crate::datafusion) fn launch_width(
        &self,
        worker_cap: u32,
    ) -> Option<NonZeroU32> {
        let width = self.maximum_producer_tasks.min(worker_cap);
        (width >= 2).then(|| NonZeroU32::new(width).expect("width is at least two"))
    }
}
