//! Producer inventory derived from the finished distributed plan, before launch.

use std::num::NonZeroU32;
use std::sync::Arc;

use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_distributed::shm::proc_for_task;
use datafusion_distributed::{NetworkBoundaryExt, NetworkCoalesceExec};
use prost::{Enumeration, Message};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
#[repr(i32)]
pub(super) enum ParallelStageRouting {
    /// The outermost network boundary always returns to the leader process.
    Leader = 0,
    /// A nested coalesce boundary returns to logical consumer task zero.
    NestedCoalesce = 1,
    /// A nested shuffle or broadcast boundary follows its partition routing.
    NestedPartitioned = 2,
}

/// Logical task addresses are independent of the number of attached PG workers.
#[derive(Clone, PartialEq, Message)]
pub(super) struct ParallelStage {
    #[prost(uint32, tag = "1")]
    pub stage_id: u32,
    #[prost(uint32, tag = "2")]
    pub task_count: u32,
    #[prost(enumeration = "ParallelStageRouting", tag = "3")]
    pub routing: i32,
    #[prost(uint32, repeated, tag = "4")]
    pub consumer_tasks: Vec<u32>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ParallelTaskAssignment {
    pub stage_id: u32,
    pub task_id: u32,
    pub task_count: u32,
}

/// The pull-based fork requests partition ranges and producer heads at runtime.
/// This inventory stores logical consumer tasks. Process destinations and
/// partition sinks are chosen only after an ExecuteTask request arrives.
pub(in crate::datafusion) struct ParallelStageCatalog {
    stages: Box<[ParallelStage]>,
}

impl ParallelStageCatalog {
    pub(in crate::datafusion) fn discover(
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let mut stages = Vec::new();
        Self::visit(plan, false, &mut stages)?;
        Ok(Self {
            stages: stages.into_boxed_slice(),
        })
    }

    fn visit(
        plan: &Arc<dyn ExecutionPlan>,
        nested: bool,
        stages: &mut Vec<ParallelStage>,
    ) -> Result<()> {
        if let Some(boundary) = plan.as_network_boundary() {
            let stage = boundary.input_stage();
            if let Some(producer) = stage.local_plan() {
                let stage_id = u32::try_from(stage.num()).map_err(|_| {
                    DataFusionError::Plan(format!(
                        "parallel stage id {} exceeds the transport address space",
                        stage.num(),
                    ))
                })?;
                let task_count = u32::try_from(stage.task_count()).map_err(|_| {
                    DataFusionError::Plan(format!(
                        "parallel stage {stage_id} task count exceeds the transport address space",
                    ))
                })?;
                let routing = if !nested {
                    ParallelStageRouting::Leader
                } else if plan.is::<NetworkCoalesceExec>() {
                    ParallelStageRouting::NestedCoalesce
                } else {
                    ParallelStageRouting::NestedPartitioned
                };
                let consumer_tasks = if routing
                    == ParallelStageRouting::NestedPartitioned
                {
                    (0..producer.output_partitioning().partition_count())
                        .map(|partition| {
                            let task = boundary.route_partition(partition)?.consumer_task;
                            u32::try_from(task).map_err(|_| DataFusionError::Plan(
                                "parallel consumer task exceeds the transport address space".to_owned(),
                            ))
                        })
                        .collect::<Result<Vec<_>>>()?
                } else {
                    Vec::new()
                };
                stages.push(ParallelStage {
                    stage_id,
                    task_count,
                    routing: routing as i32,
                    consumer_tasks,
                });
                // A network boundary's children already contain its local
                // producer plan. Returning here prevents counting it twice.
                Self::visit(producer, true, stages)?;
            }
            return Ok(());
        }
        for child in plan.children() {
            Self::visit(child, nested, stages)?;
        }
        Ok(())
    }

    pub(super) fn stages(&self) -> &[ParallelStage] {
        &self.stages
    }

    pub(super) fn from_stages(stages: Vec<ParallelStage>) -> Self {
        Self {
            stages: stages.into_boxed_slice(),
        }
    }

    pub(super) fn maximum_producer_tasks(&self) -> u32 {
        self.stages
            .iter()
            .map(|stage| stage.task_count)
            .max()
            .unwrap_or(0)
    }

    /// Launch only when both the finished plan and the PG cap permit at least
    /// two producers. A single producer is not the parallel execution mode.
    pub(in crate::datafusion) fn launch_width(
        &self,
        worker_cap: u32,
    ) -> Option<NonZeroU32> {
        let width = self.maximum_producer_tasks().min(worker_cap);
        (width >= 2).then(|| NonZeroU32::new(width).expect("width is at least two"))
    }

    /// Short launch changes process ownership, not stage task count or plan
    /// specialization. Every original task remains addressed exactly once.
    pub(super) fn assignments(
        &self,
        worker_proc: u32,
        attached_workers: NonZeroU32,
    ) -> impl Iterator<Item = ParallelTaskAssignment> + '_ {
        self.stages.iter().flat_map(move |stage| {
            (0..stage.task_count)
                .filter(move |task| {
                    proc_for_task(attached_workers.get(), *task) == worker_proc
                })
                .map(move |task_id| ParallelTaskAssignment {
                    stage_id: stage.stage_id,
                    task_id,
                    task_count: stage.task_count,
                })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::{ParallelStage, ParallelStageCatalog};

    #[test]
    fn assigns_every_stage_task_exactly_once() {
        let catalog = ParallelStageCatalog::from_stages(vec![ParallelStage {
            stage_id: 7,
            task_count: 5,
            routing: super::ParallelStageRouting::NestedPartitioned as i32,
            consumer_tasks: vec![0, 1],
        }]);
        let workers = NonZeroU32::new(2).expect("two is non-zero");
        let mut assigned = (1..=workers.get())
            .flat_map(|process| catalog.assignments(process, workers))
            .map(|assignment| (assignment.stage_id, assignment.task_id))
            .collect::<Vec<_>>();
        assigned.sort_unstable();

        assert_eq!(assigned, vec![(7, 0), (7, 1), (7, 2), (7, 3), (7, 4)],);
    }
}
