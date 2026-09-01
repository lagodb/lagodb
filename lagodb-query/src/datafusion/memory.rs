//! Serial participant memory and spill policy.

use std::sync::Arc;

use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryPool, PeakRecordingPool,
};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};

use crate::ExecutionProfile;

use super::QueryExecutionError;

pub(super) struct RuntimeResources {
    pub(super) environment: Arc<RuntimeEnv>,
    pub(super) memory: Arc<PeakRecordingPool>,
}

/// Engine memory limit and source batch shape for one serial query participant.
#[derive(Debug, Clone, Copy)]
pub struct SerialExecutionLimits {
    engine_memory_bytes: usize,
    execution: ExecutionProfile,
}

impl SerialExecutionLimits {
    pub fn try_new(
        engine_memory_bytes: usize,
        execution: ExecutionProfile,
    ) -> Result<Self, QueryExecutionError> {
        if engine_memory_bytes == 0 {
            return Err(QueryExecutionError::InvalidLimits);
        }
        Ok(Self {
            engine_memory_bytes,
            execution,
        })
    }

    #[inline]
    pub(super) const fn maximum_batch_rows(self) -> usize {
        self.execution.maximum_batch_rows().get()
    }

    pub(super) fn runtime_env(self) -> Result<RuntimeResources, QueryExecutionError> {
        let disk = DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled);
        let memory = Arc::new(PeakRecordingPool::new(Arc::new(
            GreedyMemoryPool::new(self.engine_memory_bytes),
        )));
        let memory_pool = Arc::clone(&memory) as Arc<dyn MemoryPool>;
        let environment = RuntimeEnvBuilder::new()
            .with_memory_pool(memory_pool)
            .with_disk_manager_builder(disk)
            .build()?;
        Ok(RuntimeResources {
            environment: Arc::new(environment),
            memory,
        })
    }
}
