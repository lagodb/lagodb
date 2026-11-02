//! Build input held in memory while a hash join decides how to run.

use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::utils::memory::{
    RecordBatchMemoryCounter, estimate_memory_size,
};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    Partitioning, PlanProperties, ReplaceChildrenOptions,
};
use futures::{Stream, StreamExt, stream};

/// Build batches read so far, reserved as the hash table they would become.
pub(super) struct StagedBuild {
    /// Each batch with the bytes it holds in `reservation`.
    batches: Vec<(RecordBatch, usize)>,
    rows: usize,
    batch_bytes: usize,
    counter: RecordBatchMemoryCounter,
    reservation: MemoryReservation,
    limit_bytes: usize,
}

impl StagedBuild {
    pub(super) fn new(reservation: MemoryReservation, limit_bytes: usize) -> Self {
        Self {
            batches: Vec::new(),
            rows: 0,
            batch_bytes: 0,
            counter: RecordBatchMemoryCounter::new(),
            reservation,
            limit_bytes,
        }
    }

    /// Keep `batch`, returning whether a hash table can still hold the input.
    ///
    /// The table is sized by the same formula `HashJoinExec` reserves with. A
    /// batch that does not fit stays uncharged: the caller stops here and hands
    /// everything to a sort, which reserves for itself.
    pub(super) fn push(&mut self, batch: RecordBatch) -> Result<bool> {
        let batch_bytes = self.counter.count_batch(&batch);
        let total_rows = self.rows + batch.num_rows();
        let total_bytes = self.batch_bytes + batch_bytes;
        let needed = total_bytes + estimate_memory_size::<(u64, u64)>(total_rows, 0)?;
        if self.reserve(needed)? {
            self.rows = total_rows;
            self.batch_bytes = total_bytes;
            self.batches.push((batch, batch_bytes));
            return Ok(true);
        }
        self.reservation.resize(self.batch_bytes);
        self.batches.push((batch, 0));
        Ok(false)
    }

    fn reserve(&self, needed: usize) -> Result<bool> {
        if needed > self.limit_bytes {
            return Ok(false);
        }
        match self.reservation.try_resize(needed) {
            Ok(()) => Ok(true),
            Err(error)
                if matches!(
                    error.find_root(),
                    DataFusionError::ResourcesExhausted(_)
                ) =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    /// Yield the kept batches, returning each one's bytes as its consumer
    /// takes over, so a batch is never charged twice.
    pub(super) fn replay(
        self,
    ) -> impl Stream<Item = Result<RecordBatch>> + Send + 'static {
        let Self {
            batches,
            reservation,
            ..
        } = self;
        stream::iter(batches).map(move |(batch, bytes)| {
            reservation.shrink(bytes);
            Ok(batch)
        })
    }
}

/// Plan node that yields an already opened stream once.
///
/// `AdaptiveHashJoinExec` opens its inputs before it builds the plan that
/// continues the join, and a network input can be opened only once. This node
/// exists only in that run-time plan, which no optimizer rewrites.
pub(super) struct StagedInputExec {
    stream: Mutex<Option<SendableRecordBatchStream>>,
    properties: Arc<PlanProperties>,
    /// The input `stream` was opened from. It is never executed here.
    source: Option<Arc<dyn ExecutionPlan>>,
}

impl StagedInputExec {
    /// Stand in for `source` under the join that already validated it. It stays
    /// the child because `HashJoinExec` looks through its probe input for the
    /// scan consuming its dynamic filter.
    pub(super) fn opened(
        source: Arc<dyn ExecutionPlan>,
        stream: SendableRecordBatchStream,
    ) -> Self {
        Self {
            stream: Mutex::new(Some(stream)),
            properties: Arc::clone(source.properties()),
            source: Some(source),
        }
    }

    /// One unordered partition, for a plan private to a single join partition.
    /// A leaf, so walking that plan stops at the operators it added.
    pub(super) fn single_partition(stream: SendableRecordBatchStream) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(stream.schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            stream: Mutex::new(Some(stream)),
            properties: Arc::new(properties),
            source: None,
        }
    }
}

impl Debug for StagedInputExec {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("StagedInputExec")
    }
}

impl DisplayAs for StagedInputExec {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        formatter: &mut Formatter<'_>,
    ) -> fmt::Result {
        formatter.write_str("StagedInputExec")
    }
}

impl ExecutionPlan for StagedInputExec {
    fn name(&self) -> &str {
        "StagedInputExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.source.iter().collect()
    }

    fn apply_expressions(
        &self,
        _visitor: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(
            "staged join input cannot be rewritten".to_owned(),
        ))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.stream
            .lock()
            .expect("no code panics while holding the staged stream")
            .take()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "staged join input was executed twice".to_owned(),
                )
            })
    }
}
