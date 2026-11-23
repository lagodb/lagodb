//! Build input held in memory while a hash join decides how to run.

use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
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
    /// A batch that does not fit stays uncharged: the caller stops here and
    /// hands everything to a path that reserves for itself.
    pub(super) fn push(&mut self, batch: RecordBatch) -> Result<bool> {
        let batch_bytes = self.counter.count_batch(&batch);
        let total_rows = self.rows + batch.num_rows();
        let total_bytes = self.batch_bytes + batch_bytes;
        let needed =
            Self::table_bytes(total_bytes, total_rows, self.batches.len() + 1)?;
        if Self::try_resize_within(&self.reservation, needed, self.limit_bytes)? {
            self.rows = total_rows;
            self.batch_bytes = total_bytes;
            self.batches.push((batch, batch_bytes));
            return Ok(true);
        }
        self.reservation.resize(self.batch_bytes);
        self.batches.push((batch, 0));
        Ok(false)
    }

    /// Peak bytes of a hash table over `count` batches holding `bytes` and
    /// `rows`: the batches, the single batch they are concatenated into when
    /// there are several, and the index sized by the formula `HashJoinExec`
    /// reserves with. The inputs' bytes bound the concatenated copy.
    pub(super) fn table_bytes(
        bytes: usize,
        rows: usize,
        count: usize,
    ) -> Result<usize> {
        let copy = if count > 1 { bytes } else { 0 };
        Ok(bytes + copy + Self::index_bytes(rows)?)
    }

    fn index_bytes(rows: usize) -> Result<usize> {
        estimate_memory_size::<(u64, u64)>(rows, 0)
    }

    /// Resize `reservation` to `needed` bytes if they stay within `limit` and
    /// the pool grants them; a refusal is an answer, not an error.
    pub(super) fn try_resize_within(
        reservation: &MemoryReservation,
        needed: usize,
        limit: usize,
    ) -> Result<bool> {
        if needed > limit {
            return Ok(false);
        }
        match reservation.try_resize(needed) {
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

    /// Concatenate the kept batches within the reservation `push` made for the
    /// copy, release the originals, and yield the single batch as `replay`.
    ///
    /// TODO(datafusion-hash-join-concat): DataFusion 55's `HashJoinExec`
    /// concatenates its build batches without reserving the copy
    /// (apache/datafusion#25496, fixed in 56.0.0). Arrow returns a lone array
    /// as a slice, so a single handed-over batch leaves the join nothing to
    /// copy. Once the join reserves the copy itself, replay the batches
    /// unchanged; `push` still counts the copy because the join cannot spill.
    /// Recheck on every DataFusion upgrade.
    pub(super) fn into_coalesced(
        mut self,
    ) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + 'static> {
        if self.batches.len() > 1 {
            let schema = self.batches[0].0.schema();
            let batch =
                concat_batches(&schema, self.batches.iter().map(|(batch, _)| batch))?;
            let bytes = RecordBatchMemoryCounter::new().count_batch(&batch);
            self.batches = vec![(batch, bytes)];
            self.batch_bytes = bytes;
            self.reservation
                .resize(bytes + Self::index_bytes(self.rows)?);
        }
        Ok(self.replay())
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
