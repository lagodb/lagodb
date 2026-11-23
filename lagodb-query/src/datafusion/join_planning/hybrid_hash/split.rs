//! The probe pass of a level whose first group joins in memory.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};

use arrow_array::{BooleanArray, RecordBatch};
use arrow_buffer::BooleanBuffer;
use arrow_schema::SchemaRef;
use datafusion::arrow::compute::{BatchCoalescer, filter_record_batch};
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{Stream, StreamExt};

use super::partitioner::BucketPartitioner;
use super::probe_input::ProbeInput;

/// Unit of the group whose table is in memory: the level's first.
const MEMORY_UNIT: u16 = 0;

/// Splits a level's probe as PostgreSQL's hash join treats an outer tuple of
/// a later batch (`ExecHashJoinImpl`, `ExecHashJoinSaveTuple`): rows of the
/// memory group go to its join as the join reads them, the others to the
/// probe partitioner.
///
/// The join may stop reading early, as `HashJoinExec` does over an empty
/// build, so `finish` drains what it left: every row of a later group still
/// reaches the partitioner, and the memory group's rest no longer matters.
pub(super) struct ProbeSplit {
    schema: SchemaRef,
    state: Arc<Mutex<Option<SplitState>>>,
}

struct SplitState {
    input: SendableRecordBatchStream,
    partitioner: BucketPartitioner,
    /// Memory group rows not yet handed to its join; `None` once it ended.
    memory: Option<BatchCoalescer>,
    /// `input` has ended.
    ended: bool,
}

impl ProbeSplit {
    pub(super) fn new(
        input: SendableRecordBatchStream,
        partitioner: BucketPartitioner,
        batch_size: usize,
    ) -> Self {
        let schema = input.schema();
        let memory = BatchCoalescer::new(Arc::clone(&schema), batch_size)
            .with_biggest_coalesce_batch_size(Some(batch_size / 2));
        let state = SplitState {
            input,
            partitioner,
            memory: Some(memory),
            ended: false,
        };
        Self {
            schema,
            state: Arc::new(Mutex::new(Some(state))),
        }
    }

    /// The memory group's probe rows, coalesced as a group's probe read from
    /// disk is.
    pub(super) fn memory_input(&self) -> SendableRecordBatchStream {
        let rows = MemoryRows(Arc::clone(&self.state));
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            rows,
        ))
    }

    /// Split the rows the memory group's join left unread and close the
    /// partitioner. Called once that join has ended.
    pub(super) async fn finish(self) -> Result<ProbeInput> {
        let mut state = self
            .state
            .lock()
            .expect("no code panics while holding the probe split")
            .take()
            .expect("finish consumes the split, the only taker of its state");
        state.memory = None;
        if !state.ended {
            while let Some(batch) = state.input.next().await.transpose()? {
                state.route(batch)?;
            }
        }
        state.partitioner.finish_probe()
    }
}

impl SplitState {
    fn poll_memory(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<RecordBatch>>> {
        loop {
            let Some(memory) = self.memory.as_mut() else {
                return Poll::Ready(Ok(None));
            };
            if let Some(batch) = memory.next_completed_batch() {
                return Poll::Ready(Ok(Some(batch)));
            }
            if self.ended {
                return Poll::Ready(Ok(None));
            }
            match ready!(self.input.poll_next_unpin(cx)).transpose()? {
                Some(batch) => self.route(batch)?,
                None => {
                    self.ended = true;
                    memory.finish_buffered_batch()?;
                }
            }
        }
    }

    /// File the rows of `batch` by group.
    fn route(&mut self, batch: RecordBatch) -> Result<()> {
        let units = self.partitioner.route(&batch)?;
        let memory =
            BooleanBuffer::collect_bool(units.len(), |row| units[row] == MEMORY_UNIT);
        let memory_rows = memory.count_set_bits();
        if memory_rows == 0 {
            return self.partitioner.push(batch, units);
        }
        if memory_rows < units.len() {
            let rest =
                filter_record_batch(&batch, &BooleanArray::new(!&memory, None))?;
            let units = units
                .into_iter()
                .filter(|&unit| unit != MEMORY_UNIT)
                .collect();
            self.partitioner.push(rest, units)?;
        }
        if let Some(coalescer) = self.memory.as_mut() {
            coalescer
                .push_batch_with_filter(batch, &BooleanArray::new(memory, None))?;
        }
        Ok(())
    }
}

/// The memory group's probe input, read through the shared split.
struct MemoryRows(Arc<Mutex<Option<SplitState>>>);

impl Stream for MemoryRows {
    type Item = Result<RecordBatch>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut state = self
            .0
            .lock()
            .expect("no code panics while holding the probe split");
        match state.as_mut() {
            Some(state) => state.poll_memory(cx).map(Result::transpose),
            None => Poll::Ready(None),
        }
    }
}
