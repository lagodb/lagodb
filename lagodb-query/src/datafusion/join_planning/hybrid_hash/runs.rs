//! A partitioned join input, read back one unit range at a time.

use std::ops::Range;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_buffer::alloc::ALIGNMENT;
use arrow_schema::SchemaRef;
use datafusion::arrow::compute::BatchCoalescer;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{StreamExt, TryStreamExt, stream};

use crate::datafusion::join_planning::staged_build::StagedBuild;
use crate::datafusion::spill::{BucketChunks, BucketRun, BucketRunFormat};

/// Rows of each unit of one input, and the IPC segment bytes a reader of the
/// unit loads.
pub(super) struct BucketSizes {
    rows: Vec<usize>,
    bytes: Vec<usize>,
}

impl BucketSizes {
    pub(super) fn new(units: usize) -> Self {
        Self {
            rows: vec![0; units],
            bytes: vec![0; units],
        }
    }

    /// `rows` of `unit` were written to a run as `bytes`.
    pub(super) fn add(&mut self, unit: usize, rows: usize, bytes: usize) {
        self.rows[unit] += rows;
        self.bytes[unit] += bytes;
    }

    fn rows(&self, units: Range<usize>) -> usize {
        self.rows[units].iter().sum()
    }
}

/// Consecutive buckets whose build rows join under one hash table.
pub(super) struct BucketGroup {
    pub(super) buckets: Range<usize>,
    /// Whether a table over the group's build stays within the partition's
    /// share. Only a group of one bucket can exceed it.
    pub(super) fits: bool,
}

/// One join input split into runs on disk, stored by unit: a build by bucket,
/// a probe by the build's groups. Dropping it removes the run files.
pub(super) struct BucketedInput {
    format: Arc<BucketRunFormat>,
    runs: Vec<BucketRun>,
    sizes: BucketSizes,
}

impl BucketedInput {
    pub(super) const fn new(
        format: Arc<BucketRunFormat>,
        runs: Vec<BucketRun>,
        sizes: BucketSizes,
    ) -> Self {
        Self {
            format,
            runs,
            sizes,
        }
    }

    pub(super) fn rows(&self, units: Range<usize>) -> usize {
        self.sizes.rows(units)
    }

    /// Consecutive bucket ranges covering every bucket of a build, each as
    /// large as a hash table within `limit_bytes` can hold by the estimate
    /// `StagedBuild` checks. A bucket over the limit on its own forms its own
    /// group, which does not fit.
    ///
    /// Every run's read buffer for a range is rounded up to the allocation
    /// alignment, so each range starts with that allowance.
    pub(super) fn groups(&self, limit_bytes: usize) -> Result<Vec<BucketGroup>> {
        let rounding = ALIGNMENT * self.runs.len();
        let buckets = self.sizes.rows.len();
        let mut groups = Vec::new();
        let (mut start, mut rows, mut bytes) = (0, 0, rounding);
        for bucket in 0..buckets {
            let grown_rows = rows + self.sizes.rows[bucket];
            let grown_bytes = bytes + self.sizes.bytes[bucket];
            if bucket > start
                && StagedBuild::table_bytes(grown_bytes, grown_rows, 2)? > limit_bytes
            {
                groups.push(BucketGroup {
                    buckets: start..bucket,
                    fits: StagedBuild::table_bytes(bytes, rows, 2)? <= limit_bytes,
                });
                start = bucket;
                rows = self.sizes.rows[bucket];
                bytes = rounding + self.sizes.bytes[bucket];
            } else {
                rows = grown_rows;
                bytes = grown_bytes;
            }
        }
        groups.push(BucketGroup {
            buckets: start..buckets,
            fits: StagedBuild::table_bytes(bytes, rows, 2)? <= limit_bytes,
        });
        Ok(groups)
    }

    /// The rows of `units` as a group's build, which the group keeps whole:
    /// each run's range is read into one buffer of exactly the bytes `groups`
    /// sized the range by.
    pub(super) fn read(
        self: &Arc<Self>,
        units: Range<usize>,
    ) -> SendableRecordBatchStream {
        let input = Arc::clone(self);
        let schema = Arc::clone(self.format.schema());
        let batches = stream::iter(0..self.runs.len())
            .map(move |run| {
                let mut batches = Vec::new();
                input.runs[run].read(&input.format, units.clone(), &mut batches)?;
                Ok::<_, DataFusionError>(batches)
            })
            .map_ok(|batches| stream::iter(batches.into_iter().map(Ok)))
            .try_flatten();
        Box::pin(RecordBatchStreamAdapter::new(schema, batches))
    }

    /// The rows of `units` decoded a chunk at a time, as a group's probe or as
    /// the input of a further split, in batches of `batch_size` rows.
    pub(super) fn stream(
        self: &Arc<Self>,
        units: Range<usize>,
        batch_size: usize,
    ) -> SendableRecordBatchStream {
        let batches = RangeBatches {
            input: Arc::clone(self),
            units,
            runs: 0..self.runs.len(),
            chunks: None,
        };
        CoalescedBatches::stream(
            Arc::clone(self.format.schema()),
            batches,
            batch_size,
        )
    }
}

/// One unit range of every run in turn, decoded a chunk at a time.
struct RangeBatches {
    input: Arc<BucketedInput>,
    units: Range<usize>,
    /// Runs not opened yet.
    runs: Range<usize>,
    /// The open run's range, `None` before the first and for a run without
    /// rows of `units`.
    chunks: Option<BucketChunks>,
}

impl RangeBatches {
    fn try_next(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if let Some(chunks) = &mut self.chunks {
                match chunks.next().transpose()? {
                    Some(batch) => return Ok(Some(batch)),
                    None => self.chunks = None,
                }
            }
            let Some(run) = self.runs.next() else {
                return Ok(None);
            };
            let run = &self.input.runs[run];
            self.chunks = run.chunks(&self.input.format, self.units.clone())?;
        }
    }
}

impl Iterator for RangeBatches {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

/// The batches of a group's input in batches of `batch_size` rows. A segment
/// of a run, or a group's rows of one batch in memory, is often a small part
/// of a batch, and a hash join pays per batch, so small ones are copied into
/// full batches. One of more than half a batch passes as it is, sharing its
/// buffers, with the threshold DataFusion's own coalescing operators use.
pub(super) struct CoalescedBatches<I> {
    input: I,
    coalescer: BatchCoalescer,
    ended: bool,
}

impl<I> CoalescedBatches<I>
where
    I: Iterator<Item = Result<RecordBatch>> + Send + 'static,
{
    pub(super) fn stream(
        schema: SchemaRef,
        input: I,
        batch_size: usize,
    ) -> SendableRecordBatchStream {
        let batches = Self {
            input,
            coalescer: BatchCoalescer::new(Arc::clone(&schema), batch_size)
                .with_biggest_coalesce_batch_size(Some(batch_size / 2)),
            ended: false,
        };
        Box::pin(RecordBatchStreamAdapter::new(schema, stream::iter(batches)))
    }

    fn try_next(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if let Some(batch) = self.coalescer.next_completed_batch() {
                return Ok(Some(batch));
            }
            if self.ended {
                return Ok(None);
            }
            match self.input.next().transpose()? {
                Some(batch) => self.coalescer.push_batch(batch)?,
                None => {
                    self.ended = true;
                    self.coalescer.finish_buffered_batch()?;
                }
            }
        }
    }
}

impl<I> Iterator for CoalescedBatches<I>
where
    I: Iterator<Item = Result<RecordBatch>> + Send + 'static,
{
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}
