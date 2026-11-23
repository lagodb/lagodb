//! A level's probe split by group, kept in memory while it never spilled.

use std::ops::Range;
use std::sync::Arc;

use arrow_array::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::physical_plan::metrics::SpillMetrics;

use super::partitioner::SealedRun;
use super::runs::{BucketSizes, BucketedInput, CoalescedBatches};
use crate::datafusion::spill::BucketRunFormat;

/// A level's probe, one unit per group of its build.
pub(super) enum ProbeInput {
    /// Its one run, which never had to leave memory.
    Memory(Arc<ProbeTail>),
    Spilled(Arc<BucketedInput>),
}

impl ProbeInput {
    pub(super) fn rows(&self, unit: usize) -> usize {
        match self {
            Self::Memory(tail) => tail.run.unit_positions(unit).len(),
            Self::Spilled(input) => input.rows(unit..unit + 1),
        }
    }

    /// The rows of `unit` in batches of `batch_size` rows.
    pub(super) fn stream(
        &self,
        unit: usize,
        batch_size: usize,
    ) -> SendableRecordBatchStream {
        match self {
            Self::Memory(tail) => {
                let batches = TailBatches {
                    positions: tail.run.unit_positions(unit),
                    tail: Arc::clone(tail),
                };
                CoalescedBatches::stream(
                    Arc::clone(tail.format.schema()),
                    batches,
                    batch_size,
                )
            }
            Self::Spilled(input) => input.stream(unit..unit + 1, batch_size),
        }
    }

    /// Write the units from `first` to disk and release the memory run. A
    /// group that splits again or sorts needs a second `work_mem` beside the
    /// probe's, which the partition's budget holds only once the probe is on
    /// disk. Groups before `first` have been joined.
    pub(super) fn spill_from(&mut self, first: usize) -> Result<()> {
        if let Self::Memory(tail) = self {
            let input = tail.spill_from(first)?;
            *self = Self::Spilled(Arc::new(input));
        }
        Ok(())
    }
}

/// A probe's only run, ordered by group, with the reservation that books it.
pub(super) struct ProbeTail {
    format: Arc<BucketRunFormat>,
    run: SealedRun,
    spill: SpillMetrics,
    /// Released with the run, once the last group stream drops it.
    _reservation: MemoryReservation,
}

impl ProbeTail {
    pub(super) const fn new(
        format: Arc<BucketRunFormat>,
        run: SealedRun,
        reservation: MemoryReservation,
        spill: SpillMetrics,
    ) -> Self {
        Self {
            format,
            run,
            spill,
            _reservation: reservation,
        }
    }

    /// The units from `first` on disk. With no rows left, as a probe whose
    /// rows all fell in joined groups, nothing is written, as a partitioner
    /// writes no empty run.
    fn spill_from(&self, first: usize) -> Result<BucketedInput> {
        let mut sizes = BucketSizes::new(self.run.units());
        let runs = if self.run.rows_from(first) == 0 {
            Vec::new()
        } else {
            vec![
                self.run
                    .spill(first, &self.format, &mut sizes, &self.spill)?,
            ]
        };
        Ok(BucketedInput::new(Arc::clone(&self.format), runs, sizes))
    }
}

/// One group's rows of a memory run, taken one batch at a time.
struct TailBatches {
    tail: Arc<ProbeTail>,
    positions: Range<usize>,
}

impl Iterator for TailBatches {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.positions.is_empty() {
            return None;
        }
        Some(
            self.tail
                .run
                .take_batch(self.positions.clone())
                .map(|(batch, next)| {
                    self.positions.start = next;
                    batch
                }),
        )
    }
}
