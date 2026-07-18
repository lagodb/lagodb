//! Iceberg-aware PostgreSQL `ANALYZE` sampling.
//!
//! PostgreSQL exposes a block-oriented callback, while Iceberg's stable
//! physical population is rows in planned data files. This module treats the
//! `ReadStream` blocks as sampling tickets, selects physical row ordinals
//! uniformly without replacement, and reads those rows in per-file batches
//! through iceberg-lite's normal delete-aware pipeline.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::RecordBatch;
use iceberg_lite::arrow::SelectedRowsReadRequest;
use iceberg_lite::scan::{FileScanTask, TableScan};
use pg_arrow_conv::{ArrowBatchSource, ArrowColumnDecoder, BoundBatch};
use pg_lakebase_core::api::{AnalyzeTupleOutcome, TRIGGER_ROW_BLOCK_BASE};
use pg_lakebase_core::prelude::*;
use pgrx::pg_sys;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::scan::{
    BatchMetadataColumns, IcebergArrowBatchSource, IcebergArrowBatches,
};
use crate::error::{IcebergError, IcebergResult};

const TUPLES_PER_SYNTHETIC_BLOCK: u64 = 2048;
const MAX_VIRTUAL_BLOCKS: u64 = u32::MAX as u64 - 1;

/// Immutable whole-snapshot population captured before sampling begins.
pub(crate) struct AnalyzePopulation {
    files: Box<[AnalyzeFile]>,
    physical_rows: u64,
}

struct AnalyzeFile {
    task: FileScanTask,
    start_ordinal: u64,
    record_count: u64,
}

impl AnalyzePopulation {
    fn try_new(tasks: Vec<FileScanTask>) -> IcebergResult<Self> {
        let mut files = Vec::with_capacity(tasks.len());
        let mut paths = HashSet::with_capacity(tasks.len());
        let mut physical_rows = 0_u64;

        for task in tasks {
            if task.start != 0 || task.length != 0 {
                return Err(IcebergError::InvariantViolated(
                    "ANALYZE requires one whole-file task per Iceberg data file",
                ));
            }
            let record_count =
                task.record_count.ok_or(IcebergError::InvariantViolated(
                    "ANALYZE file task is missing its manifest record count",
                ))?;
            if record_count == 0 {
                continue;
            }
            if !paths.insert(task.data_file_path.clone()) {
                return Err(IcebergError::InvariantViolated(
                    "ANALYZE planned duplicate data-file tasks",
                ));
            }
            files.push(AnalyzeFile {
                task,
                start_ordinal: physical_rows,
                record_count,
            });
            physical_rows = physical_rows.checked_add(record_count).ok_or(
                IcebergError::InvariantViolated(
                    "Iceberg physical row population exceeds unsigned long range",
                ),
            )?;
        }

        Ok(Self {
            files: files.into_boxed_slice(),
            physical_rows,
        })
    }

    fn selected_plan<R: Rng + ?Sized>(
        self,
        candidate_count: u64,
        rng: &mut R,
    ) -> IcebergResult<AnalyzeReadPlan> {
        let ordinals = AnalyzeSampler::sample_ordinals(
            self.physical_rows,
            candidate_count,
            rng,
        )?;
        let mut ordinal_index = 0;
        let mut requests = Vec::new();
        let mut expected_files = Vec::new();

        for file in self.files {
            let file_end = file.start_ordinal.checked_add(file.record_count).ok_or(
                IcebergError::InvariantViolated(
                    "ANALYZE file ordinal range overflowed",
                ),
            )?;
            let first = ordinal_index;
            while ordinal_index < ordinals.len() && ordinals[ordinal_index] < file_end
            {
                ordinal_index += 1;
            }
            if first == ordinal_index {
                continue;
            }
            let path: Arc<str> = Arc::from(file.task.data_file_path.as_str());
            let positions = ordinals[first..ordinal_index]
                .iter()
                .map(|ordinal| ordinal - file.start_ordinal)
                .collect::<Vec<_>>();
            expected_files.push(ExpectedFile {
                path,
                positions: ExpectedPositions::Selected(
                    positions.clone().into_boxed_slice(),
                ),
            });
            requests.push(SelectedRowsReadRequest::try_new(file.task, positions)?);
        }
        if ordinal_index != ordinals.len() {
            return Err(IcebergError::InvariantViolated(
                "sampled ordinal does not belong to a planned data file",
            ));
        }

        Ok(AnalyzeReadPlan::Selected {
            requests,
            expected: ExpectedCursor::new(expected_files),
        })
    }

    fn full_plan(self) -> AnalyzeReadPlan {
        let mut tasks = Vec::with_capacity(self.files.len());
        let mut expected_files = Vec::with_capacity(self.files.len());
        for file in self.files {
            expected_files.push(ExpectedFile {
                path: Arc::from(file.task.data_file_path.as_str()),
                positions: ExpectedPositions::All(file.record_count),
            });
            tasks.push(file.task);
        }
        AnalyzeReadPlan::Full {
            tasks,
            expected: ExpectedCursor::new(expected_files),
        }
    }
}

struct AnalyzeSampler;

impl AnalyzeSampler {
    /// Floyd's algorithm: O(sample size) memory and random draws even when the
    /// physical population is much larger than platform `usize`.
    fn sample_ordinals<R: Rng + ?Sized>(
        population: u64,
        sample_size: u64,
        rng: &mut R,
    ) -> IcebergResult<Vec<u64>> {
        if sample_size > population {
            return Err(IcebergError::InvariantViolated(
                "ANALYZE sample size exceeds physical row population",
            ));
        }
        let capacity = usize::try_from(sample_size).map_err(|_| {
            IcebergError::InvariantViolated(
                "ANALYZE sample size exceeds platform collection capacity",
            )
        })?;
        let mut selected = HashSet::with_capacity(capacity);
        for upper in population.saturating_sub(sample_size)..population {
            let candidate = rng.random_range(0..=upper);
            if !selected.insert(candidate) {
                selected.insert(upper);
            }
        }
        let mut ordinals = selected.into_iter().collect::<Vec<_>>();
        ordinals.sort_unstable();
        Ok(ordinals)
    }

    fn candidate_count_and_weight(
        physical_rows: u64,
        virtual_blocks: u64,
        tickets: u64,
    ) -> IcebergResult<(u64, f64)> {
        if tickets > virtual_blocks {
            return Err(IcebergError::InvariantViolated(
                "PostgreSQL ANALYZE selected more tickets than virtual blocks",
            ));
        }
        if tickets == 0 || physical_rows == 0 {
            return Ok((0, 0.0));
        }
        if virtual_blocks == 0 {
            return Err(IcebergError::InvariantViolated(
                "non-empty ANALYZE sample has zero virtual blocks",
            ));
        }

        let candidate_count = if tickets == virtual_blocks {
            physical_rows
        } else {
            physical_rows.min(tickets)
        };
        let weight = (tickets as f64 / virtual_blocks as f64)
            * (physical_rows as f64 / candidate_count as f64);
        Ok((candidate_count, weight))
    }
}

enum AnalyzeReadPlan {
    Selected {
        requests: Vec<SelectedRowsReadRequest>,
        expected: ExpectedCursor,
    },
    Full {
        tasks: Vec<FileScanTask>,
        expected: ExpectedCursor,
    },
}

impl AnalyzeReadPlan {
    fn open(
        self,
        scan: &TableScan,
    ) -> IcebergResult<(IcebergArrowBatchSource, ExpectedCursor)> {
        let (batches, expected) = match self {
            Self::Selected { requests, expected } => {
                (scan.to_arrow_with_selected_rows(requests)?, expected)
            }
            Self::Full { tasks, expected } => {
                (scan.to_arrow_with_tasks(tasks)?, expected)
            }
        };
        Ok((
            ArrowBatchSource::new(IcebergArrowBatches(batches)),
            expected,
        ))
    }
}

/// Deferred ANALYZE plan. PostgreSQL's ticket count is not available until
/// the first `scan_analyze_next_block` callback.
pub(crate) struct AnalyzePreparation {
    scan: TableScan,
    population: AnalyzePopulation,
    decoder: ArrowColumnDecoder,
    virtual_blocks: u64,
}

impl AnalyzePreparation {
    pub(crate) fn try_new(
        scan: TableScan,
        tasks: Vec<FileScanTask>,
        decoder: ArrowColumnDecoder,
        storage_bytes: u64,
    ) -> IcebergResult<Self> {
        let population = AnalyzePopulation::try_new(tasks)?;
        let page_size = pg_sys::BLCKSZ as u64;
        let virtual_blocks =
            storage_bytes.div_ceil(page_size).min(MAX_VIRTUAL_BLOCKS);
        if population.physical_rows != 0 && virtual_blocks == 0 {
            return Err(IcebergError::InvariantViolated(
                "non-empty Iceberg population has zero virtual ANALYZE blocks",
            ));
        }
        Ok(Self {
            scan,
            population,
            decoder,
            virtual_blocks,
        })
    }

    fn start(self, tickets: u64, seed: u64) -> AmResult<AnalyzeBatchCursor> {
        let physical_rows = self.population.physical_rows;
        let (candidate_count, weight) = AnalyzeSampler::candidate_count_and_weight(
            physical_rows,
            self.virtual_blocks,
            tickets,
        )?;

        let plan = if tickets == self.virtual_blocks {
            self.population.full_plan()
        } else {
            let mut rng = StdRng::seed_from_u64(seed);
            self.population.selected_plan(candidate_count, &mut rng)?
        };
        let (source, expected) = plan.open(&self.scan)?;
        Ok(AnalyzeBatchCursor::try_new(
            source,
            self.decoder,
            expected,
            tickets,
            candidate_count,
            weight,
        )?)
    }
}

/// Aggregate state for the two PostgreSQL ANALYZE callbacks.
pub(crate) struct AnalyzeScanState {
    phase: AnalyzeScanPhase,
}

enum AnalyzeScanPhase {
    Pending(Option<AnalyzePreparation>),
    Ready(AnalyzeBatchCursor),
    Finished,
}

impl AnalyzeScanState {
    pub(crate) fn pending(preparation: AnalyzePreparation) -> Self {
        Self {
            phase: AnalyzeScanPhase::Pending(Some(preparation)),
        }
    }

    pub(crate) fn next_block(&mut self, stream: &ReadStreamHandle) -> AmResult<bool> {
        if let AnalyzeScanPhase::Pending(preparation) = &mut self.phase {
            let preparation =
                preparation.take().ok_or(IcebergError::InvariantViolated(
                    "ANALYZE preparation was consumed more than once",
                ))?;
            let mut tickets = 0_u64;
            while stream.next_block().is_some() {
                tickets =
                    tickets
                        .checked_add(1)
                        .ok_or(IcebergError::InvariantViolated(
                            "PostgreSQL ANALYZE ticket count overflowed",
                        ))?;
            }
            if tickets == 0 {
                self.phase = AnalyzeScanPhase::Finished;
                return Ok(false);
            }
            let seed = rand::rng().random::<u64>();
            self.phase = AnalyzeScanPhase::Ready(preparation.start(tickets, seed)?);
        }

        match &mut self.phase {
            AnalyzeScanPhase::Ready(cursor) => cursor.next_ticket(),
            AnalyzeScanPhase::Finished => Ok(false),
            AnalyzeScanPhase::Pending(_) => Err(IcebergError::InvariantViolated(
                "ANALYZE state remained pending after initialization",
            )
            .into()),
        }
    }

    pub(crate) fn next_tuple(
        &mut self,
        out: &mut SlotColumns<'_>,
    ) -> AmResult<AnalyzeTupleOutcome> {
        match &mut self.phase {
            AnalyzeScanPhase::Ready(cursor) => cursor.next_tuple(out),
            AnalyzeScanPhase::Finished => Ok(AnalyzeTupleOutcome::end_of_block()),
            AnalyzeScanPhase::Pending(_) => Err(IcebergError::InvariantViolated(
                "ANALYZE tuple callback ran before block initialization",
            )
            .into()),
        }
    }
}

struct AnalyzeBoundBatch {
    decoded: BoundBatch,
    metadata: BatchMetadataColumns,
}

struct AnalyzeBatchCursor {
    source: IcebergArrowBatchSource,
    decoder: ArrowColumnDecoder,
    current: Option<AnalyzeBoundBatch>,
    row_index: usize,
    expected: ExpectedCursor,
    file_indices: HashMap<Arc<str>, usize>,
    cached_returned_file: Option<(Arc<str>, usize)>,
    tickets: u64,
    next_ticket: u64,
    ticket_end: u64,
    candidate_count: u64,
    live_weight: f64,
}

impl AnalyzeBatchCursor {
    fn try_new(
        source: IcebergArrowBatchSource,
        decoder: ArrowColumnDecoder,
        expected: ExpectedCursor,
        tickets: u64,
        candidate_count: u64,
        live_weight: f64,
    ) -> IcebergResult<Self> {
        let mut file_indices = HashMap::with_capacity(expected.files.len());
        for (index, file) in expected.files.iter().enumerate() {
            if file_indices.insert(file.path.clone(), index).is_some() {
                return Err(IcebergError::InvariantViolated(
                    "ANALYZE expected duplicate data-file paths",
                ));
            }
        }
        Ok(Self {
            source,
            decoder,
            current: None,
            row_index: 0,
            expected,
            file_indices,
            cached_returned_file: None,
            tickets,
            next_ticket: 0,
            ticket_end: 0,
            candidate_count,
            live_weight,
        })
    }

    fn next_ticket(&mut self) -> AmResult<bool> {
        if self.next_ticket == self.tickets {
            return Ok(false);
        }
        self.next_ticket += 1;
        self.ticket_end = u64::try_from(
            (u128::from(self.next_ticket) * u128::from(self.candidate_count))
                / u128::from(self.tickets),
        )
        .map_err(|_| {
            IcebergError::InvariantViolated(
                "ANALYZE ticket boundary exceeds unsigned long range",
            )
        })?;
        Ok(true)
    }

    fn next_tuple(
        &mut self,
        out: &mut SlotColumns<'_>,
    ) -> AmResult<AnalyzeTupleOutcome> {
        while self.expected.ordinal < self.ticket_end {
            let expected =
                self.expected
                    .current()
                    .ok_or(IcebergError::InvariantViolated(
                        "ANALYZE expected candidate stream ended early",
                    ))?;
            match self.peek_returned_identity()? {
                Some(returned) => match returned.cmp(&expected) {
                    Ordering::Less => {
                        return Err(IcebergError::InvariantViolated(
                            "ANALYZE reader returned rows out of planned order",
                        )
                        .into());
                    }
                    Ordering::Equal => {
                        let ordinal = self.expected.ordinal;
                        self.write_current_row(out, ordinal)?;
                        self.expected.advance()?;
                        return Ok(AnalyzeTupleOutcome::visible(self.live_weight));
                    }
                    Ordering::Greater => self.expected.advance()?,
                },
                None => self.expected.advance()?,
            }
        }

        if self.next_ticket == self.tickets {
            if self.expected.current().is_some() {
                return Err(IcebergError::InvariantViolated(
                    "ANALYZE ticket partition did not consume all candidates",
                )
                .into());
            }
            if self.peek_returned_identity()?.is_some() {
                return Err(IcebergError::InvariantViolated(
                    "ANALYZE reader returned an unrequested physical row",
                )
                .into());
            }
        }
        Ok(AnalyzeTupleOutcome::end_of_block())
    }

    fn peek_returned_identity(&mut self) -> AmResult<Option<RowIdentity>> {
        loop {
            if let Some(batch) = self.current.as_ref()
                && self.row_index < self.decoder.num_rows(&batch.decoded)
            {
                let path = batch.metadata.file(self.row_index)?;
                let file_index = if let Some((cached_path, index)) =
                    self.cached_returned_file.as_ref()
                    && cached_path.as_ref() == path
                {
                    *index
                } else {
                    let index = *self.file_indices.get(path).ok_or(
                        IcebergError::InvariantViolated(
                            "ANALYZE reader returned an unplanned data file",
                        ),
                    )?;
                    self.cached_returned_file = Some((Arc::from(path), index));
                    index
                };
                return Ok(Some(RowIdentity {
                    file_index,
                    position: batch.metadata.position(self.row_index)?,
                }));
            }

            self.current = None;
            let Some(batch) = self.source.next_batch()? else {
                return Ok(None);
            };
            self.current = Some(Self::bind_batch(&self.decoder, batch)?);
            self.row_index = 0;
        }
    }

    fn bind_batch(
        decoder: &ArrowColumnDecoder,
        batch: RecordBatch,
    ) -> AmResult<AnalyzeBoundBatch> {
        let metadata = BatchMetadataColumns::try_new(&batch)?;
        let decoded = decoder.bind(batch)?;
        Ok(AnalyzeBoundBatch { decoded, metadata })
    }

    fn write_current_row(
        &mut self,
        out: &mut SlotColumns<'_>,
        sample_ordinal: u64,
    ) -> AmResult<()> {
        let batch = self
            .current
            .as_ref()
            .ok_or(IcebergError::InvariantViolated(
                "ANALYZE current batch disappeared before decode",
            ))?;
        self.decoder
            .write_row(&batch.decoded, self.row_index, out)?;
        out.set_tid(&Self::synthetic_tid(sample_ordinal)?);
        self.row_index += 1;
        Ok(())
    }

    fn synthetic_tid(sample_ordinal: u64) -> IcebergResult<ItemPointer> {
        let block = sample_ordinal / TUPLES_PER_SYNTHETIC_BLOCK;
        if block >= u64::from(TRIGGER_ROW_BLOCK_BASE) {
            return Err(IcebergError::AnalyzeTidCapacityExceeded);
        }
        let offset = u16::try_from(sample_ordinal % TUPLES_PER_SYNTHETIC_BLOCK + 1)
            .map_err(|_| {
            IcebergError::InvariantViolated(
                "ANALYZE synthetic tuple offset exceeds PostgreSQL range",
            )
        })?;
        Ok(ItemPointer {
            block_number: u32::try_from(block).map_err(|_| {
                IcebergError::InvariantViolated(
                    "ANALYZE synthetic tuple block exceeds PostgreSQL range",
                )
            })?,
            offset,
        })
    }
}

struct ExpectedCursor {
    files: Box<[ExpectedFile]>,
    file_index: usize,
    position_index: u64,
    ordinal: u64,
}

struct ExpectedFile {
    path: Arc<str>,
    positions: ExpectedPositions,
}

enum ExpectedPositions {
    Selected(Box<[u64]>),
    All(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RowIdentity {
    file_index: usize,
    position: u64,
}

impl ExpectedCursor {
    fn new(files: Vec<ExpectedFile>) -> Self {
        Self {
            files: files.into_boxed_slice(),
            file_index: 0,
            position_index: 0,
            ordinal: 0,
        }
    }

    fn current(&self) -> Option<RowIdentity> {
        let file = self.files.get(self.file_index)?;
        let position = match &file.positions {
            ExpectedPositions::Selected(positions) => {
                *positions.get(usize::try_from(self.position_index).ok()?)?
            }
            ExpectedPositions::All(count) if self.position_index < *count => {
                self.position_index
            }
            ExpectedPositions::All(_) => return None,
        };
        Some(RowIdentity {
            file_index: self.file_index,
            position,
        })
    }

    fn advance(&mut self) -> IcebergResult<()> {
        let file = self.files.get(self.file_index).ok_or(
            IcebergError::InvariantViolated(
                "ANALYZE expected cursor advanced past its population",
            ),
        )?;
        self.position_index = self.position_index.checked_add(1).ok_or(
            IcebergError::InvariantViolated(
                "ANALYZE expected position index overflowed",
            ),
        )?;
        let exhausted = match &file.positions {
            ExpectedPositions::Selected(positions) => {
                usize::try_from(self.position_index)
                    .map_or(true, |index| index >= positions.len())
            }
            ExpectedPositions::All(count) => self.position_index >= *count,
        };
        if exhausted {
            self.file_index += 1;
            self.position_index = 0;
        }
        self.ordinal =
            self.ordinal
                .checked_add(1)
                .ok_or(IcebergError::InvariantViolated(
                    "ANALYZE candidate ordinal overflowed",
                ))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floyd_sampler_is_sorted_unique_and_bounded() {
        let mut rng = StdRng::seed_from_u64(42);
        let sample =
            AnalyzeSampler::sample_ordinals(10_000, 1_000, &mut rng).unwrap();

        assert_eq!(sample.len(), 1_000);
        assert!(sample.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(sample.iter().all(|ordinal| *ordinal < 10_000));
    }

    #[test]
    fn floyd_sampler_handles_empty_full_and_u64_populations() {
        let mut rng = StdRng::seed_from_u64(7);
        assert!(
            AnalyzeSampler::sample_ordinals(0, 0, &mut rng)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            AnalyzeSampler::sample_ordinals(5, 5, &mut rng).unwrap(),
            vec![0, 1, 2, 3, 4]
        );

        let sample = AnalyzeSampler::sample_ordinals(u64::MAX, 4, &mut rng).unwrap();
        assert_eq!(sample.len(), 4);
        assert!(sample.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(AnalyzeSampler::sample_ordinals(3, 4, &mut rng).is_err());
    }

    #[test]
    fn floyd_sampler_has_near_uniform_long_run_frequency() {
        const POPULATION: usize = 10;
        const SAMPLE_SIZE: u64 = 3;
        const RUNS: u64 = 10_000;
        let expected = RUNS * SAMPLE_SIZE / POPULATION as u64;
        let tolerance = expected / 10;
        let mut counts = [0_u64; POPULATION];

        for seed in 0..RUNS {
            let mut rng = StdRng::seed_from_u64(seed);
            for ordinal in AnalyzeSampler::sample_ordinals(
                POPULATION as u64,
                SAMPLE_SIZE,
                &mut rng,
            )
            .unwrap()
            {
                counts[ordinal as usize] += 1;
            }
        }

        assert!(
            counts
                .into_iter()
                .all(|count| count.abs_diff(expected) <= tolerance)
        );
    }

    #[test]
    fn candidate_weight_matches_postgresql_extrapolation() {
        let (candidates, weight) =
            AnalyzeSampler::candidate_count_and_weight(10_000, 1_000, 100).unwrap();
        assert_eq!(candidates, 100);
        assert_eq!(weight, 10.0);
        assert_eq!(1_000.0 / 100.0 * 100.0 * weight, 10_000.0);

        let (candidates, weight) =
            AnalyzeSampler::candidate_count_and_weight(10_000, 1_000, 1_000).unwrap();
        assert_eq!(candidates, 10_000);
        assert_eq!(weight, 1.0);

        let (candidates, weight) =
            AnalyzeSampler::candidate_count_and_weight(10, 1_000, 100).unwrap();
        assert_eq!(candidates, 10);
        assert!((weight - 0.1).abs() < f64::EPSILON);
    }

    #[test]
    fn ticket_boundaries_partition_every_candidate_once() {
        let boundaries = (1_u128..=7)
            .map(|ticket| u64::try_from(ticket * 23 / 7).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(boundaries.last(), Some(&23));
        assert!(boundaries.windows(2).all(|pair| pair[0] <= pair[1]));
    }

    #[test]
    fn synthetic_tids_are_monotonic_across_block_boundaries() {
        assert_eq!(
            AnalyzeBatchCursor::synthetic_tid(2047).unwrap(),
            ItemPointer {
                block_number: 0,
                offset: 2048,
            }
        );
        assert_eq!(
            AnalyzeBatchCursor::synthetic_tid(2048).unwrap(),
            ItemPointer {
                block_number: 1,
                offset: 1,
            }
        );
    }
}
