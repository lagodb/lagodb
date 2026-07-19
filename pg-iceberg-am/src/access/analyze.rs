//! Iceberg-aware PostgreSQL `ANALYZE` sampling.
//!
//! PostgreSQL exposes a block-oriented callback, while Iceberg's stable
//! physical population is rows in planned data files. This module treats the
//! `ReadStream` blocks as sampling tickets, selects a bounded number of data
//! files, and samples their physical rows through iceberg-lite's normal
//! delete-aware pipeline.

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
    zero_row_files: Box<[AnalyzeFile]>,
    physical_rows: u64,
}

struct AnalyzeFile {
    task: FileScanTask,
    record_count: u64,
}

impl AnalyzePopulation {
    fn try_new(tasks: Vec<FileScanTask>) -> IcebergResult<Self> {
        let mut files = Vec::with_capacity(tasks.len());
        let mut zero_row_files = Vec::new();
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
            if !paths.insert(task.data_file_path.clone()) {
                return Err(IcebergError::InvariantViolated(
                    "ANALYZE planned duplicate data-file tasks",
                ));
            }
            if record_count == 0 {
                zero_row_files.push(AnalyzeFile { task, record_count });
                continue;
            }
            files.push(AnalyzeFile { task, record_count });
            physical_rows = physical_rows.checked_add(record_count).ok_or(
                IcebergError::InvariantViolated(
                    "Iceberg physical row population exceeds unsigned long range",
                ),
            )?;
        }

        Ok(Self {
            files: files.into_boxed_slice(),
            zero_row_files: zero_row_files.into_boxed_slice(),
            physical_rows,
        })
    }

    fn locality_plan<R: Rng + ?Sized>(
        self,
        desired_candidates: u64,
        max_data_files: usize,
        rng: &mut R,
    ) -> IcebergResult<AnalyzePlannedSample> {
        let full_plan_file_count = self
            .files
            .len()
            .checked_add(self.zero_row_files.len())
            .ok_or(IcebergError::InvariantViolated(
                "ANALYZE data-file count overflowed",
            ))?;
        if desired_candidates == self.physical_rows
            && full_plan_file_count != 0
            && full_plan_file_count <= max_data_files
        {
            return Ok(AnalyzePlannedSample {
                read_plan: self.full_plan(),
                candidate_count: desired_candidates,
            });
        }
        let record_counts = self
            .files
            .iter()
            .map(|file| file.record_count)
            .collect::<Vec<_>>();
        let sampling = AnalyzeSampler::sample_population(
            &record_counts,
            self.physical_rows,
            desired_candidates,
            max_data_files,
            rng,
        )?;

        let mut requests = Vec::new();
        let mut expected_files = Vec::new();
        let mut samples = sampling.files.into_iter().peekable();

        for (file_index, file) in self.files.into_vec().into_iter().enumerate() {
            let Some(sample) = samples.peek() else {
                break;
            };
            if sample.file_index != file_index {
                continue;
            }
            let sample = samples.next().ok_or(IcebergError::InvariantViolated(
                "ANALYZE sampled file disappeared during plan construction",
            ))?;
            if sample.positions.is_empty() {
                continue;
            }
            let path: Arc<str> = Arc::from(file.task.data_file_path.as_str());
            let positions = sample
                .positions
                .iter()
                .map(|position| position.position)
                .collect::<Vec<_>>();
            expected_files.push(ExpectedFile {
                path,
                positions: ExpectedPositions::Selected(
                    sample.positions.into_boxed_slice(),
                ),
            });
            requests.push(SelectedRowsReadRequest::try_new(file.task, positions)?);
        }
        if samples.next().is_some() {
            return Err(IcebergError::InvariantViolated(
                "ANALYZE sampled file does not belong to the population",
            ));
        }

        Ok(AnalyzePlannedSample {
            read_plan: AnalyzeReadPlan::Selected {
                requests,
                expected: ExpectedCursor::new(expected_files),
            },
            candidate_count: sampling.observation_count,
        })
    }

    fn full_plan(self) -> AnalyzeReadPlan {
        let file_count = self.files.len() + self.zero_row_files.len();
        let mut tasks = Vec::with_capacity(file_count);
        let mut expected_files = Vec::with_capacity(file_count);
        for file in self.files.into_iter().chain(self.zero_row_files) {
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

struct AnalyzePlannedSample {
    read_plan: AnalyzeReadPlan,
    candidate_count: u64,
}

struct AnalyzePopulationSample {
    files: Vec<AnalyzeFileSample>,
    observation_count: u64,
}

struct AnalyzeFileSample {
    file_index: usize,
    positions: Vec<SampledPosition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SampledPosition {
    position: u64,
    multiplicity: u64,
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

    /// Select a locality-bounded file set whose probability is proportional to
    /// its manifest row count, then draw a fixed number of observations from
    /// that set. Every physical row has the same expected observation count.
    fn sample_population<R: Rng + ?Sized>(
        record_counts: &[u64],
        physical_rows: u64,
        desired_candidates: u64,
        max_data_files: usize,
        rng: &mut R,
    ) -> IcebergResult<AnalyzePopulationSample> {
        if desired_candidates > physical_rows {
            return Err(IcebergError::InvariantViolated(
                "ANALYZE desired sample exceeds physical row population",
            ));
        }
        if max_data_files == 0 {
            return Err(IcebergError::InvariantViolated(
                "ANALYZE data-file limit must be positive",
            ));
        }
        let verified_rows = record_counts.iter().try_fold(0_u64, |sum, count| {
            sum.checked_add(*count)
                .ok_or(IcebergError::InvariantViolated(
                    "ANALYZE physical row population overflowed",
                ))
        })?;
        if verified_rows != physical_rows {
            return Err(IcebergError::InvariantViolated(
                "ANALYZE file record counts do not match physical population",
            ));
        }
        if record_counts.is_empty() || physical_rows == 0 || desired_candidates == 0 {
            return Ok(AnalyzePopulationSample {
                files: Vec::new(),
                observation_count: 0,
            });
        }

        let sampled_file_count = record_counts.len().min(max_data_files);
        let total_file_count = u64::try_from(record_counts.len()).map_err(|_| {
            IcebergError::InvariantViolated(
                "ANALYZE file population exceeds unsigned long range",
            )
        })?;
        let anchor_ticket = rng.random_range(0..physical_rows);
        let mut cumulative_rows = 0_u64;
        let anchor_index = record_counts
            .iter()
            .position(|record_count| {
                cumulative_rows = cumulative_rows.saturating_add(*record_count);
                anchor_ticket < cumulative_rows
            })
            .ok_or(IcebergError::InvariantViolated(
                "ANALYZE weighted file sample did not select an anchor",
            ))?;

        let mut file_indices = Vec::with_capacity(sampled_file_count);
        file_indices.push(anchor_index);
        if sampled_file_count > 1 {
            let remaining_population = total_file_count.checked_sub(1).ok_or(
                IcebergError::InvariantViolated(
                    "ANALYZE file population underflowed",
                ),
            )?;
            let remaining_sample =
                u64::try_from(sampled_file_count - 1).map_err(|_| {
                    IcebergError::InvariantViolated(
                        "ANALYZE sampled file count exceeds unsigned long range",
                    )
                })?;
            for logical_index in
                Self::sample_ordinals(remaining_population, remaining_sample, rng)?
            {
                let logical_index = usize::try_from(logical_index).map_err(|_| {
                    IcebergError::InvariantViolated(
                        "ANALYZE sampled file index exceeds platform capacity",
                    )
                })?;
                file_indices.push(if logical_index < anchor_index {
                    logical_index
                } else {
                    logical_index + 1
                });
            }
        }
        file_indices.sort_unstable();

        let selected_rows = file_indices.iter().try_fold(0_u64, |sum, index| {
            sum.checked_add(record_counts[*index]).ok_or(
                IcebergError::InvariantViolated(
                    "ANALYZE selected row population overflowed",
                ),
            )
        })?;
        let observations =
            Self::sample_observations(selected_rows, desired_candidates, rng)?;
        let mut observations = observations.into_iter().peekable();
        let mut selected_offset = 0_u64;
        let mut files = Vec::with_capacity(file_indices.len());
        for file_index in file_indices {
            let file_end = selected_offset
                .checked_add(record_counts[file_index])
                .ok_or(IcebergError::InvariantViolated(
                    "ANALYZE selected file boundary overflowed",
                ))?;
            let mut positions = Vec::new();
            while let Some(observation) = observations.peek() {
                if observation.position >= file_end {
                    break;
                }
                let observation =
                    observations.next().ok_or(IcebergError::InvariantViolated(
                        "ANALYZE sampled observation disappeared",
                    ))?;
                positions.push(SampledPosition {
                    position: observation.position - selected_offset,
                    multiplicity: observation.multiplicity,
                });
            }
            if !positions.is_empty() {
                files.push(AnalyzeFileSample {
                    file_index,
                    positions,
                });
            }
            selected_offset = file_end;
        }
        if observations.next().is_some() {
            return Err(IcebergError::InvariantViolated(
                "ANALYZE sampled observation lies outside selected files",
            ));
        }

        Ok(AnalyzePopulationSample {
            files,
            observation_count: desired_candidates,
        })
    }

    fn sample_observations<R: Rng + ?Sized>(
        population: u64,
        observation_count: u64,
        rng: &mut R,
    ) -> IcebergResult<Vec<SampledPosition>> {
        if observation_count <= population {
            return Ok(Self::sample_ordinals(population, observation_count, rng)?
                .into_iter()
                .map(|position| SampledPosition {
                    position,
                    multiplicity: 1,
                })
                .collect());
        }
        if population == 0 {
            return Err(IcebergError::InvariantViolated(
                "ANALYZE cannot sample observations from an empty file set",
            ));
        }
        let capacity = usize::try_from(observation_count).map_err(|_| {
            IcebergError::InvariantViolated(
                "ANALYZE observation count exceeds platform collection capacity",
            )
        })?;
        let mut observations = Vec::with_capacity(capacity);
        for _ in 0..observation_count {
            observations.push(rng.random_range(0..population));
        }
        observations.sort_unstable();

        let mut sampled: Vec<SampledPosition> = Vec::with_capacity(
            observations
                .len()
                .min(usize::try_from(population).unwrap_or(usize::MAX)),
        );
        for position in observations {
            if let Some(last) = sampled.last_mut()
                && last.position == position
            {
                last.multiplicity = last.multiplicity.checked_add(1).ok_or(
                    IcebergError::InvariantViolated(
                        "ANALYZE observation multiplicity overflowed",
                    ),
                )?;
            } else {
                sampled.push(SampledPosition {
                    position,
                    multiplicity: 1,
                });
            }
        }
        Ok(sampled)
    }

    fn desired_candidate_count(
        physical_rows: u64,
        virtual_blocks: u64,
        tickets: u64,
        target_rows: u64,
    ) -> IcebergResult<u64> {
        if tickets > virtual_blocks {
            return Err(IcebergError::InvariantViolated(
                "PostgreSQL ANALYZE selected more tickets than virtual blocks",
            ));
        }
        if tickets == 0 || physical_rows == 0 {
            return Ok(0);
        }
        if virtual_blocks == 0 {
            return Err(IcebergError::InvariantViolated(
                "non-empty ANALYZE sample has zero virtual blocks",
            ));
        }
        let requested_rows = if tickets < virtual_blocks {
            tickets
        } else {
            tickets.max(target_rows)
        };
        Ok(physical_rows.min(requested_rows))
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
                (scan.to_arrow_with_validated_tasks(tasks)?, expected)
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
    #[cfg(not(feature = "pg17"))]
    target_rows: u64,
}

impl AnalyzePreparation {
    pub(crate) fn try_new(
        scan: TableScan,
        tasks: Vec<FileScanTask>,
        decoder: ArrowColumnDecoder,
        storage_bytes: u64,
        #[cfg(not(feature = "pg17"))] statistics_target: i32,
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
        #[cfg(not(feature = "pg17"))]
        let target_rows = {
            const MIN_ANALYZE_SAMPLE_ROWS: u64 = 100;
            const SAMPLE_ROWS_PER_STATISTICS_TARGET: u64 = 300;
            let statistics_target =
                u64::try_from(statistics_target).map_err(|_| {
                    IcebergError::InvariantViolated(
                        "ANALYZE statistics target must not be negative",
                    )
                })?;
            statistics_target
                .checked_mul(SAMPLE_ROWS_PER_STATISTICS_TARGET)
                .ok_or(IcebergError::InvariantViolated(
                    "ANALYZE statistics sample target overflowed",
                ))?
                .max(MIN_ANALYZE_SAMPLE_ROWS)
        };
        Ok(Self {
            scan,
            population,
            decoder,
            virtual_blocks,
            #[cfg(not(feature = "pg17"))]
            target_rows,
        })
    }

    fn start(
        self,
        tickets: u64,
        target_rows: u64,
        seed: u64,
    ) -> AmResult<AnalyzeBatchCursor> {
        let physical_rows = self.population.physical_rows;
        let desired_candidates = AnalyzeSampler::desired_candidate_count(
            physical_rows,
            self.virtual_blocks,
            tickets,
            target_rows,
        )?;
        let mut rng = StdRng::seed_from_u64(seed);
        let max_data_files = crate::gucs::analyze_max_data_files();
        let sample = self.population.locality_plan(
            desired_candidates,
            max_data_files,
            &mut rng,
        )?;
        let weight = if sample.candidate_count == 0 {
            0.0
        } else {
            (tickets as f64 / self.virtual_blocks as f64)
                * (physical_rows as f64 / sample.candidate_count as f64)
        };
        let (source, expected) = sample.read_plan.open(&self.scan)?;
        Ok(AnalyzeBatchCursor::try_new(
            source,
            self.decoder,
            expected,
            tickets,
            sample.candidate_count,
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

    pub(crate) fn next_block(
        &mut self,
        stream: &AnalyzeReadStreamHandle,
    ) -> AmResult<bool> {
        if let AnalyzeScanPhase::Pending(preparation) = &mut self.phase {
            let preparation =
                preparation.take().ok_or(IcebergError::InvariantViolated(
                    "ANALYZE preparation was consumed more than once",
                ))?;
            #[cfg(feature = "pg17")]
            let initial_sampler = stream.analyze_sampler_state().ok_or(
                IcebergError::InvariantViolated(
                    "ANALYZE ReadStream is missing valid PG17 BlockSampler state",
                ),
            )?;
            #[cfg(feature = "pg17")]
            if initial_sampler.visited_blocks() != 0
                || initial_sampler.selected_blocks() != 0
            {
                return Err(IcebergError::InvariantViolated(
                    "ANALYZE ReadStream was consumed before provider initialization",
                )
                .into());
            }
            let mut tickets = 0_u64;
            while stream.next_block().is_some() {
                tickets =
                    tickets
                        .checked_add(1)
                        .ok_or(IcebergError::InvariantViolated(
                            "PostgreSQL ANALYZE ticket count overflowed",
                        ))?;
            }
            #[cfg(feature = "pg17")]
            let completed_sampler = stream.analyze_sampler_state().ok_or(
                IcebergError::InvariantViolated(
                    "ANALYZE ReadStream lost its PG17 BlockSampler state",
                ),
            )?;
            #[cfg(feature = "pg17")]
            {
                let population_blocks = initial_sampler.population_blocks();
                let target_rows = initial_sampler.target_rows();
                let expected_tickets = population_blocks.min(target_rows);
                if completed_sampler.population_blocks() != population_blocks
                    || completed_sampler.target_rows() != target_rows
                    || preparation.virtual_blocks != population_blocks
                    || tickets != expected_tickets
                    || completed_sampler.selected_blocks() != tickets
                {
                    return Err(IcebergError::InvariantViolated(
                        "ANALYZE PG17 BlockSampler state is inconsistent with consumed tickets",
                    )
                    .into());
                }
            }
            if tickets == 0 {
                self.phase = AnalyzeScanPhase::Finished;
                return Ok(false);
            }
            let seed = rand::rng().random::<u64>();
            #[cfg(feature = "pg17")]
            let target_rows = initial_sampler.target_rows();
            #[cfg(not(feature = "pg17"))]
            let target_rows = preparation.target_rows;
            self.phase = AnalyzeScanPhase::Ready(
                preparation.start(tickets, target_rows, seed)?,
            );
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
                        if self.expected.advance()? {
                            self.row_index += 1;
                        }
                        return Ok(AnalyzeTupleOutcome::visible(self.live_weight));
                    }
                    Ordering::Greater => {
                        self.expected.advance()?;
                    }
                },
                None => {
                    self.expected.advance()?;
                }
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
    repetition_index: u64,
    ordinal: u64,
}

struct ExpectedFile {
    path: Arc<str>,
    positions: ExpectedPositions,
}

enum ExpectedPositions {
    Selected(Box<[SampledPosition]>),
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
            repetition_index: 0,
            ordinal: 0,
        }
    }

    fn current(&self) -> Option<RowIdentity> {
        let file = self.files.get(self.file_index)?;
        let position = match &file.positions {
            ExpectedPositions::Selected(positions) => {
                positions
                    .get(usize::try_from(self.position_index).ok()?)?
                    .position
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

    /// Advance one logical observation. Returns true when the underlying
    /// physical row has no remaining multiplicity and the reader may advance.
    fn advance(&mut self) -> IcebergResult<bool> {
        let file = self.files.get(self.file_index).ok_or(
            IcebergError::InvariantViolated(
                "ANALYZE expected cursor advanced past its population",
            ),
        )?;
        let multiplicity = match &file.positions {
            ExpectedPositions::Selected(positions) => positions
                .get(usize::try_from(self.position_index).map_err(|_| {
                    IcebergError::InvariantViolated(
                        "ANALYZE expected position index exceeds platform capacity",
                    )
                })?)
                .ok_or(IcebergError::InvariantViolated(
                    "ANALYZE expected selected position disappeared",
                ))?
                .multiplicity,
            ExpectedPositions::All(_) => 1,
        };
        self.repetition_index = self.repetition_index.checked_add(1).ok_or(
            IcebergError::InvariantViolated(
                "ANALYZE expected repetition index overflowed",
            ),
        )?;
        let physical_row_exhausted = self.repetition_index == multiplicity;
        if physical_row_exhausted {
            self.repetition_index = 0;
            self.position_index = self.position_index.checked_add(1).ok_or(
                IcebergError::InvariantViolated(
                    "ANALYZE expected position index overflowed",
                ),
            )?;
            let file_exhausted = match &file.positions {
                ExpectedPositions::Selected(positions) => {
                    usize::try_from(self.position_index)
                        .map_or(true, |index| index >= positions.len())
                }
                ExpectedPositions::All(count) => self.position_index >= *count,
            };
            if file_exhausted {
                self.file_index += 1;
                self.position_index = 0;
            }
        }
        self.ordinal =
            self.ordinal
                .checked_add(1)
                .ok_or(IcebergError::InvariantViolated(
                    "ANALYZE candidate ordinal overflowed",
                ))?;
        Ok(physical_row_exhausted)
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
    fn locality_sample_has_fixed_observation_and_file_bounds() {
        let mut record_counts = vec![1; 99];
        record_counts.push(10_000_000);
        let physical_rows = record_counts.iter().sum();
        let mut rng = StdRng::seed_from_u64(9);
        let sample = AnalyzeSampler::sample_population(
            &record_counts,
            physical_rows,
            30_000,
            8,
            &mut rng,
        )
        .unwrap();

        assert_eq!(sample.observation_count, 30_000);
        assert!(sample.files.len() <= 8);
        let unique_positions = sample
            .files
            .iter()
            .map(|file| file.positions.len())
            .sum::<usize>();
        assert!(unique_positions <= 30_000);
        assert!(
            sample
                .files
                .windows(2)
                .all(|pair| pair[0].file_index < pair[1].file_index)
        );
        assert_eq!(
            sample
                .files
                .iter()
                .flat_map(|file| &file.positions)
                .map(|position| position.multiplicity)
                .sum::<u64>(),
            30_000
        );
    }

    #[test]
    fn uneven_files_produce_unbiased_fixed_size_observations() {
        const RUNS: u64 = 20_000;
        let record_counts = [10_u64, 100, 1_000];
        let physical_rows = record_counts.into_iter().sum();
        let mut observations = [0_u64; 3];

        for seed in 0..RUNS {
            let mut rng = StdRng::seed_from_u64(seed);
            let sample = AnalyzeSampler::sample_population(
                &record_counts,
                physical_rows,
                60,
                2,
                &mut rng,
            )
            .unwrap();
            assert_eq!(sample.observation_count, 60);
            for file in sample.files {
                observations[file.file_index] += file
                    .positions
                    .iter()
                    .map(|position| position.multiplicity)
                    .sum::<u64>();
            }
        }

        let total_observations = RUNS * 60;
        for (observed, population) in observations.into_iter().zip(record_counts) {
            let observed_fraction = observed as f64 / total_observations as f64;
            let population_fraction = population as f64 / physical_rows as f64;
            assert!((observed_fraction - population_fraction).abs() < 0.005);
        }
    }

    #[test]
    fn replacement_sampling_preserves_fixed_observation_count() {
        let sample = (0..10_000)
            .find_map(|seed| {
                let mut rng = StdRng::seed_from_u64(seed);
                let sample = AnalyzeSampler::sample_population(
                    &[1, 1, 1, 100],
                    103,
                    20,
                    1,
                    &mut rng,
                )
                .unwrap();
                (sample.files[0].positions[0].multiplicity > 1).then_some(sample)
            })
            .expect(
                "a small anchor file should be selected across deterministic seeds",
            );

        assert_eq!(sample.files.len(), 1);
        assert_eq!(sample.observation_count, 20);
        assert_eq!(sample.files[0].positions.len(), 1);
        assert_eq!(sample.files[0].positions[0].multiplicity, 20);
    }

    #[test]
    fn desired_candidates_are_bounded_by_postgresql_sample_target() {
        assert_eq!(
            AnalyzeSampler::desired_candidate_count(10_000, 1_000, 100, 30_000)
                .unwrap(),
            100
        );
        assert_eq!(
            AnalyzeSampler::desired_candidate_count(10_000_000, 1_000, 100, 30_000)
                .unwrap(),
            100
        );
        assert_eq!(
            AnalyzeSampler::desired_candidate_count(
                10_000_000,
                1_000,
                1_000,
                30_000,
            )
            .unwrap(),
            30_000
        );
        assert_eq!(
            AnalyzeSampler::desired_candidate_count(
                10_000_000, 1_000_000, 300_000, 30_000,
            )
            .unwrap(),
            300_000
        );
        assert!(
            AnalyzeSampler::desired_candidate_count(10, 100, 101, 30_000).is_err()
        );
    }

    #[test]
    fn locality_sampler_handles_empty_full_and_invalid_boundaries() {
        let mut rng = StdRng::seed_from_u64(23);
        let empty =
            AnalyzeSampler::sample_population(&[], 0, 0, 32, &mut rng).unwrap();
        assert!(empty.files.is_empty());
        assert_eq!(empty.observation_count, 0);

        let full =
            AnalyzeSampler::sample_population(&[5, 5], 10, 10, 32, &mut rng).unwrap();
        assert_eq!(full.observation_count, 10);
        assert_eq!(full.files.len(), 2);
        assert!(full.files.iter().all(|file| {
            file.positions.len() == 5
                && file
                    .positions
                    .iter()
                    .all(|position| position.multiplicity == 1)
        }));

        assert!(
            AnalyzeSampler::sample_population(&[10], 10, 1, 0, &mut rng).is_err()
        );
        assert!(AnalyzeSampler::sample_population(&[9], 10, 1, 1, &mut rng).is_err());
        assert!(
            AnalyzeSampler::sample_population(&[10], 10, 11, 1, &mut rng).is_err()
        );
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
    fn expected_cursor_repeats_one_physical_position_without_reordering() {
        let mut cursor = ExpectedCursor::new(vec![ExpectedFile {
            path: Arc::from("data.parquet"),
            positions: ExpectedPositions::Selected(
                vec![
                    SampledPosition {
                        position: 3,
                        multiplicity: 2,
                    },
                    SampledPosition {
                        position: 8,
                        multiplicity: 1,
                    },
                ]
                .into_boxed_slice(),
            ),
        }]);

        assert_eq!(cursor.current().unwrap().position, 3);
        assert!(!cursor.advance().unwrap());
        assert_eq!(cursor.current().unwrap().position, 3);
        assert!(cursor.advance().unwrap());
        assert_eq!(cursor.current().unwrap().position, 8);
        assert!(cursor.advance().unwrap());
        assert!(cursor.current().is_none());
        assert_eq!(cursor.ordinal, 3);
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
