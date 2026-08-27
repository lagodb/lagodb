//! Iceberg-aware PostgreSQL `ANALYZE` sampling.
//!
//! PostgreSQL exposes a block-oriented callback, while Iceberg's stable
//! physical population is rows in planned data files. This module treats the
//! `ReadStream` blocks as sampling tickets, selects a bounded number of data
//! files, and samples their physical rows through iceberg-lite's normal
//! delete-aware pipeline.

mod cursor;
mod population;
mod sampling;

pub(crate) use population::AnalyzePopulation;

use iceberg_lite::scan::{FileScanTask, TableScan};
use lagodb_core::api::AnalyzeTupleOutcome;
use lagodb_core::prelude::*;
use pg_arrow_conv::ArrowColumnDecoder;
use pgrx::pg_sys;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use self::cursor::AnalyzeBatchCursor;
use self::sampling::AnalyzeSampler;
use crate::error::{IcebergError, IcebergResult};
use crate::managed_table::gucs::analyze_max_data_files;

const MAX_VIRTUAL_BLOCKS: u64 = u32::MAX as u64 - 1;

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
        let max_data_files = analyze_max_data_files();
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
            let initial_sampler = stream.analyze_sampler_state().ok_or(
                IcebergError::InvariantViolated(
                    "ANALYZE ReadStream is missing valid PG17 BlockSampler state",
                ),
            )?;
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
            let completed_sampler = stream.analyze_sampler_state().ok_or(
                IcebergError::InvariantViolated(
                    "ANALYZE ReadStream lost its PG17 BlockSampler state",
                ),
            )?;
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
            let target_rows = initial_sampler.target_rows();
            self.phase = AnalyzeScanPhase::Ready(preparation.start(
                tickets,
                target_rows,
                seed,
            )?);
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
