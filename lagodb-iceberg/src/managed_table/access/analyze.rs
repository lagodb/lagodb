//! Iceberg-aware PostgreSQL `ANALYZE` sampling.
//!
//! PostgreSQL exposes a block-oriented callback, while Iceberg's stable
//! physical population is rows in planned data files. This module treats
//! PostgreSQL's blocks as sampling tickets, selects a bounded number of data
//! files, and samples their physical rows through iceberg-lite's normal
//! delete-aware pipeline. The block protocol differs by PostgreSQL major, so
//! each one owns its callback state machine.

mod block_domain;
mod cursor;
#[cfg(feature = "pg16")]
mod pg16;
mod population;
#[cfg(any(feature = "pg17", feature = "pg18"))]
mod read_stream;
mod returned;
mod sampling;

pub(crate) use block_domain::AnalyzeBlockDomain;
#[cfg(feature = "pg16")]
pub(crate) use pg16::AnalyzeScanState;
pub(crate) use population::AnalyzePopulation;
#[cfg(any(feature = "pg17", feature = "pg18"))]
pub(crate) use read_stream::AnalyzeScanState;

use iceberg_lite::scan::TableScan;
use lagodb_arrow::ArrowColumnDecoder;
use lagodb_core::prelude::*;
use rand::SeedableRng;
use rand::rngs::StdRng;

use self::cursor::AnalyzeBatchCursor;
use crate::error::{IcebergError, IcebergResult};
use crate::managed_table::gucs::analyze_max_data_files;
use crate::scan::AnalyzeScanInput;

/// Deferred ANALYZE plan. PostgreSQL's ticket count is not available until
/// the first `scan_analyze_next_block` callback.
pub(crate) struct AnalyzePreparation {
    scan: TableScan,
    population: AnalyzePopulation,
    decoder: ArrowColumnDecoder,
    virtual_blocks: u64,
}
impl AnalyzePreparation {
    pub(crate) fn try_new(input: AnalyzeScanInput) -> IcebergResult<Self> {
        let AnalyzeScanInput {
            scan,
            tasks,
            decoder,
            storage_bytes,
        } = input;
        let population = AnalyzePopulation::try_new(tasks)?;
        let virtual_blocks = AnalyzeBlockDomain::blocks(storage_bytes);
        // With zero blocks PostgreSQL samples nothing and records the
        // relation as empty, so a non-empty population must not proceed.
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

    /// Select up to `requested_candidates` rows and open a reader that splits
    /// them across `tickets` callbacks. `live_weight` receives the number of
    /// candidates actually selected and returns each visible row's weight.
    fn open_cursor(
        self,
        requested_candidates: u64,
        tickets: u64,
        seed: u64,
        live_weight: impl FnOnce(u32) -> f64,
    ) -> AmResult<AnalyzeBatchCursor> {
        let mut rng = StdRng::seed_from_u64(seed);
        let max_data_files = analyze_max_data_files();
        let sample = self.population.locality_plan(
            requested_candidates,
            max_data_files,
            &mut rng,
        )?;
        let weight = live_weight(sample.candidate_count);
        // SAFETY: AnalyzeScanInput supplies the scan and decoder from one
        // PgReadPlan. Sampling changes selected rows, not the scan projection.
        let (returned, expected) =
            unsafe { sample.read_plan.open(&self.scan, self.decoder) }?;
        Ok(AnalyzeBatchCursor::new(
            returned,
            expected,
            tickets,
            sample.candidate_count,
            weight,
        ))
    }
}
