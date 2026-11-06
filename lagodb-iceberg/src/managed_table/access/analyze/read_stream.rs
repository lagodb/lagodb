//! PostgreSQL 17/18 ANALYZE sampling over the `ReadStream` block protocol.
//!
//! The first block callback drains the stream, so the ticket count and
//! `targrows` are known before any data file is selected.

use std::mem;

use lagodb_core::prelude::*;
use rand::Rng;

use super::AnalyzePreparation;
use super::cursor::AnalyzeBatchCursor;
use super::sampling::AnalyzeSampler;
use crate::error::IcebergError;

impl AnalyzePreparation {
    fn start(
        self,
        tickets: u64,
        target_rows: u64,
        seed: u64,
    ) -> AmResult<AnalyzeBatchCursor> {
        let physical_rows = self.population.physical_rows;
        let virtual_blocks = self.virtual_blocks;
        let desired_candidates = AnalyzeSampler::desired_candidate_count(
            physical_rows,
            virtual_blocks,
            tickets,
            target_rows,
        )?;
        self.open_cursor(desired_candidates, tickets, seed, |candidate_count| {
            if candidate_count == 0 {
                0.0
            } else {
                (tickets as f64 / virtual_blocks as f64)
                    * (physical_rows as f64 / candidate_count as f64)
            }
        })
    }
}

/// Aggregate state for the two PostgreSQL ANALYZE callbacks.
pub(crate) struct AnalyzeScanState {
    phase: AnalyzeScanPhase,
}

enum AnalyzeScanPhase {
    Pending(AnalyzePreparation),
    Ready(AnalyzeBatchCursor),
    Transitioning,
    Finished,
}

impl AnalyzeScanState {
    pub(crate) fn pending(preparation: AnalyzePreparation) -> Self {
        Self {
            phase: AnalyzeScanPhase::Pending(preparation),
        }
    }

    pub(crate) fn next_block(
        &mut self,
        stream: &AnalyzeReadStreamHandle,
    ) -> AmResult<bool> {
        if matches!(&self.phase, AnalyzeScanPhase::Pending(_)) {
            let phase =
                mem::replace(&mut self.phase, AnalyzeScanPhase::Transitioning);
            let AnalyzeScanPhase::Pending(preparation) = phase else {
                return Err(IcebergError::InvariantViolated(
                    "ANALYZE entered an invalid lifecycle transition",
                )
                .into());
            };
            let initial_sampler = stream.analyze_sampler_state().ok_or(
                IcebergError::InvariantViolated(
                    "ANALYZE ReadStream is missing valid PostgreSQL BlockSampler state",
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
                    "ANALYZE ReadStream lost its PostgreSQL BlockSampler state",
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
                        "ANALYZE BlockSampler state is inconsistent with consumed tickets",
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
            AnalyzeScanPhase::Pending(_) | AnalyzeScanPhase::Transitioning => {
                Err(IcebergError::InvariantViolated(
                    "ANALYZE state remained incomplete after initialization",
                )
                .into())
            }
        }
    }

    pub(crate) fn next_tuple(
        &mut self,
        out: &mut SlotDatumWriter<'_>,
    ) -> AmResult<AnalyzeTupleOutcome> {
        match &mut self.phase {
            AnalyzeScanPhase::Ready(cursor) => cursor.next_tuple(out),
            AnalyzeScanPhase::Finished => Ok(AnalyzeTupleOutcome::end_of_block()),
            AnalyzeScanPhase::Pending(_) | AnalyzeScanPhase::Transitioning => {
                Err(IcebergError::InvariantViolated(
                    "ANALYZE tuple callback ran before block initialization",
                )
                .into())
            }
        }
    }
}
