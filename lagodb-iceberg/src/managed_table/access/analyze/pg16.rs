//! PostgreSQL 16 ANALYZE sampling over the per-block callback protocol.
//!
//! PostgreSQL 16 passes one block number per callback and keeps both its
//! `BlockSampler` and `targrows` private, so the ticket count stays unknown
//! until the scan ends. The first ticket therefore emits every candidate with
//! zero weight. Each ticket then reports the same live-row share, which makes
//! PostgreSQL's `liverows / visited_blocks * total_blocks` come out to the
//! visible fraction of the physical rows however many tickets it visited.
//!
//! With no LagoDB PG16 ANALYZE executor/fork, this state receives a
//! relation-local sample budget rather than the invocation's exact `targrows`.
//! A non-inherited built-in ANALYZE remains statistically valid; a column-list
//! invocation can do excess candidate work. For inherited ANALYZE, PostgreSQL
//! computes `childtargrows` from the parent's selected statistics and child
//! sizes, so a smaller child-local budget can under-represent that child in the
//! parent's combined MCV and histogram sample. The live-row correction keeps
//! the child's row-count estimate valid but cannot repair that sample mixture.

use std::mem;

use lagodb_core::prelude::*;
use pgrx::pg_sys;
use rand::Rng;

use super::AnalyzePreparation;
use super::cursor::AnalyzeBatchCursor;

impl AnalyzePreparation {
    fn emit_all_candidates(self, sample_budget: u64) -> AmResult<AnalyzeBatchCursor> {
        let seed = rand::rng().random::<u64>();
        self.open_cursor(sample_budget, 1, seed, |_| 0.0)
    }
}

/// Aggregate state for the two PostgreSQL ANALYZE callbacks.
pub(crate) struct AnalyzeScanState {
    virtual_blocks: u64,
    phase: AnalyzeScanPhase,
}

enum AnalyzeScanPhase {
    Pending {
        preparation: AnalyzePreparation,
        sample_budget: u64,
    },
    /// The first ticket is emitting every candidate.
    Emitting {
        cursor: AnalyzeBatchCursor,
        physical_rows: u64,
        visible_rows: u64,
    },
    /// All candidates are out; tickets only report their live-row share.
    Drained {
        live_delta: f64,
    },
    Transitioning,
}

impl AnalyzeScanState {
    pub(crate) fn pending(
        preparation: AnalyzePreparation,
        sample_budget: u64,
    ) -> Self {
        Self {
            virtual_blocks: preparation.virtual_blocks,
            phase: AnalyzeScanPhase::Pending {
                preparation,
                sample_budget,
            },
        }
    }

    /// The block number is opaque: PostgreSQL's `BlockSampler` draws it below
    /// the relation size this AM reported, and each block is one ticket.
    pub(crate) fn next_block(
        &mut self,
        _block: pg_sys::BlockNumber,
    ) -> AmResult<bool> {
        match mem::replace(&mut self.phase, AnalyzeScanPhase::Transitioning) {
            AnalyzeScanPhase::Pending {
                preparation,
                sample_budget,
            } => {
                let physical_rows = preparation.population.physical_rows;
                let mut cursor = preparation.emit_all_candidates(sample_budget)?;
                cursor.next_ticket();
                self.phase = AnalyzeScanPhase::Emitting {
                    cursor,
                    physical_rows,
                    visible_rows: 0,
                };
                Ok(true)
            }
            AnalyzeScanPhase::Drained { live_delta } => {
                self.phase = AnalyzeScanPhase::Drained { live_delta };
                Ok(true)
            }
            AnalyzeScanPhase::Emitting { .. } | AnalyzeScanPhase::Transitioning => {
                unreachable!(
                    "PostgreSQL drains a block's tuples before requesting the next block"
                )
            }
        }
    }

    pub(crate) fn next_tuple(
        &mut self,
        out: &mut SlotDatumWriter<'_>,
    ) -> AmResult<AnalyzeTupleOutcome> {
        match &mut self.phase {
            AnalyzeScanPhase::Emitting {
                cursor,
                physical_rows,
                visible_rows,
            } => {
                let outcome = cursor.next_tuple(out)?;
                if outcome.found {
                    *visible_rows += 1;
                    return Ok(outcome);
                }
                let live_delta = Self::live_delta(
                    *physical_rows,
                    self.virtual_blocks,
                    *visible_rows,
                    cursor.candidate_count(),
                );
                self.phase = AnalyzeScanPhase::Drained { live_delta };
                Ok(AnalyzeTupleOutcome::end_of_block_with_live_delta(
                    live_delta,
                ))
            }
            AnalyzeScanPhase::Drained { live_delta } => Ok(
                AnalyzeTupleOutcome::end_of_block_with_live_delta(*live_delta),
            ),
            AnalyzeScanPhase::Pending { .. } | AnalyzeScanPhase::Transitioning => {
                unreachable!(
                    "PostgreSQL requests ANALYZE tuples only after an accepted block"
                )
            }
        }
    }

    /// One ticket's share of the estimated live rows.
    fn live_delta(
        physical_rows: u64,
        virtual_blocks: u64,
        visible_rows: u64,
        candidate_count: u32,
    ) -> f64 {
        if candidate_count == 0 {
            return 0.0;
        }
        (physical_rows as f64 / virtual_blocks as f64)
            * (visible_rows as f64 / f64::from(candidate_count))
    }
}
