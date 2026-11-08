//! PostgreSQL 17/18 ANALYZE sampling over the `ReadStream` block protocol.
//!
//! The first block callback drains the stream, so the ticket count and
//! `targrows` are known before any data file is selected.

use std::mem;

use lagodb_core::prelude::*;
use rand::Rng;

use super::AnalyzePreparation;
use super::cursor::AnalyzeBatchCursor;
use crate::error::IcebergError;

impl AnalyzePreparation {
    /// PostgreSQL selects `min(virtual_blocks, target_rows)` tickets, which
    /// either covers every virtual block or is `target_rows` itself. Both
    /// cases request `target_rows` candidates, the size PostgreSQL samples.
    fn start(
        self,
        tickets: AnalyzeTickets,
        seed: u64,
    ) -> AmResult<AnalyzeBatchCursor> {
        let physical_rows = self.population.physical_rows;
        let virtual_blocks = self.virtual_blocks;
        let ticket_count = tickets.tickets();
        self.open_cursor(
            tickets.target_rows(),
            ticket_count,
            seed,
            |candidate_count| {
                if candidate_count == 0 {
                    0.0
                } else {
                    (ticket_count as f64 / virtual_blocks as f64)
                        * (physical_rows as f64 / f64::from(candidate_count))
                }
            },
        )
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
        match &mut self.phase {
            AnalyzeScanPhase::Ready(cursor) => Ok(cursor.next_ticket()),
            AnalyzeScanPhase::Finished => Ok(false),
            AnalyzeScanPhase::Pending(_) => self.begin_sampling(stream),
            AnalyzeScanPhase::Transitioning => unreachable!(
                "a failed ANALYZE transition raises ERROR before PostgreSQL requests another block"
            ),
        }
    }

    /// Drain PostgreSQL's tickets on the first block callback and open the
    /// sample they select.
    fn begin_sampling(&mut self, stream: &AnalyzeReadStreamHandle) -> AmResult<bool> {
        let AnalyzeScanPhase::Pending(preparation) =
            mem::replace(&mut self.phase, AnalyzeScanPhase::Transitioning)
        else {
            unreachable!("only the Pending phase begins ANALYZE sampling");
        };
        let tickets = stream.drain_tickets()?;
        // Live-row weights scale by `virtual_blocks` while PostgreSQL scales
        // by its block count; relation_size reported both from one snapshot.
        if tickets.population_blocks() != preparation.virtual_blocks {
            return Err(IcebergError::InvariantViolated(
                "ANALYZE virtual blocks differ from the relation size PostgreSQL sampled",
            )
            .into());
        }
        if tickets.tickets() == 0 {
            self.phase = AnalyzeScanPhase::Finished;
            return Ok(false);
        }
        let seed = rand::rng().random::<u64>();
        let mut cursor = preparation.start(tickets, seed)?;
        let accepted = cursor.next_ticket();
        self.phase = AnalyzeScanPhase::Ready(cursor);
        Ok(accepted)
    }

    pub(crate) fn next_tuple(
        &mut self,
        out: &mut SlotDatumWriter<'_>,
    ) -> AmResult<AnalyzeTupleOutcome> {
        match &mut self.phase {
            AnalyzeScanPhase::Ready(cursor) => cursor.next_tuple(out),
            AnalyzeScanPhase::Finished => Ok(AnalyzeTupleOutcome::end_of_block()),
            AnalyzeScanPhase::Pending(_) | AnalyzeScanPhase::Transitioning => {
                unreachable!(
                    "PostgreSQL requests ANALYZE tuples only after an accepted block"
                )
            }
        }
    }
}
