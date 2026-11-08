//! Delete-aware ANALYZE read plan and slot-writing cursor.

use std::cmp::Ordering;
use std::iter::Enumerate;
use std::ops::Range;
use std::vec;

use iceberg_lite::arrow::SelectedRowsReadRequest;
use iceberg_lite::scan::{FileScanTask, TableScan};
use lagodb_arrow::ArrowColumnDecoder;
use lagodb_core::prelude::*;

use super::returned::ReturnedRows;
use super::sampling::SampledPosition;
use crate::error::{IcebergError, IcebergResult};
use crate::scan::batch::InterruptibleArrowBatches;

const TUPLES_PER_SYNTHETIC_BLOCK: u64 = 2048;

pub(super) enum AnalyzeReadPlan {
    Selected {
        requests: Vec<SelectedRowsReadRequest>,
        files: Vec<ExpectedFile>,
    },
    Full {
        tasks: Vec<FileScanTask>,
        files: Vec<ExpectedFile>,
    },
}

impl AnalyzeReadPlan {
    /// # Safety
    /// `decoder` must be built for the projection of `scan`.
    pub(super) unsafe fn open(
        self,
        scan: &TableScan,
        decoder: ArrowColumnDecoder,
    ) -> IcebergResult<(ReturnedRows, ExpectedCursor)> {
        let (batches, files) = match self {
            Self::Selected { requests, files } => {
                (scan.to_arrow_with_selected_rows(requests)?, files)
            }
            Self::Full { tasks, files } => {
                (scan.to_arrow_with_validated_tasks(tasks)?, files)
            }
        };
        let (paths, positions) = files
            .into_iter()
            .map(|file| (file.path, file.positions))
            .unzip();
        let source = InterruptibleArrowBatches(batches);
        // SAFETY: the caller pairs `decoder` with this scan's projection.
        let returned = unsafe { ReturnedRows::new(source, decoder, paths) };
        Ok((returned, ExpectedCursor::new(positions)))
    }
}

/// Merges the planned candidates with the reader's visible rows and splits
/// the candidate ordinals across PostgreSQL's sampling tickets.
pub(super) struct AnalyzeBatchCursor {
    returned: ReturnedRows,
    expected: ExpectedCursor,
    tickets: u64,
    next_ticket: u64,
    ticket_end: u64,
    candidate_count: u64,
    live_weight: f64,
}

impl AnalyzeBatchCursor {
    pub(super) fn new(
        returned: ReturnedRows,
        expected: ExpectedCursor,
        tickets: u64,
        candidate_count: u64,
        live_weight: f64,
    ) -> Self {
        Self {
            returned,
            expected,
            tickets,
            next_ticket: 0,
            ticket_end: 0,
            candidate_count,
            live_weight,
        }
    }

    #[cfg(feature = "pg16")]
    pub(super) const fn candidate_count(&self) -> u64 {
        self.candidate_count
    }

    pub(super) fn next_ticket(&mut self) -> AmResult<bool> {
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

    pub(super) fn next_tuple(
        &mut self,
        out: &mut SlotDatumWriter<'_>,
    ) -> AmResult<AnalyzeTupleOutcome> {
        while let Some(expected) = self.expected.peek_before(self.ticket_end) {
            match self.returned.current()? {
                Some(returned) => match returned.identity().cmp(&expected) {
                    Ordering::Less => {
                        return Err(IcebergError::InvariantViolated(
                            "ANALYZE reader returned rows out of planned order",
                        )
                        .into());
                    }
                    Ordering::Equal => {
                        let ordinal = self.expected.ordinal();
                        returned.write(out)?;
                        out.set_tid(&Self::synthetic_tid(ordinal)?);
                        if self.expected.advance() {
                            self.returned.consume();
                        }
                        return Ok(AnalyzeTupleOutcome::visible(self.live_weight));
                    }
                    // The reader is past this candidate: it was deleted.
                    Ordering::Greater => {
                        self.expected.advance();
                    }
                },
                // The reader is exhausted: every remaining candidate was deleted.
                None => {
                    self.expected.advance();
                }
            }
        }

        if self.next_ticket == self.tickets {
            self.returned.finish()?;
        }
        Ok(AnalyzeTupleOutcome::end_of_block())
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct RowIdentity {
    pub(super) file_index: usize,
    pub(super) position: u64,
}

pub(super) struct ExpectedFile {
    pub(super) path: Box<str>,
    pub(super) positions: ExpectedPositions,
}

/// One planned file's candidate positions, in ascending order.
pub(super) enum ExpectedPositions {
    Selected(vec::IntoIter<SampledPosition>),
    All(Range<u64>),
}

impl Iterator for ExpectedPositions {
    type Item = SampledPosition;

    fn next(&mut self) -> Option<SampledPosition> {
        match self {
            Self::Selected(positions) => positions.next(),
            Self::All(positions) => {
                positions.next().map(|position| SampledPosition {
                    position,
                    multiplicity: 1,
                })
            }
        }
    }
}

/// Planned candidate observations in reader order. A physical row sampled
/// with replacement stays at the head for each of its observations.
///
/// The sampler assigns exactly `candidate_count` observations, so the head
/// exists whenever the ordinal is below a ticket boundary.
pub(super) struct ExpectedCursor {
    files: Enumerate<vec::IntoIter<ExpectedPositions>>,
    file_index: usize,
    positions: ExpectedPositions,
    head: Option<RowIdentity>,
    /// Observations of `head` not yet consumed.
    remaining: u64,
    ordinal: u64,
}

impl ExpectedCursor {
    fn new(files: Vec<ExpectedPositions>) -> Self {
        let mut cursor = Self {
            files: files.into_iter().enumerate(),
            file_index: 0,
            positions: ExpectedPositions::All(0..0),
            head: None,
            remaining: 0,
            ordinal: 0,
        };
        cursor.load_head();
        cursor
    }

    /// The next observation, unless the ticket ending at `ticket_end` is full.
    fn peek_before(&self, ticket_end: u64) -> Option<RowIdentity> {
        if self.ordinal < ticket_end {
            self.head
        } else {
            None
        }
    }

    const fn ordinal(&self) -> u64 {
        self.ordinal
    }

    /// Consume one observation of the head. Returns true when its physical
    /// row has no remaining observations and the reader may advance.
    fn advance(&mut self) -> bool {
        self.ordinal += 1;
        self.remaining -= 1;
        if self.remaining != 0 {
            return false;
        }
        self.load_head();
        true
    }

    fn load_head(&mut self) {
        loop {
            if let Some(sampled) = self.positions.next() {
                self.head = Some(RowIdentity {
                    file_index: self.file_index,
                    position: sampled.position,
                });
                self.remaining = sampled.multiplicity;
                return;
            }
            let Some((file_index, positions)) = self.files.next() else {
                self.head = None;
                return;
            };
            self.file_index = file_index;
            self.positions = positions;
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expected_cursor_repeats_one_physical_position_without_reordering() {
        let mut cursor = ExpectedCursor::new(vec![ExpectedPositions::Selected(
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
            .into_iter(),
        )]);

        assert_eq!(cursor.peek_before(u64::MAX).unwrap().position, 3);
        assert!(!cursor.advance());
        assert_eq!(cursor.peek_before(u64::MAX).unwrap().position, 3);
        assert!(cursor.peek_before(cursor.ordinal()).is_none());
        assert!(cursor.advance());
        assert_eq!(cursor.peek_before(u64::MAX).unwrap().position, 8);
        assert!(cursor.advance());
        assert!(cursor.peek_before(u64::MAX).is_none());
        assert_eq!(cursor.ordinal(), 3);
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
