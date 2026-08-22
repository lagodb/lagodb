//! Delete-aware ANALYZE read plan and slot-writing cursor.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::rc::Rc;

use arrow_array::{Int64Array, RecordBatch};
use iceberg_lite::arrow::SelectedRowsReadRequest;
use iceberg_lite::scan::{FileScanTask, TableScan};
use pg_arrow_conv::{ArrowBatchSource, ArrowColumnDecoder, BoundBatch};
use pg_lakebase_core::api::{AnalyzeTupleOutcome, TRIGGER_ROW_BLOCK_BASE};
use pg_lakebase_core::prelude::*;

use super::sampling::SampledPosition;
use crate::engine::scan::batch::{
    IcebergArrowBatchSource, IcebergArrowBatches, RowLocationLayout,
    position_unchecked,
};
use crate::error::{IcebergError, IcebergResult};

const TUPLES_PER_SYNTHETIC_BLOCK: u64 = 2048;

pub(super) enum AnalyzeReadPlan {
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
    pub(super) fn open(
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

struct AnalyzeBoundBatch {
    decoded: BoundBatch,
    positions: Int64Array,
    file_index: usize,
}

pub(super) struct AnalyzeBatchCursor {
    source: IcebergArrowBatchSource,
    decoder: ArrowColumnDecoder,
    current: Option<AnalyzeBoundBatch>,
    row_location_layout: Option<RowLocationLayout>,
    row_index: usize,
    expected: ExpectedCursor,
    file_indices: HashMap<Rc<str>, usize>,
    /// Batch-level fast path for consecutive batches from the same file.
    /// Selected-row reads can legally produce one-row batches, so retaining
    /// this avoids replacing the old string comparison with a hash per row.
    last_bound_file: Option<(Rc<str>, usize)>,
    tickets: u64,
    next_ticket: u64,
    ticket_end: u64,
    candidate_count: u64,
    live_weight: f64,
}

impl AnalyzeBatchCursor {
    pub(super) fn try_new(
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
            row_location_layout: None,
            row_index: 0,
            expected,
            file_indices,
            last_bound_file: None,
            tickets,
            next_ticket: 0,
            ticket_end: 0,
            candidate_count,
            live_weight,
        })
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
                return Ok(Some(RowIdentity {
                    file_index: batch.file_index,
                    // SAFETY: Iceberg's row-number producer supplies non-null,
                    // non-negative positions before publishing the batch.
                    position: unsafe {
                        position_unchecked(&batch.positions, self.row_index)
                    },
                }));
            }

            self.current = None;
            let Some(batch) = self.source.next_batch()? else {
                return Ok(None);
            };
            let Some(bound) = self.bind_batch(batch)? else {
                continue;
            };
            self.current = Some(bound);
            self.row_index = 0;
        }
    }

    fn bind_batch(
        &mut self,
        batch: RecordBatch,
    ) -> AmResult<Option<AnalyzeBoundBatch>> {
        let layout = match self.row_location_layout {
            Some(layout) => layout,
            None => {
                let layout = RowLocationLayout::try_new(&batch)?;
                self.row_location_layout = Some(layout);
                layout
            }
        };
        // SAFETY: this is the Iceberg reader projection used to create the
        // stable layout; `bind` handles empty batches without reading `_file`.
        let locations = unsafe { layout.bind(&batch) }?;
        let Some(locations) = locations else {
            // Preserve the old empty-batch schema validation without creating
            // a row-bearing ANALYZE state.
            let _ = self.decoder.bind(batch)?;
            return Ok(None);
        };
        let path = locations.file_path();
        let cached_file_index = if let Some((cached_path, index)) =
            self.last_bound_file.as_ref()
            && cached_path.as_ref() == path
        {
            Some(*index)
        } else {
            None
        };
        let resolved_file_index =
            cached_file_index.or_else(|| self.file_indices.get(path).copied());
        let positions = locations.into_positions();

        // Preserve the old validation order: decoded columns are bound before
        // ANALYZE reports a returned file outside its expected population.
        let decoded = self.decoder.bind(batch)?;
        let file_index =
            resolved_file_index.ok_or(IcebergError::InvariantViolated(
                "ANALYZE reader returned an unplanned data file",
            ))?;
        if cached_file_index.is_none() {
            let expected_path = self
                .expected
                .files
                .get(file_index)
                .ok_or(IcebergError::InvariantViolated(
                    "ANALYZE file index is outside its expected population",
                ))?
                .path
                .clone();
            self.last_bound_file = Some((expected_path, file_index));
        }
        Ok(Some(AnalyzeBoundBatch {
            decoded,
            positions,
            file_index,
        }))
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
        // SAFETY: ANALYZE receives the same relation-bound decoder plan as the
        // scan cursor; its destinations were validated against that slot
        // layout while the plan was constructed.
        unsafe {
            self.decoder
                .write_row_unchecked(&batch.decoded, self.row_index, out)
        }?;
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

pub(super) struct ExpectedCursor {
    files: Box<[ExpectedFile]>,
    file_index: usize,
    position_index: u64,
    repetition_index: u64,
    ordinal: u64,
}

pub(super) struct ExpectedFile {
    pub(super) path: Rc<str>,
    pub(super) positions: ExpectedPositions,
}

pub(super) enum ExpectedPositions {
    Selected(Box<[SampledPosition]>),
    All(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RowIdentity {
    file_index: usize,
    position: u64,
}

impl ExpectedCursor {
    pub(super) fn new(files: Vec<ExpectedFile>) -> Self {
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
    fn expected_cursor_repeats_one_physical_position_without_reordering() {
        let mut cursor = ExpectedCursor::new(vec![ExpectedFile {
            path: Rc::from("data.parquet"),
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
