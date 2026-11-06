//! Visible rows returned by the delete-aware ANALYZE reader.

use arrow_array::{Int64Array, RecordBatch};
use lagodb_arrow::{ArrowColumnDecoder, BoundBatch};
use lagodb_core::prelude::*;

use super::cursor::RowIdentity;
use crate::error::{IcebergError, IcebergResult};
use crate::scan::batch::{AnalyzeBatchSource, RowLocationLayout, position_unchecked};

/// Reader output in planned `(file, position)` order.
///
/// iceberg-lite reads file requests sequentially, so batches arrive in plan
/// order and positions ascend within each file. Rows removed by deletes are
/// simply absent, which makes this a subsequence of the planned candidates.
pub(super) struct ReturnedRows {
    source: AnalyzeBatchSource,
    decoder: ArrowColumnDecoder,
    layout: Option<RowLocationLayout>,
    /// Planned data-file paths; `file_index` only moves forward over them.
    planned_paths: Box<[Box<str>]>,
    file_index: usize,
    batch: Option<ReturnedBatch>,
    row: usize,
}

struct ReturnedBatch {
    decoded: BoundBatch,
    positions: Int64Array,
}

/// The reader's current row; valid until the reader advances.
pub(super) struct ReturnedRow<'a> {
    decoder: &'a ArrowColumnDecoder,
    batch: &'a ReturnedBatch,
    file_index: usize,
    row: usize,
}

impl ReturnedRows {
    /// # Safety
    /// `source` must emit the projected schema used to construct `decoder`.
    pub(super) unsafe fn new(
        source: AnalyzeBatchSource,
        decoder: ArrowColumnDecoder,
        planned_paths: Vec<Box<str>>,
    ) -> Self {
        Self {
            source,
            decoder,
            layout: None,
            planned_paths: planned_paths.into_boxed_slice(),
            file_index: 0,
            batch: None,
            row: 0,
        }
    }

    /// The reader's current row, loading batches as needed; `None` at EOF.
    pub(super) fn current(&mut self) -> AmResult<Option<ReturnedRow<'_>>> {
        while !self.has_row() {
            self.batch = None;
            let Some(batch) = self.source.next_batch()? else {
                return Ok(None);
            };
            self.batch = self.bind(batch)?;
            self.row = 0;
        }
        Ok(self.batch.as_ref().map(|batch| ReturnedRow {
            decoder: &self.decoder,
            batch,
            file_index: self.file_index,
            row: self.row,
        }))
    }

    pub(super) fn consume(&mut self) {
        self.row += 1;
    }

    /// Drive the reader to EOF and reject any row the plan did not request.
    /// A full read opens its trailing zero-row files here, which runs their
    /// footer record-count validation.
    pub(super) fn finish(&mut self) -> AmResult<()> {
        if self.current()?.is_some() {
            return Err(IcebergError::InvariantViolated(
                "ANALYZE reader returned an unrequested physical row",
            )
            .into());
        }
        Ok(())
    }

    fn has_row(&self) -> bool {
        self.batch
            .as_ref()
            .is_some_and(|batch| self.row < batch.decoded.num_rows())
    }

    fn bind(&mut self, batch: RecordBatch) -> AmResult<Option<ReturnedBatch>> {
        let layout = match self.layout {
            Some(layout) => layout,
            None => *self.layout.insert(RowLocationLayout::try_new(&batch)?),
        };
        // SAFETY: this is the Iceberg reader projection used to create the
        // stable layout; `bind` handles empty batches without reading `_file`.
        let Some(locations) = unsafe { layout.bind(&batch) }? else {
            return Ok(None);
        };
        self.seek_file(locations.file_path())?;
        let positions = locations.into_positions();
        // SAFETY: the constructor pairs the decoder with this scan source;
        // selected-row and full reads both use iceberg-lite's schema conversion.
        let decoded = unsafe { self.decoder.bind_batch_unchecked(batch) }?;
        Ok(Some(ReturnedBatch { decoded, positions }))
    }

    /// Map a batch to its planned file. The search starts at the current
    /// file, so consecutive batches of one file cost a single comparison.
    fn seek_file(&mut self, path: &str) -> IcebergResult<()> {
        let offset = self.planned_paths[self.file_index..]
            .iter()
            .position(|planned| planned.as_ref() == path)
            .ok_or(IcebergError::InvariantViolated(
                "ANALYZE reader returned a data file outside the planned order",
            ))?;
        self.file_index += offset;
        Ok(())
    }
}

impl ReturnedRow<'_> {
    pub(super) fn identity(&self) -> RowIdentity {
        RowIdentity {
            file_index: self.file_index,
            // SAFETY: `row` is within the batch, and Iceberg's row-number
            // producer supplies non-null, non-negative positions before
            // publishing it.
            position: unsafe { position_unchecked(&self.batch.positions, self.row) },
        }
    }

    pub(super) fn write(&self, out: &mut SlotDatumWriter<'_>) -> AmResult<()> {
        // SAFETY: ANALYZE receives the same relation-bound decoder plan as the
        // scan cursor; its destinations were validated against that slot
        // layout while the plan was constructed.
        unsafe {
            self.decoder
                .write_row_unchecked(&self.batch.decoded, self.row, out)
        }?;
        Ok(())
    }
}
