//! Write side: buffering lifecycle shared by row and columnar batch writers.

use std::convert::Infallible;

use crate::datum::DatumConversionError;
use crate::tuple::{Row, RowDatumCodec, TupleSlotRow};

/// Common lifecycle for buffering AM-specific batches.
///
/// This trait intentionally does not define an append method. Row buffers and
/// provider-owned columnar buffers have different source ownership rules, but
/// they share flush, clear, length, and memory accounting behavior.
pub trait BatchBuffer {
    /// The completed batch type consumed by the writer.
    type Batch;
    /// Error raised while appending rows or finishing the batch.
    type Error;

    /// Finish the current batch and reset the buffer for reuse.
    fn finish_batch(&mut self) -> Result<Self::Batch, Self::Error>;

    /// Drop all buffered data.
    fn clear(&mut self);

    /// Number of rows currently buffered.
    fn len(&self) -> usize;

    /// Estimated memory footprint of buffered row data in bytes.
    fn estimated_size(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` when the caller should flush the buffered batch.
    ///
    /// The default checks whether the estimated footprint has reached
    /// `max_bytes`. Implementations may also impose a row-count limit so
    /// values with little or no estimated payload still trigger a flush.
    ///
    /// This is a **memory-pressure** signal, not a target file-size signal:
    /// `estimated_size` reflects the Rust representation of buffered rows
    /// (cell sizes, owned bytes, etc.), which has only a loose relationship
    /// to the size of any file that may eventually be produced from those
    /// rows (Parquet, for example, is column-encoded, dictionary-compressed,
    /// and Snappy/ZSTD-compressed). Concrete output file sizing belongs to
    /// the writer layer (e.g. the rolling file writer's target file size).
    fn should_flush(&self, max_bytes: usize) -> bool {
        self.estimated_size() >= max_bytes
    }
}

/// Row-oriented batch buffer backed by owned [`Row`] values.
///
/// # Role: the buffering half of the row-world write path
///
/// This is the **row-world** write buffer, the counterpart to the
/// `Cell`-based conversion (`lagodb-arrow`'s `CellRule::build(&[Row], ..)`):
/// a row-mode access method or FDW accumulates owned `Row`s here (one slot at a
/// time, since `ExecForeignInsert` hands over one slot per call) and, on flush,
/// converts the buffered `&[Row]` to a columnar batch.
///
/// It is deliberately **not** on the columnar hot path. Columnar AMs (the
/// in-tree Iceberg AM) append tuple slots directly into a provider-owned
/// relation-bound buffer such as `lagodb-arrow`'s `BoundWriteBuffer`, which
/// skips the owned-`Row` materialization this type does. As a result the in-tree code
/// base does not drive `RowBatchBuffer` today — it is exercised by unit tests
/// and retained as the row-world buffering primitive a future row-mode FDW
/// will use. It is **not** dead/legacy code: removing it would leave the
/// row-world write path (which keeps `CellRule::build`) without its buffering
/// half. See the columnar-datapath-refactor design, goal #3 / §3.1.
///
/// Use [`Self::push_row`] when ownership is already available. [`Self::copy_row`]
/// is intentionally named to make deep row copies explicit in hot paths.
///
/// `push_slot_row()` materializes an owned `Row` from a slot view. That is the
/// correct fallback for row-mode batching because PostgreSQL slot memory cannot
/// be borrowed after the callback returns.
#[derive(Debug, Clone, Default)]
pub struct RowBatchBuffer {
    rows: Vec<Row>,
    estimated_size: usize,
}

impl RowBatchBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            rows: Vec::with_capacity(capacity),
            estimated_size: 0,
        }
    }

    pub fn push_row(&mut self, row: Row) {
        self.estimated_size += row.estimated_size();
        self.rows.push(row);
    }

    pub fn copy_row(&mut self, row: &Row) {
        self.push_row(row.clone());
    }

    /// # Safety
    ///
    /// `codec` must be bound to the same tuple descriptor as `row`'s slot.
    /// The slot must remain valid for the duration of materialization.
    pub unsafe fn push_slot_row(
        &mut self,
        row: TupleSlotRow<'_>,
        codec: &RowDatumCodec,
    ) -> Result<(), DatumConversionError> {
        self.push_row(unsafe { row.to_owned_row(codec) }?);
        Ok(())
    }

    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    pub fn take_rows(&mut self) -> Vec<Row> {
        self.estimated_size = 0;
        std::mem::take(&mut self.rows)
    }

    pub fn into_rows(self) -> Vec<Row> {
        self.rows
    }

    pub fn clear(&mut self) {
        self.rows.clear();
        self.estimated_size = 0;
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn estimated_size(&self) -> usize {
        self.estimated_size
    }

    /// Returns `true` when the estimated in-memory footprint of buffered rows
    /// has reached `max_bytes`.
    ///
    /// See [`BatchBuffer::should_flush`] for the semantic contract: this is a
    /// memory-pressure trigger, not a Parquet (or other output-format) file
    /// size trigger.
    pub fn should_flush(&self, max_bytes: usize) -> bool {
        self.estimated_size >= max_bytes
    }
}

impl BatchBuffer for RowBatchBuffer {
    type Batch = Vec<Row>;
    type Error = Infallible;

    fn finish_batch(&mut self) -> Result<Self::Batch, Self::Error> {
        Ok(self.take_rows())
    }

    fn clear(&mut self) {
        RowBatchBuffer::clear(self);
    }

    fn len(&self) -> usize {
        RowBatchBuffer::len(self)
    }

    fn estimated_size(&self) -> usize {
        RowBatchBuffer::estimated_size(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datum::Cell;

    #[test]
    fn row_batch_tracks_size_and_rows() {
        let mut row = Row::with_width(2);
        row.set_cell(0, Some(Cell::I32(7)));
        row.set_cell(1, Some(Cell::String("lagodb".to_string())));
        let row_size = row.estimated_size();

        let mut buffer = RowBatchBuffer::new();
        buffer.copy_row(&row);

        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.estimated_size(), row_size);
        assert!(buffer.should_flush(row_size));

        let rows = buffer.take_rows();
        assert_eq!(rows.len(), 1);
        assert!(buffer.is_empty());
        assert_eq!(buffer.estimated_size(), 0);
    }

    #[test]
    fn row_batch_accepts_owned_rows() {
        let mut row = Row::new();
        row.set_cell(0, Some(Cell::I64(42)));

        let mut buffer = RowBatchBuffer::with_capacity(1);
        buffer.push_row(row);

        assert_eq!(buffer.rows().len(), 1);
        assert_eq!(buffer.len(), 1);
    }
}
