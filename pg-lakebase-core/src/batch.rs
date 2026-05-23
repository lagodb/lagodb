//! Batch buffering abstractions for DML write paths.
//!
//! The core crate defines the buffering lifecycle without committing every
//! access method to a concrete storage layout. Row-oriented implementations can
//! use [`RowBatchBuffer`], while columnar access methods can append PostgreSQL
//! datum views through [`SlotColumnarBatchBuffer`] without pulling Arrow or
//! another columnar format into core.
//!
//! The split is deliberate:
//!
//! - [`RowBatchBuffer`] owns `Row` values and is safe across callback
//!   boundaries.
//! - [`SlotColumnarBatchBuffer`] consumes `TupleSlotRow` / `PgDatumRef` views
//!   during the callback and must copy anything it needs into its own builders.
//!
//! Core does not provide a `Cell`-based columnar buffer. For DML hot paths,
//! materializing `Cell` first would add the same intermediate allocations that
//! the slot/datum path is designed to avoid.

use std::convert::Infallible;

use crate::tuple::{PgDatumRef, Row, TupleSlotRow};

/// Common lifecycle for buffering AM-specific batches.
///
/// This trait intentionally does not define an append method. Row buffers,
/// slot/datum columnar buffers, and future batch strategies have different
/// source ownership rules, but they share flush, clear, length, and memory
/// accounting behavior.
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

    /// Returns `true` when the buffer's estimated in-memory footprint has
    /// reached `max_bytes` and the caller should flush.
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

/// Per-column encoder for direct PostgreSQL datum sources.
///
/// This is the fast path for Arrow-backed DML buffers: the concrete appender
/// can inspect PostgreSQL type metadata and append directly into its physical
/// builder without first allocating a materialized cell. Target-format
/// decisions such as Iceberg decimal scale, fixed-width binary length,
/// timestamp representation, JSON encoding, and list nullability live in the
/// concrete appender.
pub trait DatumColumnAppender {
    /// Finished physical column, such as an Arrow `ArrayRef`.
    type Column;
    /// Error raised while encoding or finishing this column.
    type Error;

    /// Append one PostgreSQL datum view to this output column.
    ///
    /// `None` represents a missing value for this column index. SQL NULL values
    /// are represented by `Some(value)` where `value.is_null()` is true.
    fn append_datum(
        &mut self,
        value: Option<PgDatumRef<'_>>,
    ) -> Result<(), Self::Error>;

    /// Finish the current column values and reset the appender for reuse.
    fn finish(&mut self) -> Result<Self::Column, Self::Error>;

    /// Drop buffered values from the current in-progress batch.
    fn clear(&mut self);

    /// Number of values appended for the current batch.
    fn len(&self) -> usize;

    /// Estimated memory footprint of encoded column data in bytes.
    fn estimated_size(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Extension point for AM-owned columnar buffers fed directly from PostgreSQL
/// tuple slots.
///
/// This is the abstraction a future Arrow companion crate should implement for
/// the DML hot path. It keeps core independent of Arrow while avoiding the
/// intermediate row/cell allocation layer.
pub trait SlotColumnarBatchBuffer: BatchBuffer {
    /// Finished physical column type.
    type Column;

    /// Number of physical columns in the target batch.
    fn column_count(&self) -> usize;

    /// Append one PostgreSQL datum view to the column at `column_index`.
    fn append_datum_to_column(
        &mut self,
        column_index: usize,
        value: Option<PgDatumRef<'_>>,
    ) -> Result<(), Self::Error>;

    /// Complete the current logical row after all column datums were appended.
    fn finish_row(&mut self) -> Result<(), Self::Error>;

    /// Finish all columns for the current batch.
    fn finish_columns(&mut self) -> Result<Vec<Self::Column>, Self::Error>;

    /// Append a PostgreSQL tuple-slot row through the datum interface.
    fn append_slot_row(&mut self, row: TupleSlotRow<'_>) -> Result<(), Self::Error> {
        for column_index in 0..self.column_count() {
            self.append_datum_to_column(column_index, row.datum_at(column_index))?;
        }
        self.finish_row()
    }
}

/// Append a tuple-slot row to a homogenous set of datum column appenders.
pub fn append_slot_row_to_datum_columns<A>(
    columns: &mut [A],
    row: TupleSlotRow<'_>,
) -> Result<(), A::Error>
where
    A: DatumColumnAppender,
{
    for (column_index, column) in columns.iter_mut().enumerate() {
        column.append_datum(row.datum_at(column_index))?;
    }
    Ok(())
}

/// Row-oriented batch buffer backed by owned [`Row`] values.
///
/// This is the core default for AMs that want to keep row-shaped batches. Use
/// [`Self::push_row`] when ownership is already available. [`Self::copy_row`]
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
        self.estimated_size += row.size;
        self.rows.push(row);
    }

    pub fn copy_row(&mut self, row: &Row) {
        self.push_row(row.clone());
    }

    pub fn push_slot_row(&mut self, row: TupleSlotRow<'_>) {
        self.push_row(row.to_owned_row());
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
    use crate::tuple::Cell;

    #[test]
    fn row_batch_tracks_size_and_rows() {
        let mut row = Row::with_capacity(2);
        row.set_cell(0, Some(Cell::I32(7)));
        row.set_cell(1, Some(Cell::String("lakebase".to_string())));
        let row_size = row.size;

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
        row.push(Some(Cell::I64(42)));

        let mut buffer = RowBatchBuffer::with_capacity(1);
        buffer.push_row(row);

        assert_eq!(buffer.rows().len(), 1);
        assert_eq!(buffer.len(), 1);
    }
}
