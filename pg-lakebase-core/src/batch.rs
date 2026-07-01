//! Batch buffering and transport abstractions for the mutation write path and the
//! columnar scan read path.
//!
//! The core crate defines the buffering/transport *contracts* without
//! committing to a concrete storage layout or naming a columnar library — the
//! Arrow implementations live in `pg-arrow-conv`. core stays Arrow-agnostic and
//! the per-row path stays `dyn`-free (generics + enum dispatch), so the
//! abstraction is zero-cost.
//!
//! Two write worlds, deliberately split by how the source row is owned:
//!
//! - **Column world (hot path):** [`SlotColumnarBatchBuffer`] consumes
//!   `TupleSlotRow` / `PgDatumRef` views *during* the callback and copies what
//!   it needs straight into its column builders. This is what columnar AMs (the
//!   in-tree Iceberg AM) use; the Arrow implementation is `pg-arrow-conv`'s
//!   `SlotRecordBatchBuffer`.
//! - **Row world:** [`RowBatchBuffer`] owns [`Row`] values that are safe across
//!   callback boundaries — the buffering half of the row-mode / FDW write path,
//!   paired with `pg-arrow-conv`'s `ColumnRule::build(&[Row], ..)`. Not on the
//!   columnar hot path (see [`RowBatchBuffer`] for why it is retained).
//!
//! Core does not provide a `Cell`-based columnar buffer: on a mutation hot path,
//! materializing `Cell` first would add the same intermediate allocations the
//! slot/datum path is designed to avoid.
//!
//! The read path mirrors this: an [`AmScanBatchSource`] yields AM-defined column
//! batches and a [`BatchRowDecoder`] writes one batch row into a slot, paired by
//! the core-provided [`BatchRowCursor`]. Their Arrow implementations
//! (`ArrowBatchSource` / `ArrowColumnDecoder`) also live in `pg-arrow-conv`.

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
/// This is the fast path for Arrow-backed mutation buffers: the concrete appender
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

    /// Append one PostgreSQL datum view to this output column, returning the
    /// number of bytes it added to the column's in-memory footprint.
    ///
    /// `None` represents a missing value for this column index. SQL NULL values
    /// are represented by `Some(value)` where `value.is_null()` is true. A
    /// missing or NULL value adds `0` bytes.
    ///
    /// The returned size lets the owning buffer keep a running memory estimate
    /// in O(1) per append, without re-summing every column.
    fn append_datum(
        &mut self,
        value: Option<PgDatumRef<'_>>,
    ) -> Result<usize, Self::Error>;

    /// Finish the current column values and reset the appender for reuse.
    fn finish(&mut self) -> Result<Self::Column, Self::Error>;

    /// Drop buffered values from the current in-progress batch.
    fn clear(&mut self);

    /// Number of values appended for the current batch.
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Extension point for AM-owned columnar buffers fed directly from PostgreSQL
/// tuple slots.
///
/// This is the abstraction a future Arrow companion crate should implement for
/// the mutation hot path. It keeps core independent of Arrow while avoiding the
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
        let datums = row.datums();
        for column_index in 0..self.column_count() {
            self.append_datum_to_column(column_index, datums.datum_at(column_index))?;
        }
        self.finish_row()
    }
}

/// Row-oriented batch buffer backed by owned [`Row`] values.
///
/// # Role: the buffering half of the row-world write path
///
/// This is the **row-world** write buffer, the counterpart to the
/// `Cell`-based conversion (`pg-arrow-conv`'s `ColumnRule::build(&[Row], ..)`):
/// a row-mode access method or FDW accumulates owned `Row`s here (one slot at a
/// time, since `ExecForeignInsert` hands over one slot per call) and, on flush,
/// converts the buffered `&[Row]` to a columnar batch.
///
/// It is deliberately **not** on the columnar hot path. Columnar AMs (the
/// in-tree Iceberg AM) append tuple slots directly into a
/// [`SlotColumnarBatchBuffer`] (e.g. `pg-arrow-conv`'s `SlotRecordBatchBuffer`),
/// which skips the owned-`Row` materialization this type does. As a result the
/// in-tree code base does not drive `RowBatchBuffer` today — it is exercised by
/// unit tests and retained as the row-world buffering primitive a future
/// row-mode FDW will use. It is **not** dead/legacy code: removing it would
/// leave the row-world write path (which keeps `ColumnRule::build`) without its
/// buffering half. See the columnar-datapath-refactor design, goal #3 / §3.1.
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

// ---------------------------------------------------------------------------
// Read side: column batch → slot.
// ---------------------------------------------------------------------------

use crate::api::AmResult;
use crate::tuple::SlotColumns;

/// Yields one AM-defined column batch at a time. The batch is owned by value so
/// its lifetime is independent of any PostgreSQL memory context.
pub trait AmScanBatchSource {
    type Batch;

    fn next_batch(&mut self) -> AmResult<Option<Self::Batch>>;
}

/// Decodes a column batch into slots. The concrete implementation lives in the
/// conversion crate; core never names a columnar format here.
///
/// Decoding is split into a per-batch [`bind`](Self::bind) step and a per-row
/// [`write_row`](Self::write_row) step. `bind` resolves each column's concrete
/// typed array **once per batch** (the validation + downcast that would
/// otherwise repeat on every value), producing a [`Bound`](Self::Bound) the
/// per-row path reads without any per-value type dispatch.
pub trait BatchRowDecoder {
    type Batch;
    /// Per-batch bound state: columns resolved to their concrete typed arrays.
    type Bound;

    /// Bind a freshly fetched batch: validate the stream's schema and resolve
    /// each mapped column to its concrete typed array, once. This is where a
    /// producer/plan type drift surfaces as a clean error (the exact decimal
    /// scale, fixed width, timestamp unit/tz — not just the array kind a
    /// per-value downcast would check), at the batch boundary rather than
    /// per row.
    fn bind(&self, batch: Self::Batch) -> AmResult<Self::Bound>;

    fn num_rows(&self, bound: &Self::Bound) -> usize;

    fn write_row(
        &self,
        bound: &Self::Bound,
        row_idx: usize,
        out: &mut SlotColumns<'_>,
    ) -> AmResult<()>;
}

/// The driver an `AmScanSession`'s associated `BatchDriver` type is bound to.
/// Kept object-safe-free: the session holds a concrete type, so the per-row
/// call monomorphizes.
pub trait ScanBatchDriver {
    fn next_into_slot(&mut self, out: &mut SlotColumns<'_>) -> AmResult<bool>;
}

/// Core-provided adapter pairing a batch source with a row decoder and exposing
/// a "fetch one row into the slot" driver. Generic over both halves so the
/// per-row decode is a static call.
#[allow(dead_code)]
pub struct BatchRowCursor<S, D>
where
    S: AmScanBatchSource,
    D: BatchRowDecoder<Batch = S::Batch>,
{
    source: S,
    decoder: D,
    /// The current batch, bound once on fetch (validated + downcast per column),
    /// then read row by row. Dropped before the next batch is fetched so at most
    /// one batch is ever live.
    current: Option<D::Bound>,
    row_idx: usize,
}

impl<S, D> BatchRowCursor<S, D>
where
    S: AmScanBatchSource,
    D: BatchRowDecoder<Batch = S::Batch>,
{
    pub fn new(source: S, decoder: D) -> Self {
        Self {
            source,
            decoder,
            current: None,
            row_idx: 0,
        }
    }

    pub fn next_into_slot(&mut self, out: &mut SlotColumns<'_>) -> AmResult<bool> {
        loop {
            if let Some(bound) = self.current.as_ref()
                && self.row_idx < self.decoder.num_rows(bound)
            {
                self.decoder.write_row(bound, self.row_idx, out)?;
                self.row_idx += 1;
                return Ok(true);
            }

            // Drop the exhausted batch before fetching the next so at most one
            // batch is ever live.
            self.current = None;
            match self.source.next_batch()? {
                Some(batch) => {
                    // Bind once per batch: this is where the per-column type is
                    // validated and the array downcast, so per-row decode does
                    // no per-value downcast.
                    self.current = Some(self.decoder.bind(batch)?);
                    self.row_idx = 0;
                }
                None => return Ok(false),
            }
        }
    }

    // Reserved for the direct columnar emit path; not implemented here:
    // pub fn next_batch(&mut self) -> AmResult<Option<S::Batch>> { .. }
}

impl<S, D> ScanBatchDriver for BatchRowCursor<S, D>
where
    S: AmScanBatchSource,
    D: BatchRowDecoder<Batch = S::Batch>,
{
    fn next_into_slot(&mut self, out: &mut SlotColumns<'_>) -> AmResult<bool> {
        BatchRowCursor::next_into_slot(self, out)
    }
}

#[cfg(test)]
mod read_tests {
    use super::*;
    use std::cell::Cell as StdCell;
    use std::rc::Rc;

    use pgrx::pg_sys;

    /// A batch that reports itself live for as long as it exists, so a test can
    /// assert the cursor never holds more than one batch at a time.
    struct LiveBatch {
        rows: Vec<Vec<Option<i64>>>,
        live: Rc<StdCell<usize>>,
    }

    impl Drop for LiveBatch {
        fn drop(&mut self) {
            self.live.set(self.live.get() - 1);
        }
    }

    struct FakeSource {
        batches: std::vec::IntoIter<Vec<Vec<Option<i64>>>>,
        live: Rc<StdCell<usize>>,
        max_live: Rc<StdCell<usize>>,
    }

    impl AmScanBatchSource for FakeSource {
        type Batch = LiveBatch;

        fn next_batch(&mut self) -> AmResult<Option<LiveBatch>> {
            match self.batches.next() {
                Some(rows) => {
                    self.live.set(self.live.get() + 1);
                    if self.live.get() > self.max_live.get() {
                        self.max_live.set(self.live.get());
                    }
                    Ok(Some(LiveBatch {
                        rows,
                        live: self.live.clone(),
                    }))
                }
                None => Ok(None),
            }
        }
    }

    struct FakeDecoder;

    impl BatchRowDecoder for FakeDecoder {
        type Batch = LiveBatch;
        type Bound = LiveBatch;

        fn bind(&self, batch: LiveBatch) -> AmResult<LiveBatch> {
            Ok(batch)
        }

        fn num_rows(&self, bound: &LiveBatch) -> usize {
            bound.rows.len()
        }

        fn write_row(
            &self,
            bound: &LiveBatch,
            row_idx: usize,
            out: &mut SlotColumns<'_>,
        ) -> AmResult<()> {
            for (col, value) in bound.rows[row_idx].iter().enumerate() {
                out.set_datum(col, value.map(|n| pg_sys::Datum::from(n as usize)));
            }
            Ok(())
        }
    }

    /// Owns the backing arrays a [`SlotColumns`] writes through. The `values`
    /// buffer never reallocates after construction, so the raw pointers handed
    /// to the slot stay valid for the slot's lifetime.
    struct HostSlot {
        slot: pg_sys::TupleTableSlot,
        tuple_desc: Box<pg_sys::TupleDescData>,
        values: Vec<pg_sys::Datum>,
        nulls: Vec<bool>,
    }

    impl HostSlot {
        fn new(natts: usize) -> Box<Self> {
            // `nulls` starts all-true so the test can assert the writer flips
            // only the positions it actually writes. This is the test harness's
            // own initial state, *not* what `ExecClearTuple` produces (a real
            // cleared slot leaves `tts_isnull` untouched / `palloc0`'d).
            let mut tuple_desc: Box<pg_sys::TupleDescData> =
                Box::new(unsafe { std::mem::zeroed() });
            tuple_desc.natts = natts as i32;
            let mut boxed = Box::new(HostSlot {
                slot: unsafe { std::mem::zeroed() },
                tuple_desc,
                values: vec![pg_sys::Datum::from(0usize); natts],
                nulls: vec![true; natts],
            });
            boxed.slot.tts_values = boxed.values.as_mut_ptr();
            boxed.slot.tts_isnull = boxed.nulls.as_mut_ptr();
            boxed.slot.tts_tupleDescriptor = &mut *boxed.tuple_desc;
            boxed
        }

        fn columns(&mut self) -> SlotColumns<'_> {
            unsafe { SlotColumns::new(&mut self.slot, std::ptr::null_mut()) }
        }
    }

    #[test]
    fn cursor_visits_every_row_once_across_batches() {
        let batches = vec![
            vec![vec![Some(1), Some(2)], vec![Some(3), Some(4)]],
            vec![vec![Some(5), Some(6)]],
        ];
        let live = Rc::new(StdCell::new(0));
        let max_live = Rc::new(StdCell::new(0));
        let source = FakeSource {
            batches: batches.into_iter(),
            live: live.clone(),
            max_live: max_live.clone(),
        };
        let mut cursor = BatchRowCursor::new(source, FakeDecoder);

        let natts = 2;
        let mut host = HostSlot::new(natts);
        let mut produced = Vec::new();
        loop {
            let mut cols = host.columns();
            if !cursor.next_into_slot(&mut cols).unwrap() {
                break;
            }
            produced
                .push((host.values[0].value() as i64, host.values[1].value() as i64));
        }

        assert_eq!(produced, vec![(1, 2), (3, 4), (5, 6)]);
        assert_eq!(max_live.get(), 1, "more than one batch was live at once");
        assert_eq!(live.get(), 0, "final batch was not dropped at end of scan");
    }

    #[test]
    fn cursor_signals_end_of_scan_once() {
        let live = Rc::new(StdCell::new(0));
        let max_live = Rc::new(StdCell::new(0));
        let source = FakeSource {
            batches: vec![vec![vec![Some(7)]]].into_iter(),
            live,
            max_live,
        };
        let mut cursor = BatchRowCursor::new(source, FakeDecoder);

        let natts = 1;
        let mut host = HostSlot::new(natts);

        let mut trues = 0;
        loop {
            let mut cols = host.columns();
            if cursor.next_into_slot(&mut cols).unwrap() {
                trues += 1;
            } else {
                break;
            }
        }
        assert_eq!(trues, 1);

        // End-of-scan stays terminal on repeated calls.
        let mut cols = host.columns();
        assert!(!cursor.next_into_slot(&mut cols).unwrap());
    }

    #[test]
    fn slot_columns_marks_null_and_leaves_unmapped_positions_null() {
        let natts = 3;
        let mut host = HostSlot::new(natts);
        {
            let mut cols = host.columns();
            cols.set_datum(0, Some(pg_sys::Datum::from(42usize)));
            cols.set_datum(1, None);
            // Index 2 is intentionally never written.
        }

        assert_eq!(host.nulls, vec![false, true, true]);
        assert_eq!(host.values[0].value() as i64, 42);
    }
}
