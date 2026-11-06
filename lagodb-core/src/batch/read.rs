//! Read side: column batch → slot.

use pgrx::PgSqlErrorCode;

use crate::diag::PgReportError;
use crate::handles::ScanDirection;
use crate::tuple::SlotDatumWriter;

/// Yields one AM-defined column batch at a time. The batch is owned by value so
/// its lifetime is independent of any PostgreSQL memory context.
pub trait AmScanBatchSource {
    type Batch;

    fn next_batch(&mut self) -> Result<Option<Self::Batch>, PgReportError>;
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
    fn bind(&self, batch: Self::Batch) -> Result<Self::Bound, PgReportError>;

    fn num_rows(&self, bound: &Self::Bound) -> usize;

    /// Write one row through a safe slot API. Implementations must not rely on
    /// an unexpressed relationship between `bound`/decoder destinations and
    /// the caller-supplied slot; a provider-specific unchecked path must make
    /// that relationship an explicit unsafe contract instead.
    fn write_row(
        &self,
        bound: &Self::Bound,
        row_idx: usize,
        out: &mut SlotDatumWriter<'_>,
    ) -> Result<(), PgReportError>;
}

/// The driver an `AmScanSession`'s associated `BatchDriver` type is bound to.
/// Kept object-safe-free: the session holds a concrete type, so the per-row
/// call monomorphizes.
pub trait ScanBatchDriver {
    /// Fetch one row in PostgreSQL's requested direction.
    ///
    /// Implementations must not silently treat an unsupported direction as
    /// [`ScanDirection::Forward`]. A forward-only provider should return
    /// `FEATURE_NOT_SUPPORTED` for other directions.
    fn next_into_slot(
        &mut self,
        direction: ScanDirection,
        out: &mut SlotDatumWriter<'_>,
    ) -> Result<bool, PgReportError>;
}

/// Core-provided adapter pairing a batch source with a row decoder and exposing
/// a "fetch one row into the slot" driver. Generic over both halves so the
/// per-row decode is a static call.
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

    pub fn next_into_slot(
        &mut self,
        direction: ScanDirection,
        out: &mut SlotDatumWriter<'_>,
    ) -> Result<bool, PgReportError> {
        if direction != ScanDirection::Forward {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "non-forward batch scan is not supported by this access method",
            ));
        }
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
    // pub fn next_batch(&mut self) -> Result<Option<S::Batch>, PgReportError> { .. }
}

impl<S, D> ScanBatchDriver for BatchRowCursor<S, D>
where
    S: AmScanBatchSource,
    D: BatchRowDecoder<Batch = S::Batch>,
{
    fn next_into_slot(
        &mut self,
        direction: ScanDirection,
        out: &mut SlotDatumWriter<'_>,
    ) -> Result<bool, PgReportError> {
        BatchRowCursor::next_into_slot(self, direction, out)
    }
}

#[cfg(test)]
mod tests {
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

        fn next_batch(&mut self) -> Result<Option<LiveBatch>, PgReportError> {
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

        fn bind(&self, batch: LiveBatch) -> Result<LiveBatch, PgReportError> {
            Ok(batch)
        }

        fn num_rows(&self, bound: &LiveBatch) -> usize {
            bound.rows.len()
        }

        fn write_row(
            &self,
            bound: &LiveBatch,
            row_idx: usize,
            out: &mut SlotDatumWriter<'_>,
        ) -> Result<(), PgReportError> {
            for (col, value) in bound.rows[row_idx].iter().enumerate() {
                out.set_datum(col, value.map(|n| pg_sys::Datum::from(n as usize)));
            }
            Ok(())
        }
    }

    /// Owns the backing arrays a [`SlotDatumWriter`] writes through. The `values`
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

        fn columns(&mut self) -> SlotDatumWriter<'_> {
            unsafe { SlotDatumWriter::new(&mut self.slot, std::ptr::null_mut()) }
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
            if !cursor
                .next_into_slot(ScanDirection::Forward, &mut cols)
                .unwrap()
            {
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
            if cursor
                .next_into_slot(ScanDirection::Forward, &mut cols)
                .unwrap()
            {
                trues += 1;
            } else {
                break;
            }
        }
        assert_eq!(trues, 1);

        // End-of-scan stays terminal on repeated calls.
        let mut cols = host.columns();
        assert!(
            !cursor
                .next_into_slot(ScanDirection::Forward, &mut cols)
                .unwrap()
        );
    }

    #[test]
    fn forward_only_cursor_rejects_backward_without_consuming_input() {
        let source = FakeSource {
            batches: vec![vec![vec![Some(7)]]].into_iter(),
            live: Rc::new(StdCell::new(0)),
            max_live: Rc::new(StdCell::new(0)),
        };
        let mut cursor = BatchRowCursor::new(source, FakeDecoder);
        let mut host = HostSlot::new(1);

        assert!(
            cursor
                .next_into_slot(ScanDirection::Backward, &mut host.columns())
                .is_err()
        );
        assert!(
            cursor
                .next_into_slot(ScanDirection::Forward, &mut host.columns())
                .unwrap()
        );
        assert_eq!(host.values[0].value() as i64, 7);
    }

    #[test]
    fn slot_datum_writer_marks_null_and_leaves_unmapped_positions_null() {
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
