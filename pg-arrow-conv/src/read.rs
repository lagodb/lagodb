//! Columnar read-side adapters: the two halves core's
//! [`BatchRowCursor<Source, Decoder>`](pg_lakebase_core::batch) consumes.
//!
//! - [`ArrowBatchSource`] (impl [`AmScanBatchSource`]) feeds one Arrow
//!   `RecordBatch` at a time from any fallible Arrow batch iterator.
//! - [`ArrowColumnDecoder`] (impl [`BatchRowDecoder`]) decodes one batch row
//!   into a tuple slot, per column, keyed on the same [`ColumnRule`] the write
//!   encoder uses.
//!
//! The write-side analogue is [`SlotRecordBatchBuffer`](crate::buffer).

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
    ListArray, RecordBatch, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow_schema::DataType;
use pg_lakebase_core::api::{AmError, AmResult};
use pg_lakebase_core::batch::{AmScanBatchSource, BatchRowDecoder};
use pg_lakebase_core::tuple::{
    ByteaView, Cell, DatumTarget, Decimal128NumericCodec, Row, SlotColumns,
    StringView,
};
use pgrx::datum::Uuid;
use pgrx::pg_sys;

use crate::error::{ConvError, ConvResult};
use crate::rule::{ColumnRule, ListElementRule};
use crate::types::{downcast, list, temporal};

use arrow_array::FixedSizeBinaryArray;

// ---------------------------------------------------------------------------
// Batch source
// ---------------------------------------------------------------------------

/// Thin wrapper over a fallible Arrow batch iterator. It owns no buffer: the
/// cursor holds the one live batch, so the source just forwards `next()` and
/// lifts the iterator's error to the callback boundary.
///
/// It adapts any Arrow batch iterator into an [`AmScanBatchSource`] without
/// naming a table format. Batch *production* (opening Parquet, reading
/// metadata, IO) is the producing crate's domain, not a value conversion, so
/// the iterator's error only has to map into the callback boundary error
/// ([`AmError`]) and carries its own SQLSTATE classification straight through —
/// this layer never reclassifies a producer error as a [`ConvError`]
/// (`DatumConversionError`). The Iceberg AM keeps its format error type
/// (`IcebergError`) out of this layer while preserving the right SQLSTATE.
///
/// [`ConvError`]: crate::error::ConvError
pub struct ArrowBatchSource<I, E>
where
    I: Iterator<Item = Result<RecordBatch, E>>,
    E: Into<AmError>,
{
    iter: I,
}

impl<I, E> ArrowBatchSource<I, E>
where
    I: Iterator<Item = Result<RecordBatch, E>>,
    E: Into<AmError>,
{
    pub fn new(iter: I) -> Self {
        Self { iter }
    }
}

impl<I, E> AmScanBatchSource for ArrowBatchSource<I, E>
where
    I: Iterator<Item = Result<RecordBatch, E>>,
    E: Into<AmError>,
{
    type Batch = RecordBatch;

    fn next_batch(&mut self) -> AmResult<Option<RecordBatch>> {
        // The producer error maps directly to the callback boundary's
        // `PgReportError`, preserving its own SQLSTATE (IO/internal/feature),
        // instead of being collapsed into a conversion `DATA_EXCEPTION`.
        let batch = self
            .iter
            .next()
            .transpose()
            .map_err(Into::<AmError>::into)?;
        Ok(batch)
    }
}

// ---------------------------------------------------------------------------
// Row decoder
// ---------------------------------------------------------------------------

/// One mapped column: which Arrow batch column to read, the resolved rule, the
/// destination slot index, and the slot column's datum target. The target type
/// OID is carried because a single rule (`Utf8`/`Binary`) can back several
/// PostgreSQL types — `text`/`json`/`name` and `bytea`/`jsonb` — whose datum
/// construction differs; it is classified into a [`DatumTarget`] once at bind.
pub struct DecodedColumn {
    rule: ColumnRule,
    src_col: usize,
    dest: usize,
    dest_oid: pg_sys::Oid,
}

impl DecodedColumn {
    pub fn new(
        rule: ColumnRule,
        src_col: usize,
        dest: usize,
        dest_oid: pg_sys::Oid,
    ) -> Self {
        Self {
            rule,
            src_col,
            dest,
            dest_oid,
        }
    }
}

/// One column's concrete Arrow array, resolved (validated + downcast) **once**
/// when a batch is bound, so the per-row read does no per-value type dispatch.
///
/// Each held array is a cheap clone of the batch column (Arrow arrays are thin
/// `Arc`-backed handles, so `clone` is a refcount bump, not a data copy), which
/// lets a reader own its array without borrowing the batch — the batch is
/// dropped after binding while the readers keep its buffers alive.
///
/// Variants that share a physical Arrow type but differ in target meaning are
/// kept distinct (`FixedBinary` vs `Uuid` both read `FixedSizeBinary`;
/// `TimestampMicros`/`TimestampNanos` carry their tz-awareness), so the per-row
/// arm is a direct typed access with no re-inspection of the rule.
///
/// Private on purpose: the concrete Arrow variants, their split, and the list
/// special case are implementation detail behind the opaque [`ColumnReader`].
enum ReaderImpl {
    Bool(BooleanArray),
    I32(Int32Array),
    I64(Int64Array),
    F32(Float32Array),
    F64(Float64Array),
    Utf8(StringArray),
    LargeUtf8(LargeStringArray),
    Binary(BinaryArray),
    LargeBinary(LargeBinaryArray),
    FixedBinary(FixedSizeBinaryArray),
    Uuid(FixedSizeBinaryArray),
    Date32(Date32Array),
    Time64Micros(Time64MicrosecondArray),
    TimestampMicros {
        arr: TimestampMicrosecondArray,
        tz: bool,
    },
    TimestampNanos {
        arr: TimestampNanosecondArray,
        tz: bool,
    },
    Decimal128 {
        arr: Decimal128Array,
        codec: Decimal128NumericCodec,
    },
    List {
        arr: ListArray,
        element: ListElementRule,
        elem_oid: pg_sys::Oid,
    },
}

/// A batch column resolved (validated + downcast) to its concrete Arrow array
/// once, then read row by row with no per-value downcast — the bound,
/// schema-neutral building block shared by both read worlds:
///
/// - the slot-first columnar scan, via [`read_datum`](Self::read_datum);
/// - the row-world / FDW `Cell` read, via [`read_cell`](Self::read_cell).
///
/// Opaque by design: the inner Arrow representation can change without breaking
/// callers. Build one per column when a batch arrives ([`bind`](Self::bind)),
/// then read rows from it.
pub struct ColumnReader(ReaderImpl);

impl ColumnReader {
    /// Validate `array` against `rule` and downcast it once. The `accepts`
    /// check keeps the exact-type strictness the per-scan validation used to
    /// provide (decimal scale, fixed width, timestamp unit/tz), so the
    /// subsequent downcast cannot fail.
    pub fn bind(rule: &ColumnRule, array: &dyn Array) -> ConvResult<Self> {
        if !rule.accepts(array.data_type()) {
            return Err(ConvError::ArrowTypeMismatch(
                format!(
                    "batch column has type {:?}, incompatible with the column's \
                     resolved conversion rule",
                    array.data_type()
                )
                .into(),
            ));
        }
        let reader = match rule {
            ColumnRule::Bool => {
                ReaderImpl::Bool(downcast::<BooleanArray>(array, "Boolean")?.clone())
            }
            ColumnRule::I32 => {
                ReaderImpl::I32(downcast::<Int32Array>(array, "Int32")?.clone())
            }
            ColumnRule::I64 => {
                ReaderImpl::I64(downcast::<Int64Array>(array, "Int64")?.clone())
            }
            ColumnRule::F32 => {
                ReaderImpl::F32(downcast::<Float32Array>(array, "Float32")?.clone())
            }
            ColumnRule::F64 => {
                ReaderImpl::F64(downcast::<Float64Array>(array, "Float64")?.clone())
            }
            ColumnRule::Utf8 => match array.data_type() {
                DataType::Utf8 => ReaderImpl::Utf8(array.as_string::<i32>().clone()),
                _ => ReaderImpl::LargeUtf8(array.as_string::<i64>().clone()),
            },
            ColumnRule::Binary => match array.data_type() {
                DataType::Binary => {
                    ReaderImpl::Binary(array.as_binary::<i32>().clone())
                }
                _ => ReaderImpl::LargeBinary(array.as_binary::<i64>().clone()),
            },
            ColumnRule::FixedBinary { .. } => ReaderImpl::FixedBinary(
                downcast::<FixedSizeBinaryArray>(array, "FixedSizeBinary")?.clone(),
            ),
            ColumnRule::Uuid => ReaderImpl::Uuid(
                downcast::<FixedSizeBinaryArray>(array, "FixedSizeBinary (UUID)")?
                    .clone(),
            ),
            ColumnRule::Date32 => {
                ReaderImpl::Date32(downcast::<Date32Array>(array, "Date32")?.clone())
            }
            ColumnRule::Time64Micros => ReaderImpl::Time64Micros(
                downcast::<Time64MicrosecondArray>(array, "Time64Microsecond")?
                    .clone(),
            ),
            ColumnRule::Timestamp { nanos, tz } => {
                if *nanos {
                    ReaderImpl::TimestampNanos {
                        arr: downcast::<TimestampNanosecondArray>(
                            array,
                            "Timestamp(Nanosecond)",
                        )?
                        .clone(),
                        tz: *tz,
                    }
                } else {
                    ReaderImpl::TimestampMicros {
                        arr: downcast::<TimestampMicrosecondArray>(
                            array,
                            "Timestamp(Microsecond)",
                        )?
                        .clone(),
                        tz: *tz,
                    }
                }
            }
            ColumnRule::Decimal128 { precision, scale } => ReaderImpl::Decimal128 {
                arr: downcast::<Decimal128Array>(array, "Decimal128")?.clone(),
                codec: Decimal128NumericCodec::new(*precision, *scale)?,
            },
            ColumnRule::List {
                element, elem_oid, ..
            } => ReaderImpl::List {
                arr: downcast::<ListArray>(array, "List")?.clone(),
                element: *element,
                elem_oid: *elem_oid,
            },
        };
        Ok(ColumnReader(reader))
    }

    /// Slot-first read keyed on a pre-resolved [`DatumTarget`]: decode the value
    /// at `row_idx` into a PostgreSQL datum, or `None` for SQL NULL. No downcast
    /// happens here — the concrete array was resolved in [`Self::bind`].
    ///
    /// For a **scalar/varlena** column the value becomes a stack `Cell` fed
    /// through [`Cell::into_datum_for`] using the bind-time `target`, so the hot
    /// loop never re-runs the builtin-OID lookup. For a **list** column `target`
    /// is ignored — the produced array's element type comes from the `elem_oid`
    /// bound into the rule at [`ColumnReader::bind`].
    ///
    /// # Safety
    ///
    /// Builds the datum through PostgreSQL internals: a backend must be active
    /// and the caller must have switched to the memory context the varlena (or
    /// array) payload should be palloc'd into.
    pub unsafe fn read_datum_for(
        &self,
        row_idx: usize,
        target: DatumTarget,
    ) -> ConvResult<Option<pg_sys::Datum>> {
        match &self.0 {
            ReaderImpl::List {
                arr,
                element,
                elem_oid,
            } => {
                if arr.is_null(row_idx) {
                    return Ok(None);
                }
                Ok(Some(unsafe {
                    list::array_datum_at(arr, row_idx, *element, *elem_oid)
                }?))
            }
            _ => {
                // SAFETY: the borrowed-view `Cell` returned here is consumed
                // immediately by `into_datum_for` below (which copies into a
                // palloc'd datum) within this reader's lifetime, so the view
                // never escapes.
                let Some(cell) = (unsafe { self.read_cell(row_idx) })? else {
                    return Ok(None);
                };
                let datum =
                    unsafe { cell.into_datum_for(target) }.ok_or_else(|| {
                        ConvError::DatumConversionError(format!(
                            "value is not representable as PostgreSQL target \
                             {target:?}"
                        ))
                    })?;
                Ok(Some(datum))
            }
        }
    }

    /// Slot-first read for callers that only hold the destination type OID.
    ///
    /// Resolves the [`DatumTarget`] from `oid` and defers to
    /// [`Self::read_datum_for`]. `typmod` is unused (kept for call-site
    /// symmetry). The columnar scan path resolves the target once at bind and
    /// calls [`Self::read_datum_for`] directly.
    ///
    /// # Safety
    ///
    /// Builds the datum through PostgreSQL internals: a backend must be active
    /// and the caller must have switched to the memory context the varlena (or
    /// array) payload should be palloc'd into.
    pub unsafe fn read_datum(
        &self,
        row_idx: usize,
        oid: pg_sys::Oid,
        _typmod: i32,
    ) -> ConvResult<Option<pg_sys::Datum>> {
        unsafe { self.read_datum_for(row_idx, DatumTarget::from_oid(oid)) }
    }

    /// Row-world read: decode the value at `row_idx` into a [`Cell`], or `None`
    /// for SQL NULL. No downcast happens here — the concrete array was resolved
    /// in [`Self::bind`]; the per-value math reuses each type module's helpers.
    ///
    /// # Safety
    ///
    /// This is `unsafe` because the `Cell::StringView` / `Cell::ByteaView`
    /// variants returned for text/binary columns are **zero-copy borrows** of
    /// the Arrow buffer this reader owns (raw `ptr`/`len`, no lifetime). The
    /// caller must ensure such a `Cell` does not outlive the `ColumnReader` and
    /// is not used (including via the safe `Cell: Display`, which dereferences
    /// the view) after the reader is dropped — copy/materialize it first (e.g.
    /// into a slot datum) if it must live longer. The owned variants (numeric,
    /// temporal, list arrays) carry no borrow and are always safe.
    pub unsafe fn read_cell(&self, row_idx: usize) -> ConvResult<Option<Cell>> {
        macro_rules! null_guard {
            ($arr:expr) => {
                if $arr.is_null(row_idx) {
                    return Ok(None);
                }
            };
        }
        let cell = match &self.0 {
            ReaderImpl::Bool(a) => {
                null_guard!(a);
                Cell::Bool(a.value(row_idx))
            }
            ReaderImpl::I32(a) => {
                null_guard!(a);
                Cell::I32(a.value(row_idx))
            }
            ReaderImpl::I64(a) => {
                null_guard!(a);
                Cell::I64(a.value(row_idx))
            }
            ReaderImpl::F32(a) => {
                null_guard!(a);
                Cell::F32(a.value(row_idx))
            }
            ReaderImpl::F64(a) => {
                null_guard!(a);
                Cell::F64(a.value(row_idx))
            }
            ReaderImpl::Utf8(a) => {
                null_guard!(a);
                str_view_cell(a.value(row_idx))
            }
            ReaderImpl::LargeUtf8(a) => {
                null_guard!(a);
                str_view_cell(a.value(row_idx))
            }
            ReaderImpl::Binary(a) => {
                null_guard!(a);
                bytea_view_cell(a.value(row_idx))
            }
            ReaderImpl::LargeBinary(a) => {
                null_guard!(a);
                bytea_view_cell(a.value(row_idx))
            }
            ReaderImpl::FixedBinary(a) => {
                null_guard!(a);
                bytea_view_cell(a.value(row_idx))
            }
            ReaderImpl::Uuid(a) => {
                null_guard!(a);
                let bytes: [u8; 16] = a.value(row_idx).try_into().map_err(|_| {
                    ConvError::ArrowTypeMismatch(std::borrow::Cow::Borrowed(
                        "UUID must be 16 bytes",
                    ))
                })?;
                // Arrow UUID bytes are RFC 4122 network order, as pgrx expects.
                Cell::Uuid(Uuid::from_bytes(bytes))
            }
            ReaderImpl::Date32(a) => {
                null_guard!(a);
                Cell::Date(temporal::pg_date_from_arrow_days(a.value(row_idx))?)
            }
            ReaderImpl::Time64Micros(a) => {
                null_guard!(a);
                Cell::Time(temporal::time_from_micros(a.value(row_idx))?)
            }
            ReaderImpl::TimestampMicros { arr, tz } => {
                null_guard!(arr);
                timestamp_cell(arr.value(row_idx), *tz)?
            }
            ReaderImpl::TimestampNanos { arr, tz } => {
                null_guard!(arr);
                let micros = temporal::unix_micros_from_nanos(arr.value(row_idx));
                timestamp_cell(micros, *tz)?
            }
            ReaderImpl::Decimal128 { arr, codec } => {
                null_guard!(arr);
                Cell::Numeric(codec.decode(arr.value(row_idx))?)
            }
            ReaderImpl::List { arr, element, .. } => {
                null_guard!(arr);
                // Row-world list cell keeps the Arrow *physical* element width
                // (see the narrowing TODO in `types::list`); the slot path uses
                // `read_datum`'s element-OID-aware route instead.
                list::cell_at(arr, row_idx, *element)?
            }
        };
        Ok(Some(cell))
    }
}

fn str_view_cell(s: &str) -> Cell {
    Cell::StringView(StringView {
        ptr: s.as_ptr(),
        len: s.len(),
    })
}

fn bytea_view_cell(bytes: &[u8]) -> Cell {
    Cell::ByteaView(ByteaView {
        ptr: bytes.as_ptr(),
        len: bytes.len(),
    })
}

fn timestamp_cell(unix_micros: i64, tz: bool) -> ConvResult<Cell> {
    if tz {
        Ok(Cell::Timestamptz(temporal::timestamptz_from_unix_micros(
            unix_micros,
        )?))
    } else {
        Ok(Cell::Timestamp(temporal::timestamp_from_unix_micros(
            unix_micros,
        )?))
    }
}

/// One batch with every mapped column resolved to its concrete typed array.
/// Produced by [`ArrowColumnDecoder::bind`] and read row by row, so the
/// validation + downcast is paid once per batch, never per value.
pub struct BoundBatch {
    columns: Box<[BoundColumn]>,
    num_rows: usize,
}

struct BoundColumn {
    reader: ColumnReader,
    dest: usize,
    /// Datum-construction target resolved once from `dest_oid` at bind, so the
    /// per-row decode dispatches on a small enum instead of re-classifying the
    /// destination OID for every value.
    target: DatumTarget,
}

/// Decodes Arrow column values into slot datums. The per-column plan is
/// resolved once (no per-row rule resolution or allocation); unmapped slot
/// positions are left untouched.
///
/// Leaving a position untouched is safe only because every column the executor
/// can read is mapped: a whole-row reference falls back to `NeededColumns::All`
/// (so the plan covers every live column), and a projected scan maps exactly
/// the referenced columns — unmapped positions are therefore dropped or
/// otherwise never-read columns. This does **not** rely on `ExecClearTuple`
/// resetting `tts_isnull`: it does not (the slot's `tts_values`/`tts_isnull`
/// arrays are `palloc0`'d, i.e. start non-NULL, and only `ExecStoreAllNullTuple`
/// marks them all NULL). Correctness comes from the "unmapped == unreferenced"
/// invariant, not from the cleared slot.
pub struct ArrowColumnDecoder {
    columns: Box<[DecodedColumn]>,
}

impl ArrowColumnDecoder {
    pub fn new(columns: Vec<DecodedColumn>) -> Self {
        Self {
            columns: columns.into_boxed_slice(),
        }
    }

    /// Decode one batch row into an owned [`Row`].
    ///
    /// This is for row-version fetch paths that must return a `Row` beyond the
    /// lifetime of the Arrow batch. Borrowed string/binary views are copied into
    /// owned cells before the row is returned.
    pub fn read_owned_row(
        &self,
        batch: RecordBatch,
        row_idx: usize,
    ) -> AmResult<Row> {
        let bound = self.bind(batch)?;
        if row_idx >= bound.num_rows {
            return Err(ConvError::DatumConversionError(
                format!(
                    "row index {row_idx} is out of range for batch with {} rows",
                    bound.num_rows
                )
                .into(),
            )
            .into());
        }

        let mut row = Row::with_capacity(
            self.columns
                .iter()
                .map(|col| col.dest)
                .max()
                .map(|dest| dest + 1)
                .unwrap_or(0),
        );
        for col in bound.columns.iter() {
            // SAFETY: `read_cell` may return borrowed views into arrays owned by
            // `bound`; `Cell::into_owned` copies those view variants before
            // `bound` is dropped at the end of this method.
            let cell =
                unsafe { col.reader.read_cell(row_idx) }?.map(Cell::into_owned);
            row.set_cell(col.dest, cell);
        }
        Ok(row)
    }
}

impl BatchRowDecoder for ArrowColumnDecoder {
    type Batch = RecordBatch;
    type Bound = BoundBatch;

    /// Resolve every mapped column to its concrete typed array, once per batch.
    /// Each `src_col` is bounds-checked and its array validated + downcast here
    /// (see [`ColumnReader::bind`]), so `write_row` does no per-value downcast
    /// and a producer/plan type drift surfaces as a clean
    /// [`ConvError::ArrowTypeMismatch`] at the batch boundary.
    fn bind(&self, batch: RecordBatch) -> AmResult<BoundBatch> {
        let num_rows = batch.num_rows();
        let mut columns = Vec::with_capacity(self.columns.len());
        for col in self.columns.iter() {
            let array = batch.columns().get(col.src_col).ok_or_else(|| {
                ConvError::ArrowTypeMismatch(
                    format!(
                        "batch has {} columns but the plan reads column {}",
                        batch.num_columns(),
                        col.src_col
                    )
                    .into(),
                )
            })?;
            columns.push(BoundColumn {
                reader: ColumnReader::bind(&col.rule, array.as_ref())?,
                dest: col.dest,
                target: DatumTarget::from_oid(col.dest_oid),
            });
        }
        Ok(BoundBatch {
            columns: columns.into_boxed_slice(),
            num_rows,
        })
    }

    fn num_rows(&self, bound: &BoundBatch) -> usize {
        bound.num_rows
    }

    fn write_row(
        &self,
        bound: &BoundBatch,
        row_idx: usize,
        out: &mut SlotColumns<'_>,
    ) -> AmResult<()> {
        for col in bound.columns.iter() {
            // The shim already switched to the slot's target context, so the
            // varlena/array palloc lands where the per-row reset expects it.
            let datum = unsafe { col.reader.read_datum_for(row_idx, col.target) }?;
            out.set_datum(col.dest, datum);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(vals: &[i32]) -> RecordBatch {
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vals.to_vec()))])
            .unwrap()
    }

    #[test]
    fn forwards_batches_in_order_then_ends() {
        let items: Vec<Result<RecordBatch, ConvError>> =
            vec![Ok(batch(&[1, 2])), Ok(batch(&[3]))];
        let mut source = ArrowBatchSource::new(items.into_iter());

        assert_eq!(source.next_batch().unwrap().unwrap().num_rows(), 2);
        assert_eq!(source.next_batch().unwrap().unwrap().num_rows(), 1);
        assert!(source.next_batch().unwrap().is_none());
    }

    #[test]
    fn lifts_iterator_error() {
        let items: Vec<Result<RecordBatch, ConvError>> =
            vec![Err(ConvError::InvariantViolated("batch source failure"))];
        let mut source = ArrowBatchSource::new(items.into_iter());

        assert!(source.next_batch().is_err());
    }
}
