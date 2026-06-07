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
    ByteaView, Cell, Decimal128NumericCodec, SlotColumns, StringView,
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
/// destination slot index, and the slot column's target type. The target
/// `(oid, typmod)` is carried because a single rule (`Utf8`/`Binary`) can back
/// several PostgreSQL types — `text`/`json`/`name` and `bytea`/`jsonb` — whose
/// datum construction differs.
pub struct DecodedColumn {
    rule: ColumnRule,
    src_col: usize,
    dest: usize,
    dest_oid: pg_sys::Oid,
    dest_typmod: i32,
}

impl DecodedColumn {
    pub fn new(
        rule: ColumnRule,
        src_col: usize,
        dest: usize,
        dest_oid: pg_sys::Oid,
        dest_typmod: i32,
    ) -> Self {
        Self {
            rule,
            src_col,
            dest,
            dest_oid,
            dest_typmod,
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
enum ColumnReader {
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
    TimestampMicros { arr: TimestampMicrosecondArray, tz: bool },
    TimestampNanos { arr: TimestampNanosecondArray, tz: bool },
    Decimal128 { arr: Decimal128Array, codec: Decimal128NumericCodec },
    List { arr: ListArray, element: ListElementRule, elem_oid: pg_sys::Oid },
}

impl ColumnReader {
    /// Validate `array` against `rule` and downcast it once. The `accepts`
    /// check keeps the exact-type strictness the per-scan validation used to
    /// provide (decimal scale, fixed width, timestamp unit/tz), so the
    /// subsequent downcast cannot fail.
    fn bind(rule: &ColumnRule, array: &dyn Array) -> ConvResult<Self> {
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
                Self::Bool(downcast::<BooleanArray>(array, "Boolean")?.clone())
            }
            ColumnRule::I32 => {
                Self::I32(downcast::<Int32Array>(array, "Int32")?.clone())
            }
            ColumnRule::I64 => {
                Self::I64(downcast::<Int64Array>(array, "Int64")?.clone())
            }
            ColumnRule::F32 => {
                Self::F32(downcast::<Float32Array>(array, "Float32")?.clone())
            }
            ColumnRule::F64 => {
                Self::F64(downcast::<Float64Array>(array, "Float64")?.clone())
            }
            ColumnRule::Utf8 => match array.data_type() {
                DataType::Utf8 => Self::Utf8(array.as_string::<i32>().clone()),
                _ => Self::LargeUtf8(array.as_string::<i64>().clone()),
            },
            ColumnRule::Binary => match array.data_type() {
                DataType::Binary => Self::Binary(array.as_binary::<i32>().clone()),
                _ => Self::LargeBinary(array.as_binary::<i64>().clone()),
            },
            ColumnRule::FixedBinary { .. } => Self::FixedBinary(
                downcast::<FixedSizeBinaryArray>(array, "FixedSizeBinary")?.clone(),
            ),
            ColumnRule::Uuid => Self::Uuid(
                downcast::<FixedSizeBinaryArray>(array, "FixedSizeBinary (UUID)")?
                    .clone(),
            ),
            ColumnRule::Date32 => {
                Self::Date32(downcast::<Date32Array>(array, "Date32")?.clone())
            }
            ColumnRule::Time64Micros => Self::Time64Micros(
                downcast::<Time64MicrosecondArray>(array, "Time64Microsecond")?
                    .clone(),
            ),
            ColumnRule::Timestamp { nanos, tz } => {
                if *nanos {
                    Self::TimestampNanos {
                        arr: downcast::<TimestampNanosecondArray>(
                            array,
                            "Timestamp(Nanosecond)",
                        )?
                        .clone(),
                        tz: *tz,
                    }
                } else {
                    Self::TimestampMicros {
                        arr: downcast::<TimestampMicrosecondArray>(
                            array,
                            "Timestamp(Microsecond)",
                        )?
                        .clone(),
                        tz: *tz,
                    }
                }
            }
            ColumnRule::Decimal128 { precision, scale } => Self::Decimal128 {
                arr: downcast::<Decimal128Array>(array, "Decimal128")?.clone(),
                codec: Decimal128NumericCodec::new(*precision, *scale)?,
            },
            ColumnRule::List {
                element, elem_oid, ..
            } => Self::List {
                arr: downcast::<ListArray>(array, "List")?.clone(),
                element: *element,
                elem_oid: *elem_oid,
            },
        };
        Ok(reader)
    }

    /// Read the value at `row_idx` into a PostgreSQL datum for the target
    /// `(oid, typmod)`, or `None` for SQL NULL. No downcast happens here — the
    /// concrete array was resolved in [`Self::bind`].
    ///
    /// # Safety
    ///
    /// Builds the datum through PostgreSQL internals: a backend must be active
    /// and the caller must have switched to the memory context the varlena (or
    /// array) payload should be palloc'd into.
    unsafe fn read(
        &self,
        row_idx: usize,
        oid: pg_sys::Oid,
        typmod: i32,
    ) -> ConvResult<Option<pg_sys::Datum>> {
        // A list builds its array datum directly (bypassing the owned `Cell`);
        // every other rule produces a stack `Cell` fed through the target-aware
        // `Cell::into_datum_typed`, exactly as the row-world reader does.
        match self {
            ColumnReader::List {
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
            scalar => {
                let Some(cell) = scalar.read_cell(row_idx)? else {
                    return Ok(None);
                };
                let datum =
                    unsafe { cell.into_datum_typed(oid, typmod) }.ok_or_else(|| {
                        ConvError::DatumConversionError(format!(
                            "value is not representable as PostgreSQL type {}",
                            u32::from(oid)
                        ))
                    })?;
                Ok(Some(datum))
            }
        }
    }

    /// Build the stack [`Cell`] at `row_idx` for a non-list rule, or `None` for
    /// SQL NULL. The per-value math mirrors the row-world `extract_*` path; only
    /// the downcast is hoisted out (it happened in [`Self::bind`]).
    fn read_cell(&self, row_idx: usize) -> ConvResult<Option<Cell>> {
        macro_rules! null_guard {
            ($arr:expr) => {
                if $arr.is_null(row_idx) {
                    return Ok(None);
                }
            };
        }
        let cell = match self {
            ColumnReader::Bool(a) => {
                null_guard!(a);
                Cell::Bool(a.value(row_idx))
            }
            ColumnReader::I32(a) => {
                null_guard!(a);
                Cell::I32(a.value(row_idx))
            }
            ColumnReader::I64(a) => {
                null_guard!(a);
                Cell::I64(a.value(row_idx))
            }
            ColumnReader::F32(a) => {
                null_guard!(a);
                Cell::F32(a.value(row_idx))
            }
            ColumnReader::F64(a) => {
                null_guard!(a);
                Cell::F64(a.value(row_idx))
            }
            ColumnReader::Utf8(a) => {
                null_guard!(a);
                str_view_cell(a.value(row_idx))
            }
            ColumnReader::LargeUtf8(a) => {
                null_guard!(a);
                str_view_cell(a.value(row_idx))
            }
            ColumnReader::Binary(a) => {
                null_guard!(a);
                bytea_view_cell(a.value(row_idx))
            }
            ColumnReader::LargeBinary(a) => {
                null_guard!(a);
                bytea_view_cell(a.value(row_idx))
            }
            ColumnReader::FixedBinary(a) => {
                null_guard!(a);
                bytea_view_cell(a.value(row_idx))
            }
            ColumnReader::Uuid(a) => {
                null_guard!(a);
                let bytes: [u8; 16] =
                    a.value(row_idx).try_into().map_err(|_| {
                        ConvError::ArrowTypeMismatch(std::borrow::Cow::Borrowed(
                            "UUID must be 16 bytes",
                        ))
                    })?;
                // Arrow UUID bytes are RFC 4122 network order, as pgrx expects.
                Cell::Uuid(Uuid::from_bytes(bytes))
            }
            ColumnReader::Date32(a) => {
                null_guard!(a);
                Cell::Date(temporal::pg_date_from_arrow_days(a.value(row_idx))?)
            }
            ColumnReader::Time64Micros(a) => {
                null_guard!(a);
                Cell::Time(temporal::time_from_micros(a.value(row_idx))?)
            }
            ColumnReader::TimestampMicros { arr, tz } => {
                null_guard!(arr);
                timestamp_cell(arr.value(row_idx), *tz)?
            }
            ColumnReader::TimestampNanos { arr, tz } => {
                null_guard!(arr);
                let micros = temporal::unix_micros_from_nanos(arr.value(row_idx));
                timestamp_cell(micros, *tz)?
            }
            ColumnReader::Decimal128 { arr, codec } => {
                null_guard!(arr);
                Cell::Numeric(codec.decode(arr.value(row_idx))?)
            }
            ColumnReader::List { .. } => {
                return Err(ConvError::InvariantViolated(
                    "list reader has no Cell form; use read()",
                ));
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
    dest_oid: pg_sys::Oid,
    dest_typmod: i32,
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
                dest_oid: col.dest_oid,
                dest_typmod: col.dest_typmod,
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
            let datum = unsafe {
                col.reader.read(row_idx, col.dest_oid, col.dest_typmod)
            }?;
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
