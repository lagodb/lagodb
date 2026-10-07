//! Concrete Arrow array bindings, Cell readers and batch sources.
//!
//! [`ArrowBatchSource`] feeds one Arrow `RecordBatch` at a time. [`ColumnReader`]
//! binds concrete arrays for the Cell API and its caller-supplied semantic Datum
//! adapter. Declared Datum plans, physical codecs, recursive readers and slot
//! decoding belong to `datum_codec`.

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, RecordBatch, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow_schema::DataType;
use lagodb_core::api::{AmError, AmResult};
use lagodb_core::batch::AmScanBatchSource;
use lagodb_core::tuple::{
    ByteaView, Cell, ColumnDatumCodec, ColumnDatumTarget, Decimal128NumericCodec,
    StringView,
};
use pgrx::datum::Uuid;
use pgrx::pg_sys;

use crate::column_encoder::{downcast, temporal};
use crate::error::{ArrowConversionError, ArrowConversionResult};
use crate::rule::CellRule;

use arrow_array::FixedSizeBinaryArray;

pub(crate) mod list;

/// Read one Arrow validity bit after the enclosing bound batch established the
/// row index for every column.
///
/// # Safety
///
/// `row_idx` must be within `array`.
#[inline]
pub(crate) unsafe fn is_null_unchecked(array: &impl Array, row_idx: usize) -> bool {
    array.nulls().is_some_and(|nulls| {
        // SAFETY: upheld by this function's contract. Arrow's safe `is_null`
        // would repeat the same row bound already established by BoundBatch.
        !unsafe { nulls.inner().value_unchecked(row_idx) }
    })
}

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
/// this layer never reclassifies a producer error as a [`ArrowConversionError`]
/// (`DatumConversionError`). The Iceberg AM keeps its format error type
/// (`IcebergError`) out of this layer while preserving the right SQLSTATE.
///
/// [`ArrowConversionError`]: crate::error::ArrowConversionError
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

/// One column's concrete Arrow array, resolved (validated + downcast) **once**
/// when a batch is bound. Each value still dispatches on the bound reader enum,
/// but does not repeat Arrow downcasts or rule resolution.
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
pub(crate) enum ReaderImpl {
    Bool(BooleanArray),
    I16(Int16Array),
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
    List(Box<list::ListReader>),
}

/// A semantic batch column resolved (validated + downcast) to its concrete
/// Arrow array once, then read row by row with no per-value downcast.
///
/// This public reader belongs to the row-world / FDW `Cell` API. The
/// slot-first scan path uses the private `datum_codec::BoundDatumReader`, whose variants
/// also encode the final datum conversion.
///
/// Opaque by design: the inner Arrow representation can change without breaking
/// callers. Build one per column when a batch arrives ([`bind`](Self::bind)),
/// then read rows from it.
pub struct ColumnReader {
    reader: ReaderImpl,
}

impl ColumnReader {
    /// Bind a semantic reader for the row-world `Cell` API. Provider physical
    /// codecs are represented separately by `datum_codec::BoundDatumReader` so their
    /// bytes cannot be materialized as a semantic `Cell`.
    pub fn bind(rule: &CellRule, array: &dyn Array) -> ArrowConversionResult<Self> {
        if rule.requires_utf8_server_encoding() {
            ColumnDatumTarget::validate_utf8_server_encoding()
                .map_err(ArrowConversionError::from)?;
        }
        Ok(Self {
            reader: Self::bind_reader(rule, array)?,
        })
    }

    /// Validate `array` against `rule` and downcast it once. The `accepts`
    /// check keeps the exact-type strictness the per-scan validation used to
    /// provide (decimal scale, fixed width, timestamp unit/tz), so the
    /// subsequent downcast cannot fail.
    pub(crate) fn bind_reader(
        rule: &CellRule,
        array: &dyn Array,
    ) -> ArrowConversionResult<ReaderImpl> {
        if !rule.accepts(array.data_type()) {
            return Err(ArrowConversionError::ArrowTypeMismatch(
                format!(
                    "batch column has type {:?}, incompatible with the column's \
                     resolved conversion rule",
                    array.data_type()
                )
                .into(),
            ));
        }
        // SAFETY: the complete rule was checked above, including child types.
        unsafe { Self::bind_reader_unchecked(rule, array) }
    }

    /// Bind the concrete arrays without repeating schema validation.
    ///
    /// # Safety
    /// `rule.accepts(array.data_type())` must hold, including decimal scale,
    /// fixed binary width and all nested element types.
    pub(crate) unsafe fn bind_reader_unchecked(
        rule: &CellRule,
        array: &dyn Array,
    ) -> ArrowConversionResult<ReaderImpl> {
        let reader = match rule {
            CellRule::Bool => {
                ReaderImpl::Bool(downcast::<BooleanArray>(array, "Boolean")?.clone())
            }
            CellRule::I16 => {
                ReaderImpl::I16(downcast::<Int16Array>(array, "Int16")?.clone())
            }
            CellRule::I32 => {
                ReaderImpl::I32(downcast::<Int32Array>(array, "Int32")?.clone())
            }
            CellRule::I64 => {
                ReaderImpl::I64(downcast::<Int64Array>(array, "Int64")?.clone())
            }
            CellRule::F32 => {
                ReaderImpl::F32(downcast::<Float32Array>(array, "Float32")?.clone())
            }
            CellRule::F64 => {
                ReaderImpl::F64(downcast::<Float64Array>(array, "Float64")?.clone())
            }
            CellRule::Utf8 => match array.data_type() {
                DataType::Utf8 => ReaderImpl::Utf8(array.as_string::<i32>().clone()),
                _ => ReaderImpl::LargeUtf8(array.as_string::<i64>().clone()),
            },
            CellRule::Binary => match array.data_type() {
                DataType::Binary => {
                    ReaderImpl::Binary(array.as_binary::<i32>().clone())
                }
                _ => ReaderImpl::LargeBinary(array.as_binary::<i64>().clone()),
            },
            CellRule::FixedBinary { .. } => ReaderImpl::FixedBinary(
                downcast::<FixedSizeBinaryArray>(array, "FixedSizeBinary")?.clone(),
            ),
            CellRule::Uuid => ReaderImpl::Uuid(
                downcast::<FixedSizeBinaryArray>(array, "FixedSizeBinary (UUID)")?
                    .clone(),
            ),
            CellRule::Date32 => {
                ReaderImpl::Date32(downcast::<Date32Array>(array, "Date32")?.clone())
            }
            CellRule::Time64Micros => ReaderImpl::Time64Micros(
                downcast::<Time64MicrosecondArray>(array, "Time64Microsecond")?
                    .clone(),
            ),
            CellRule::Timestamp { nanos, tz } => {
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
            CellRule::Decimal128 { precision, scale } => ReaderImpl::Decimal128 {
                arr: downcast::<Decimal128Array>(array, "Decimal128")?.clone(),
                codec: Decimal128NumericCodec::new(*precision, *scale)?,
            },
            CellRule::List {
                element, elem_oid, ..
            } => ReaderImpl::List(Box::new(list::ListReader::bind(
                array, *element, *elem_oid,
            )?)),
        };
        Ok(reader)
    }

    /// Slot-first read of a standard semantic value into a PostgreSQL Datum.
    /// No downcast happens here — the concrete array was resolved by
    /// [`Self::bind`].
    ///
    /// # Safety
    ///
    /// `row_idx` must be within the bound Arrow array, and `target` must be the
    /// semantic codec for this column's destination attribute.
    ///
    /// Builds the datum through PostgreSQL internals: a backend must be active
    /// and the caller must have switched to the memory context the varlena (or
    /// array) payload should be palloc'd into. The supplied codec must match
    /// the bound column contract; physical codecs do not parse or validate
    /// producer-owned bytes.
    /// Int32 values targeting int2 must retain the producer's PG int2 range.
    unsafe fn read_standard_datum(
        &self,
        row_idx: usize,
        target: ColumnDatumCodec,
    ) -> ArrowConversionResult<Option<pg_sys::Datum>> {
        match &self.reader {
            ReaderImpl::List(reader) => unsafe { reader.datum(row_idx) },
            _ => {
                // SAFETY: the borrowed-view `Cell` returned here is consumed
                // immediately by the bound codec below (which copies into a
                // palloc'd datum) within this reader's lifetime, so the view
                // never escapes.
                let Some(cell) = (unsafe { self.read_cell_unchecked(row_idx) })?
                else {
                    return Ok(None);
                };
                let datum = unsafe { target.cell_to_datum(cell) }
                    .map_err(ArrowConversionError::from)?;
                Ok(Some(datum))
            }
        }
    }

    /// Slot-first read using a caller-bound semantic datum codec.
    ///
    /// The codec must be bound once for the destination column before this
    /// method is used repeatedly. Its type excludes provider-selected physical
    /// codecs; those are bound by [`ArrowColumnDecoder`](crate::ArrowColumnDecoder) and use the private
    /// bound-reader path directly.
    ///
    /// # Safety
    ///
    /// `row_idx` must be within the bound Arrow array, and `target` must be the
    /// semantic codec for this column's destination attribute.
    ///
    /// Builds the datum through PostgreSQL internals: a backend must be active
    /// and the caller must have switched to the memory context the varlena (or
    /// array) payload should be palloc'd into.
    pub unsafe fn read_datum_unchecked(
        &self,
        row_idx: usize,
        target: ColumnDatumCodec,
    ) -> ArrowConversionResult<Option<pg_sys::Datum>> {
        unsafe { self.read_standard_datum(row_idx, target) }
    }

    /// Row-world read: decode the value at `row_idx` into a [`Cell`], or `None`
    /// for SQL NULL. No downcast happens here — the concrete array was resolved
    /// in [`Self::bind`]; the per-value math reuses each type module's helpers.
    ///
    /// # Safety
    ///
    /// `row_idx` must be within the bound Arrow array.
    ///
    /// This is `unsafe` because the `Cell::StringView` / `Cell::ByteaView`
    /// variants returned for text/binary columns are **zero-copy borrows** of
    /// the Arrow buffer this reader owns (raw `ptr`/`len`, no lifetime). The
    /// caller must ensure such a `Cell` does not outlive the `ColumnReader` and
    /// is not used (including via the safe `Cell: Display`, which dereferences
    /// the view) after the reader is dropped — copy/materialize it first (e.g.
    /// into a slot datum) if it must live longer. The owned variants (numeric,
    /// temporal, list arrays) carry no borrow and are always safe. Provider
    /// physical readers use the separate private `datum_codec::BoundDatumReader` type and
    /// cannot enter this API.
    pub unsafe fn read_cell_unchecked(
        &self,
        row_idx: usize,
    ) -> ArrowConversionResult<Option<Cell>> {
        macro_rules! null_guard {
            ($arr:expr) => {
                if unsafe { is_null_unchecked($arr, row_idx) } {
                    return Ok(None);
                }
            };
        }
        let cell = match &self.reader {
            ReaderImpl::Bool(a) => {
                null_guard!(a);
                Cell::Bool(unsafe { a.value_unchecked(row_idx) })
            }
            ReaderImpl::I16(a) => {
                null_guard!(a);
                Cell::I16(unsafe { a.value_unchecked(row_idx) })
            }
            ReaderImpl::I32(a) => {
                null_guard!(a);
                Cell::I32(unsafe { a.value_unchecked(row_idx) })
            }
            ReaderImpl::I64(a) => {
                null_guard!(a);
                Cell::I64(unsafe { a.value_unchecked(row_idx) })
            }
            ReaderImpl::F32(a) => {
                null_guard!(a);
                Cell::F32(unsafe { a.value_unchecked(row_idx) })
            }
            ReaderImpl::F64(a) => {
                null_guard!(a);
                Cell::F64(unsafe { a.value_unchecked(row_idx) })
            }
            ReaderImpl::Utf8(a) => {
                null_guard!(a);
                str_view_cell(unsafe { a.value_unchecked(row_idx) })
            }
            ReaderImpl::LargeUtf8(a) => {
                null_guard!(a);
                str_view_cell(unsafe { a.value_unchecked(row_idx) })
            }
            ReaderImpl::Binary(a) => {
                null_guard!(a);
                bytea_view_cell(unsafe { a.value_unchecked(row_idx) })
            }
            ReaderImpl::LargeBinary(a) => {
                null_guard!(a);
                bytea_view_cell(unsafe { a.value_unchecked(row_idx) })
            }
            ReaderImpl::FixedBinary(a) => {
                null_guard!(a);
                bytea_view_cell(unsafe { a.value_unchecked(row_idx) })
            }
            ReaderImpl::Uuid(a) => {
                null_guard!(a);
                let value = unsafe { a.value_unchecked(row_idx) };
                // SAFETY: `bind_reader` accepted only FixedSizeBinary(16) for
                // this variant, so every value slice has exactly 16 bytes.
                let bytes: [u8; 16] = unsafe { value.try_into().unwrap_unchecked() };
                // Arrow UUID bytes are RFC 4122 network order, as pgrx expects.
                Cell::Uuid(Uuid::from_bytes(bytes))
            }
            ReaderImpl::Date32(a) => {
                null_guard!(a);
                Cell::Date(temporal::pg_date_from_arrow_days(unsafe {
                    a.value_unchecked(row_idx)
                })?)
            }
            ReaderImpl::Time64Micros(a) => {
                null_guard!(a);
                Cell::Time(temporal::time_from_micros(unsafe {
                    a.value_unchecked(row_idx)
                })?)
            }
            ReaderImpl::TimestampMicros { arr, tz } => {
                null_guard!(arr);
                timestamp_cell(unsafe { arr.value_unchecked(row_idx) }, *tz)?
            }
            ReaderImpl::TimestampNanos { arr, tz } => {
                null_guard!(arr);
                let micros = temporal::unix_micros_from_nanos(unsafe {
                    arr.value_unchecked(row_idx)
                });
                timestamp_cell(micros, *tz)?
            }
            ReaderImpl::Decimal128 { arr, codec } => {
                null_guard!(arr);
                Cell::Numeric(codec.decode(unsafe { arr.value_unchecked(row_idx) })?)
            }
            ReaderImpl::List(reader) => return unsafe { reader.cell(row_idx) },
        };
        Ok(Some(cell))
    }
}

pub(crate) fn str_view_cell(s: &str) -> Cell {
    // SAFETY: s owns a live UTF-8 buffer for the lifetime of the returned
    // view; callers of the surrounding reader control that lifetime.
    Cell::StringView(unsafe { StringView::from_raw_parts(s.as_ptr(), s.len()) })
}

pub(crate) fn bytea_view_cell(bytes: &[u8]) -> Cell {
    // SAFETY: bytes remains live while the returned zero-copy cell is used.
    Cell::ByteaView(unsafe { ByteaView::from_raw_parts(bytes.as_ptr(), bytes.len()) })
}

pub(crate) fn timestamp_cell(
    unix_micros: i64,
    tz: bool,
) -> ArrowConversionResult<Cell> {
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
