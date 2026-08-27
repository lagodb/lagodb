//! Columnar read-side adapters: the two halves core's
//! [`BatchRowCursor<Source, Decoder>`](lagodb_core::batch) consumes.
//!
//! - [`ArrowBatchSource`] (impl [`AmScanBatchSource`]) feeds one Arrow
//!   `RecordBatch` at a time from any fallible Arrow batch iterator.
//! - [`ArrowColumnDecoder`] (impl [`BatchRowDecoder`]) decodes one batch row
//!   into a tuple slot, per column, keyed on the same [`ColumnRule`] the write
//!   encoder uses.
//!
//! Relation-bound mutation writers use
//! [`BoundWriteBuffer`](crate::BoundWriteBuffer) when source codecs are fixed
//! during planning.

use std::rc::Rc;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
    ListArray, RecordBatch, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow_schema::DataType;
use lagodb_core::api::{AmError, AmResult};
use lagodb_core::batch::{AmScanBatchSource, BatchRowDecoder};
use lagodb_core::tuple::{
    ByteaView, Cell, ColumnDatumCodec, ColumnDatumTarget, Decimal128NumericCodec,
    SlotColumns, StringView,
};
use pgrx::datum::Uuid;
use pgrx::pg_sys;

use crate::datum::DatumCodec;
use crate::error::{ArrowConversionError, ArrowConversionResult};
use crate::rule::{ColumnRule, ListElementRule};
use crate::types::{downcast, list, temporal};

use arrow_array::FixedSizeBinaryArray;

mod bound;

/// Read one Arrow validity bit after the enclosing bound batch established the
/// row index for every column.
///
/// # Safety
///
/// `row_idx` must be within `array`.
#[inline]
unsafe fn is_null_unchecked(array: &impl Array, row_idx: usize) -> bool {
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

/// One mapped column: which Arrow batch column to read, the resolved rule, the
/// destination slot index, and the provider-selected datum codec. A physical
/// codec is part of the bound plan so a generic Arrow type or PostgreSQL OID
/// cannot silently select provider-specific storage semantics.
pub struct DecodedColumn {
    rule: ColumnRule,
    src_col: usize,
    dest: usize,
    target_oid: pg_sys::Oid,
    codec: DatumCodec,
}

impl DecodedColumn {
    /// Construct a decoder column from a relation-bound target.
    ///
    /// # Safety
    ///
    /// `target_oid` must be the actual type OID of the destination attribute
    /// at `dest` in the live relation tuple descriptor. `dest` must be within
    /// that descriptor's slot width. The caller must also keep the
    /// relation/slot layout unchanged for the lifetime of the decoder. The
    /// constructor validates the codec/OID pairing, but it cannot inspect an
    /// arbitrary descriptor from these scalar arguments alone.
    pub unsafe fn new(
        rule: ColumnRule,
        src_col: usize,
        dest: usize,
        target_oid: pg_sys::Oid,
        codec: DatumCodec,
    ) -> ArrowConversionResult<Self> {
        codec.validate_target_oid(target_oid)?;
        if rule.requires_utf8_server_encoding() {
            ColumnDatumTarget::validate_utf8_server_encoding()
                .map_err(ArrowConversionError::from)?;
        }
        Ok(Self {
            rule,
            src_col,
            dest,
            target_oid,
            codec,
        })
    }

    /// The relation attribute OID validated against this column's codec.
    #[inline]
    pub fn target_oid(&self) -> pg_sys::Oid {
        self.target_oid
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
        element_codec: ColumnDatumCodec,
    },
}

/// Slot-first reader with the final datum conversion encoded in its variant.
///
/// Unlike [`ColumnReader`], this type cannot represent a semantic-only reader
/// or a physical codec paired with the wrong Arrow representation. Binding
/// pays those checks once per batch; the row loop performs one exhaustive
/// reader dispatch.
enum BoundDatumReader {
    Bool(BooleanArray, ColumnDatumCodec),
    I32(Int32Array, ColumnDatumCodec),
    I64(Int64Array, ColumnDatumCodec),
    F32(Float32Array, ColumnDatumCodec),
    F64(Float64Array, ColumnDatumCodec),
    Utf8(StringArray, ColumnDatumCodec),
    LargeUtf8(LargeStringArray, ColumnDatumCodec),
    Binary(BinaryArray, ColumnDatumCodec),
    LargeBinary(LargeBinaryArray, ColumnDatumCodec),
    FixedBinary(FixedSizeBinaryArray, ColumnDatumCodec),
    Uuid(FixedSizeBinaryArray, ColumnDatumCodec),
    Date32(Date32Array, ColumnDatumCodec),
    Time64Micros(Time64MicrosecondArray, ColumnDatumCodec),
    TimestampMicros {
        arr: TimestampMicrosecondArray,
        tz: bool,
        target: ColumnDatumCodec,
    },
    TimestampNanos {
        arr: TimestampNanosecondArray,
        tz: bool,
        target: ColumnDatumCodec,
    },
    Decimal128 {
        arr: Decimal128Array,
        codec: Decimal128NumericCodec,
        target: ColumnDatumCodec,
    },
    List {
        arr: ListArray,
        element: ListElementRule,
        element_codec: ColumnDatumCodec,
    },
    PrevalidatedJsonText(StringArray),
    PrevalidatedLargeJsonText(LargeStringArray),
    PostgresJsonbVarlena(BinaryArray),
    PostgresLargeJsonbVarlena(LargeBinaryArray),
}

/// A semantic batch column resolved (validated + downcast) to its concrete
/// Arrow array once, then read row by row with no per-value downcast.
///
/// This public reader belongs to the row-world / FDW `Cell` API. The
/// slot-first scan path uses the private [`BoundDatumReader`], whose variants
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
    /// codecs are represented separately by [`BoundDatumReader`] so their
    /// bytes cannot be materialized as a semantic `Cell`.
    pub fn bind(rule: &ColumnRule, array: &dyn Array) -> ArrowConversionResult<Self> {
        if matches!(rule, ColumnRule::PostgresJsonbVarlena) {
            return Err(ArrowConversionError::InvariantViolated(
                "PostgreSQL JSONB varlena columns require an explicit physical codec",
            ));
        }
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
    fn bind_reader(
        rule: &ColumnRule,
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
            ColumnRule::Binary | ColumnRule::PostgresJsonbVarlena => {
                match array.data_type() {
                    DataType::Binary => {
                        ReaderImpl::Binary(array.as_binary::<i32>().clone())
                    }
                    _ => ReaderImpl::LargeBinary(array.as_binary::<i64>().clone()),
                }
            }
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
                element_codec: ColumnDatumCodec::bind(ColumnDatumTarget::from_oid(
                    *elem_oid,
                ))
                .map_err(ArrowConversionError::from)?,
            },
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
    unsafe fn read_standard_datum(
        &self,
        row_idx: usize,
        target: ColumnDatumCodec,
    ) -> ArrowConversionResult<Option<pg_sys::Datum>> {
        match &self.reader {
            ReaderImpl::List {
                arr,
                element,
                element_codec,
            } => {
                if unsafe { is_null_unchecked(arr, row_idx) } {
                    return Ok(None);
                }
                Ok(Some(unsafe {
                    list::array_datum_at(arr, row_idx, *element, *element_codec)
                }?))
            }
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
    /// codecs; those are bound by [`ArrowColumnDecoder`] and use the private
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
    /// physical readers use the separate private [`BoundDatumReader`] type and
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
            ReaderImpl::List { arr, element, .. } => {
                null_guard!(arr);
                // Row-world list cells keep the Arrow *physical* element width
                // (see the narrowing TODO in `types::list`); the bound datum
                // path uses its element-OID-aware route instead.
                unsafe { list::cell_at(arr, row_idx, *element) }?
            }
        };
        Ok(Some(cell))
    }
}

fn str_view_cell(s: &str) -> Cell {
    // SAFETY: s owns a live UTF-8 buffer for the lifetime of the returned
    // view; callers of the surrounding reader control that lifetime.
    Cell::StringView(unsafe { StringView::from_raw_parts(s.as_ptr(), s.len()) })
}

fn bytea_view_cell(bytes: &[u8]) -> Cell {
    // SAFETY: bytes remains live while the returned zero-copy cell is used.
    Cell::ByteaView(unsafe { ByteaView::from_raw_parts(bytes.as_ptr(), bytes.len()) })
}

fn timestamp_cell(unix_micros: i64, tz: bool) -> ArrowConversionResult<Cell> {
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
    reader: BoundDatumReader,
    dest: usize,
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
///
/// The immutable column plan is reference-counted with [`Rc`]: PostgreSQL scan
/// state is backend-local, while rescans need cheap decoder handles without
/// rebuilding or cloning every column rule. The row loop still iterates the
/// slice directly and never touches the reference count.
#[derive(Clone)]
pub struct ArrowColumnDecoder {
    columns: Rc<[DecodedColumn]>,
}

impl ArrowColumnDecoder {
    pub fn new(columns: Vec<DecodedColumn>) -> Self {
        Self {
            columns: Rc::from(columns.into_boxed_slice()),
        }
    }

    /// Decode one row directly into a slot whose layout is the one used to
    /// validate the decoder's [`DecodedColumn`] destinations.
    ///
    /// This is the trusted-provider physical path. It is deliberately an
    /// unsafe method instead of being the implementation of the safe
    /// [`BatchRowDecoder::write_row`] API: the decoder stores scalar
    /// destinations, not a type-level association with a particular
    /// `SlotColumns` descriptor.
    ///
    /// # Safety
    ///
    /// `bound` must have been produced by this decoder. Every `col.dest` in
    /// `bound` must be within `out.natts()`, and `out` must be backed by the
    /// same relation slot layout whose width and
    /// destination mapping were validated when these `DecodedColumn`s were
    /// constructed. `row_idx` must be within `bound` and every bound Arrow
    /// array. The caller must also satisfy the memory-context requirements of
    /// [`BoundDatumReader::read_datum_unchecked`].
    #[inline]
    pub unsafe fn write_row_unchecked(
        &self,
        bound: &BoundBatch,
        row_idx: usize,
        out: &mut SlotColumns<'_>,
    ) -> AmResult<()> {
        for col in bound.columns.iter() {
            // The shim already switched to the slot's target context, so the
            // varlena/array palloc lands where the per-row reset expects it.
            let datum = unsafe { col.reader.read_datum_unchecked(row_idx) }?;
            // SAFETY: guaranteed by this method's contract.
            unsafe { out.set_datum_unchecked(col.dest, datum) };
        }
        Ok(())
    }
}

impl BatchRowDecoder for ArrowColumnDecoder {
    type Batch = RecordBatch;
    type Bound = BoundBatch;

    /// Resolve every mapped column to its concrete typed array, once per batch.
    /// Each `src_col` is bounds-checked and its array validated + downcast here
    /// (see [`ColumnReader::bind`]), so `write_row` does no per-value downcast
    /// and a producer/plan type drift surfaces as a clean
    /// [`ArrowConversionError::ArrowTypeMismatch`] at the batch boundary.
    fn bind(&self, batch: RecordBatch) -> AmResult<BoundBatch> {
        let num_rows = batch.num_rows();
        let mut columns = Vec::with_capacity(self.columns.len());
        for col in self.columns.iter() {
            let array = batch.columns().get(col.src_col).ok_or_else(|| {
                ArrowConversionError::ArrowTypeMismatch(
                    format!(
                        "batch has {} columns but the plan reads column {}",
                        batch.num_columns(),
                        col.src_col
                    )
                    .into(),
                )
            })?;
            columns.push(BoundColumn {
                reader: BoundDatumReader::bind(&col.rule, array.as_ref(), col.codec)?,
                dest: col.dest,
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

    /// Safe generic path: the destination bound is checked by `SlotColumns`.
    /// Trusted relation-bound providers can use
    /// [`Self::write_row_unchecked`] after upholding its layout contract.
    fn write_row(
        &self,
        bound: &BoundBatch,
        row_idx: usize,
        out: &mut SlotColumns<'_>,
    ) -> AmResult<()> {
        if row_idx >= bound.num_rows {
            return Err(ArrowConversionError::InvariantViolated(
                "batch row index is out of bounds",
            )
            .into());
        }
        for col in bound.columns.iter() {
            // The shim already switched to the slot's target context, so the
            // varlena/array palloc lands where the per-row reset expects it.
            let datum = unsafe { col.reader.read_datum_unchecked(row_idx) }?;
            out.set_datum(col.dest, datum);
        }
        Ok(())
    }
}
