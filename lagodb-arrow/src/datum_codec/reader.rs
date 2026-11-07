//! Scalar datum conversion: resources bound once per decoder ([`ScalarRead`])
//! and the batch-bound reader ([`BoundDatumReader`]).
//!
//! Fixed-width PostgreSQL types are produced directly with their `XGetDatum`
//! representation, selected while binding. Text and name use the bound core
//! text constructors; JSON, bytea and list elements keep the target-aware core
//! codec, and numeric uses the shared decimal codec. Binding enumerates every
//! admitted `(CellRule, target OID)` pair, so an incompatible pair fails before
//! any batch is read.

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Time64MicrosecondType, TimestampMicrosecondType,
    TimestampNanosecondType,
};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow_schema::DataType;
use lagodb_core::datum::{
    ArrayDatumLayout, BoundColumnDatumCodec, ByteaView, Cell, ColumnDatumTarget,
    Decimal128NumericCodec, StringView,
};
use pgrx::pg_sys;

use super::codec::{DatumCodec, DatumCodecKind};
use super::list_read::{ListRead, ListReader};
use super::recursive_read::RecursiveDatumReader;
use crate::error::{ArrowConversionError, ArrowConversionResult};
use crate::jsonb::BoundNestedJsonb;
use crate::rule::CellRule;
use crate::temporal::{
    pg_date_from_arrow_days, time_from_micros, timestamp_from_unix_micros,
    unix_micros_from_nanos,
};

const UUID_LEN: usize = 16;

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

/// Borrow Arrow text for the core codec. The view is consumed by the codec,
/// which copies it into a PostgreSQL datum before the array can be released.
pub(super) fn str_view_cell(s: &str) -> Cell {
    // SAFETY: s owns a live UTF-8 buffer for the lifetime of the returned
    // view; every caller converts the view before returning.
    Cell::StringView(unsafe { StringView::from_raw_parts(s.as_ptr(), s.len()) })
}

fn bytea_view_cell(bytes: &[u8]) -> Cell {
    // SAFETY: bytes remains live while the returned zero-copy cell is used.
    Cell::ByteaView(unsafe { ByteaView::from_raw_parts(bytes.as_ptr(), bytes.len()) })
}

/// The scalar conversion and its decoder-owned resources, selected once from a
/// `(CellRule, DatumCodec)` pair before any batch is bound.
pub(crate) enum ScalarRead {
    Bool,
    Int16,
    Int32,
    /// Provider-widened int2. The producer preserves the int2 value domain.
    Int32AsInt2,
    Int64,
    Float32,
    Float64,
    Date32,
    Time64Micros,
    /// timestamp or timestamptz. Both share PostgreSQL's `IS_VALID_TIMESTAMP`
    /// range and `TimestampGetDatum` representation, so the bound target type
    /// needs no per-value branch.
    Timestamp {
        nanos: bool,
    },
    Uuid,
    /// text, varchar or bpchar.
    Text(BoundColumnDatumCodec),
    Name(BoundColumnDatumCodec),
    /// json or jsonb text parsed by the PostgreSQL input function.
    Json(BoundColumnDatumCodec),
    Bytea(BoundColumnDatumCodec),
    Decimal128 {
        codec: Decimal128NumericCodec,
        typmod: i32,
    },
    List(ListRead),
    PrevalidatedJsonText,
    PostgresNumericVarlena,
    NumericFromInt64,
    NumericFromFloat64,
}

impl ScalarRead {
    /// Select the conversion for an Arrow rule and its destination codec.
    ///
    /// `typmod` is the declared numeric target typmod; `None` keeps the Arrow
    /// source precision and scale as the target typmod.
    pub(crate) fn bind(
        rule: &CellRule,
        codec: DatumCodec,
        typmod: Option<i32>,
    ) -> ArrowConversionResult<Self> {
        let target = match codec.kind {
            DatumCodecKind::Standard(target) => target,
            DatumCodecKind::PrevalidatedJsonText => {
                return Self::physical(
                    matches!(rule, CellRule::Utf8),
                    Self::PrevalidatedJsonText,
                );
            }
            DatumCodecKind::PostgresNumericVarlena => {
                return Self::physical(
                    matches!(rule, CellRule::Binary),
                    Self::PostgresNumericVarlena,
                );
            }
            DatumCodecKind::NumericFromInt64 => {
                return Self::physical(
                    matches!(rule, CellRule::I64),
                    Self::NumericFromInt64,
                );
            }
            DatumCodecKind::NumericFromFloat64 => {
                return Self::physical(
                    matches!(rule, CellRule::F64),
                    Self::NumericFromFloat64,
                );
            }
        };
        let read = match (rule, target.oid()) {
            (CellRule::Bool, pg_sys::BOOLOID) => Self::Bool,
            (CellRule::I16, pg_sys::INT2OID) => Self::Int16,
            (CellRule::I32, pg_sys::INT2OID) => Self::Int32AsInt2,
            (CellRule::I32, pg_sys::INT4OID) => Self::Int32,
            (CellRule::I64, pg_sys::INT8OID) => Self::Int64,
            (CellRule::F32, pg_sys::FLOAT4OID) => Self::Float32,
            (CellRule::F64, pg_sys::FLOAT8OID) => Self::Float64,
            (CellRule::Date32, pg_sys::DATEOID) => Self::Date32,
            (CellRule::Time64Micros, pg_sys::TIMEOID) => Self::Time64Micros,
            (CellRule::Timestamp { nanos, tz: false }, pg_sys::TIMESTAMPOID)
            | (CellRule::Timestamp { nanos, tz: true }, pg_sys::TIMESTAMPTZOID) => {
                Self::Timestamp { nanos: *nanos }
            }
            (CellRule::Uuid, pg_sys::UUIDOID) => Self::Uuid,
            (
                CellRule::Utf8,
                pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::BPCHAROID,
            ) => Self::Text(target),
            (CellRule::Utf8, pg_sys::NAMEOID) => Self::Name(target),
            (CellRule::Utf8, pg_sys::JSONOID | pg_sys::JSONBOID) => {
                Self::Json(target)
            }
            (CellRule::Binary | CellRule::FixedBinary { .. }, pg_sys::BYTEAOID) => {
                Self::Bytea(target)
            }
            (CellRule::Decimal128 { precision, scale }, pg_sys::NUMERICOID) => {
                let codec = Decimal128NumericCodec::new(*precision, *scale)?;
                Self::Decimal128 {
                    codec,
                    typmod: typmod.unwrap_or_else(|| codec.typmod()),
                }
            }
            // The element kind must materialize into the rule's element OID,
            // exactly as `CellRule::resolve` requires, and the target must be
            // the array of that element. SAFETY: `get_element_type` only reads
            // the pg_type syscache and returns InvalidOid for a non-array target.
            (
                CellRule::List {
                    element, elem_oid, ..
                },
                array_oid,
            ) if element.accepts_target_oid(*elem_oid)
                && unsafe { pg_sys::get_element_type(array_oid) } == *elem_oid =>
            {
                Self::List(ListRead::new(
                    *element,
                    BoundColumnDatumCodec::bind(ColumnDatumTarget::from_oid(
                        *elem_oid,
                    ))?,
                    ArrayDatumLayout::bind(*elem_oid),
                ))
            }
            _ => {
                return Err(ArrowConversionError::IncompatibleColumnType(
                    format!("{rule:?}"),
                    format!("PostgreSQL type OID {}", u32::from(target.oid())),
                ));
            }
        };
        Ok(read)
    }

    /// Admit a physical codec only with the one Arrow rule it decodes.
    fn physical(paired: bool, read: Self) -> ArrowConversionResult<Self> {
        if paired {
            Ok(read)
        } else {
            Err(ArrowConversionError::InvariantViolated(
                "physical datum codec is paired with an incompatible Arrow rule",
            ))
        }
    }

    /// Bind one batch's concrete array.
    ///
    /// # Safety
    /// `array` must satisfy the complete rule this conversion was bound from,
    /// including decimal scale, fixed binary width and list element type.
    pub(crate) unsafe fn bind_array(&self, array: &dyn Array) -> BoundDatumReader {
        use BoundDatumReader as R;

        match self {
            Self::Bool => R::Bool(array.as_boolean().clone()),
            Self::Int16 => R::Int16(array.as_primitive::<Int16Type>().clone()),
            Self::Int32 => R::Int32(array.as_primitive::<Int32Type>().clone()),
            Self::Int32AsInt2 => {
                R::Int32AsInt2(array.as_primitive::<Int32Type>().clone())
            }
            Self::Int64 => R::Int64(array.as_primitive::<Int64Type>().clone()),
            Self::Float32 => R::Float32(array.as_primitive::<Float32Type>().clone()),
            Self::Float64 => R::Float64(array.as_primitive::<Float64Type>().clone()),
            Self::Date32 => R::Date32(array.as_primitive::<Date32Type>().clone()),
            Self::Time64Micros => {
                R::Time64Micros(array.as_primitive::<Time64MicrosecondType>().clone())
            }
            Self::Timestamp { nanos: false } => R::TimestampMicros(
                array.as_primitive::<TimestampMicrosecondType>().clone(),
            ),
            Self::Timestamp { nanos: true } => R::TimestampNanos(
                array.as_primitive::<TimestampNanosecondType>().clone(),
            ),
            Self::Uuid => R::Uuid(array.as_fixed_size_binary().clone()),
            Self::Text(target) => match array.data_type() {
                DataType::Utf8 => R::Text(array.as_string::<i32>().clone(), *target),
                _ => R::LargeText(array.as_string::<i64>().clone(), *target),
            },
            Self::Name(target) => match array.data_type() {
                DataType::Utf8 => R::Name(array.as_string::<i32>().clone(), *target),
                _ => R::LargeName(array.as_string::<i64>().clone(), *target),
            },
            Self::Json(target) => match array.data_type() {
                DataType::Utf8 => R::Json(array.as_string::<i32>().clone(), *target),
                _ => R::LargeJson(array.as_string::<i64>().clone(), *target),
            },
            Self::Bytea(target) => match array.data_type() {
                DataType::Binary => {
                    R::Binary(array.as_binary::<i32>().clone(), *target)
                }
                DataType::LargeBinary => {
                    R::LargeBinary(array.as_binary::<i64>().clone(), *target)
                }
                _ => R::FixedBinary(array.as_fixed_size_binary().clone(), *target),
            },
            Self::Decimal128 { codec, typmod } => R::Decimal128 {
                arr: array.as_primitive::<Decimal128Type>().clone(),
                codec: *codec,
                typmod: *typmod,
            },
            Self::List(list) => R::List(Box::new(unsafe { list.bind(array) })),
            Self::PrevalidatedJsonText => match array.data_type() {
                DataType::Utf8 => {
                    R::PrevalidatedJsonText(array.as_string::<i32>().clone())
                }
                _ => R::PrevalidatedLargeJsonText(array.as_string::<i64>().clone()),
            },
            Self::PostgresNumericVarlena => match array.data_type() {
                DataType::Binary => {
                    R::PostgresNumericVarlena(array.as_binary::<i32>().clone())
                }
                _ => R::PostgresLargeNumericVarlena(array.as_binary::<i64>().clone()),
            },
            Self::NumericFromInt64 => {
                R::NumericFromInt64(array.as_primitive::<Int64Type>().clone())
            }
            Self::NumericFromFloat64 => {
                R::NumericFromFloat64(array.as_primitive::<Float64Type>().clone())
            }
        }
    }
}

/// Slot-first reader with the final datum conversion encoded in its variant.
///
/// It cannot represent a physical codec paired with the wrong Arrow
/// representation: binding resolved those pairs before any batch. Each value
/// dispatches on the bound variant; recursive containers then visit their
/// bound child readers.
pub(crate) enum BoundDatumReader {
    Recursive(Box<RecursiveDatumReader>),
    NestedJsonb(Box<BoundNestedJsonb>),
    Bool(BooleanArray),
    Int16(Int16Array),
    Int32(Int32Array),
    Int32AsInt2(Int32Array),
    Int64(Int64Array),
    Float32(Float32Array),
    Float64(Float64Array),
    Date32(Date32Array),
    Time64Micros(Time64MicrosecondArray),
    TimestampMicros(TimestampMicrosecondArray),
    TimestampNanos(TimestampNanosecondArray),
    Uuid(FixedSizeBinaryArray),
    Text(StringArray, BoundColumnDatumCodec),
    LargeText(LargeStringArray, BoundColumnDatumCodec),
    Name(StringArray, BoundColumnDatumCodec),
    LargeName(LargeStringArray, BoundColumnDatumCodec),
    Json(StringArray, BoundColumnDatumCodec),
    LargeJson(LargeStringArray, BoundColumnDatumCodec),
    Binary(BinaryArray, BoundColumnDatumCodec),
    LargeBinary(LargeBinaryArray, BoundColumnDatumCodec),
    FixedBinary(FixedSizeBinaryArray, BoundColumnDatumCodec),
    Decimal128 {
        arr: Decimal128Array,
        codec: Decimal128NumericCodec,
        typmod: i32,
    },
    List(Box<ListReader>),
    PrevalidatedJsonText(StringArray),
    PrevalidatedLargeJsonText(LargeStringArray),
    PostgresNumericVarlena(BinaryArray),
    PostgresLargeNumericVarlena(LargeBinaryArray),
    NumericFromInt64(Int64Array),
    NumericFromFloat64(Float64Array),
}

impl BoundDatumReader {
    /// Read one datum after the enclosing [`BoundBatch`](super::BoundBatch)
    /// established the row index against every array in the batch.
    ///
    /// # Safety
    ///
    /// `row_idx` must be within the bound RecordBatch. PostgreSQL must be
    /// active with the destination memory context selected.
    /// PostgreSQL errors unwind to the enclosing provider/FFI boundary.
    #[inline]
    pub(crate) unsafe fn read_datum_unchecked(
        &self,
        row_idx: usize,
    ) -> ArrowConversionResult<Option<pg_sys::Datum>> {
        // Resolve validity, then evaluate the variant's present-value datum.
        macro_rules! present {
            ($array:expr, |$value:ident| $datum:expr) => {{
                if unsafe { is_null_unchecked($array, row_idx) } {
                    return Ok(None);
                }
                let $value = unsafe { $array.value_unchecked(row_idx) };
                $datum
            }};
        }
        // Convert a borrowed view through the bound core codec.
        macro_rules! codec {
            ($array:expr, $target:expr, $view:expr) => {
                present!($array, |value| unsafe {
                    $target.cell_to_datum($view(value))
                }?)
            };
        }
        let datum = match self {
            Self::Recursive(reader) => return unsafe { reader.read_datum(row_idx) },
            Self::NestedJsonb(reader) => {
                return unsafe { reader.read_datum(row_idx) };
            }
            Self::List(reader) => return unsafe { reader.datum(row_idx) },
            Self::Bool(array) => present!(array, |value| pg_sys::Datum::from(value)),
            Self::Int16(array) => present!(array, |value| pg_sys::Datum::from(value)),
            Self::Int32(array) => present!(array, |value| pg_sys::Datum::from(value)),
            Self::Int32AsInt2(array) => {
                present!(array, |value| pg_sys::Datum::from(value as i16))
            }
            Self::Int64(array) => present!(array, |value| pg_sys::Datum::from(value)),
            Self::Float32(array) => {
                present!(array, |value| pg_sys::Datum::from(value.to_bits()))
            }
            Self::Float64(array) => {
                present!(array, |value| pg_sys::Datum::from(value.to_bits()))
            }
            Self::Date32(array) => present!(array, |value| pg_sys::Datum::from(
                pg_sys::DateADT::from(pg_date_from_arrow_days(value)?)
            )),
            Self::Time64Micros(array) => {
                present!(array, |value| pg_sys::Datum::from(pg_sys::TimeADT::from(
                    time_from_micros(value)?
                )))
            }
            Self::TimestampMicros(array) => {
                present!(array, |value| timestamp_datum(value)?)
            }
            Self::TimestampNanos(array) => present!(array, |value| {
                timestamp_datum(unix_micros_from_nanos(value))?
            }),
            Self::Uuid(array) => present!(array, |value| {
                // UuidPGetDatum of a palloc'd pg_uuid_t. Binding accepted only
                // FixedSizeBinary(16); Arrow stores RFC 4122 network order.
                unsafe {
                    let uuid = pg_sys::palloc(UUID_LEN).cast::<u8>();
                    uuid.copy_from_nonoverlapping(value.as_ptr(), UUID_LEN);
                    pg_sys::Datum::from(uuid)
                }
            }),
            Self::Text(array, target) => present!(array, |value| unsafe {
                target.text_datum_from_str_unchecked(value)
            }?),
            Self::LargeText(array, target) => present!(array, |value| unsafe {
                target.text_datum_from_str_unchecked(value)
            }?),
            Self::Name(array, target) => present!(array, |value| unsafe {
                target.name_datum_from_str_unchecked(value)
            }?),
            Self::LargeName(array, target) => present!(array, |value| unsafe {
                target.name_datum_from_str_unchecked(value)
            }?),
            Self::Json(array, target) => codec!(array, target, str_view_cell),
            Self::LargeJson(array, target) => codec!(array, target, str_view_cell),
            Self::Binary(array, target) => codec!(array, target, bytea_view_cell),
            Self::LargeBinary(array, target) => {
                codec!(array, target, bytea_view_cell)
            }
            Self::FixedBinary(array, target) => {
                codec!(array, target, bytea_view_cell)
            }
            // Keep the numeric_recv result in the destination PG context.
            // numeric_recv applies the bound target typmod; PG errors retain
            // their SQLSTATE at the outer FFI boundary.
            Self::Decimal128 { arr, codec, typmod } => {
                present!(arr, |value| unsafe {
                    codec.decode_datum_with_typmod(value, *typmod)
                })
            }
            Self::PrevalidatedJsonText(array) => present!(array, |value| unsafe {
                DatumCodec::copy_prevalidated_json_text(value)
            }?),
            Self::PrevalidatedLargeJsonText(array) => {
                present!(array, |value| unsafe {
                    DatumCodec::copy_prevalidated_json_text(value)
                }?)
            }
            Self::PostgresNumericVarlena(array) => present!(array, |value| unsafe {
                DatumCodec::copy_postgres_numeric_varlena(value)
            }),
            Self::PostgresLargeNumericVarlena(array) => {
                present!(array, |value| unsafe {
                    DatumCodec::copy_postgres_numeric_varlena(value)
                })
            }
            Self::NumericFromInt64(array) => {
                present!(array, |value| DatumCodec::numeric_datum_from_int64(value)?)
            }
            Self::NumericFromFloat64(array) => {
                present!(array, |value| DatumCodec::numeric_datum_from_float64(
                    value
                )?)
            }
        };
        Ok(Some(datum))
    }
}

/// `TimestampGetDatum` / `TimestampTzGetDatum` after PostgreSQL's
/// `IS_VALID_TIMESTAMP` range check, which both target types share; the two
/// produce the same Datum for the same microseconds.
fn timestamp_datum(unix_micros: i64) -> ArrowConversionResult<pg_sys::Datum> {
    let pg_micros = pg_sys::Timestamp::from(timestamp_from_unix_micros(unix_micros)?);
    Ok(pg_sys::Datum::from(pg_micros))
}
