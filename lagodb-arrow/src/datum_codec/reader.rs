//! Slot-first behavior for a batch-bound datum reader.

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use lagodb_core::tuple::{Cell, ColumnDatumCodec, Decimal128NumericCodec};
use pgrx::{datum::Uuid, pg_sys};

use super::recursive_read::RecursiveDatumReader;
use crate::column_encoder::temporal;
use crate::jsonb::BoundNestedJsonb;
use crate::read::list::ListReader;
use crate::read::{
    ColumnReader, ReaderImpl, bytea_view_cell, is_null_unchecked, str_view_cell,
    timestamp_cell,
};
use crate::{ArrowConversionError, ArrowConversionResult, CellRule, DatumCodec};

/// Slot-first reader with the final datum conversion encoded in its variant.
///
/// Unlike [`ColumnReader`], this type cannot represent a semantic-only reader
/// or a physical codec paired with the wrong Arrow representation. Binding
/// pays those checks once per batch. Each value dispatches on the bound variant;
/// recursive containers then visit their bound child readers.
pub(crate) enum BoundDatumReader {
    Recursive(Box<RecursiveDatumReader>),
    NestedJsonb(Box<BoundNestedJsonb>),
    Bool(BooleanArray, ColumnDatumCodec),
    I16(Int16Array, ColumnDatumCodec),
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
        target_typmod: i32,
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
    /// Install a declared numeric target once per batch; physical readers keep
    /// their source typmod. Other scalar readers have no numeric conversion.
    pub(super) fn with_numeric_typmod(mut self, typmod: i32) -> Self {
        if let Self::Decimal128 { target_typmod, .. } = &mut self {
            *target_typmod = typmod;
        }
        self
    }

    /// # Safety
    /// `rule.accepts(array.data_type())` must hold for the complete type.
    pub(crate) unsafe fn bind_unchecked(
        rule: &CellRule,
        array: &dyn Array,
        codec: DatumCodec,
    ) -> ArrowConversionResult<Self> {
        // SAFETY: inherited from this binding entry point.
        let reader = unsafe { ColumnReader::bind_reader_unchecked(rule, array) }?;
        if let Some(target) = codec.standard_target() {
            return Self::from_standard(reader, target);
        }

        if codec.is_prevalidated_json_text() {
            return match reader {
                ReaderImpl::Utf8(array) => Ok(Self::PrevalidatedJsonText(array)),
                ReaderImpl::LargeUtf8(array) => {
                    Ok(Self::PrevalidatedLargeJsonText(array))
                }
                _ => Err(ArrowConversionError::InvariantViolated(
                    "the prevalidated JSON text codec requires an Utf8 rule",
                )),
            };
        }

        if codec.is_postgres_numeric_varlena() {
            if !matches!(rule, CellRule::Binary) {
                return Err(ArrowConversionError::InvariantViolated(
                    "the PostgreSQL NUMERIC varlena codec requires a binary rule",
                ));
            }
            return match reader {
                ReaderImpl::Binary(array) => Ok(Self::PostgresNumericVarlena(array)),
                ReaderImpl::LargeBinary(array) => {
                    Ok(Self::PostgresLargeNumericVarlena(array))
                }
                _ => Err(ArrowConversionError::InvariantViolated(
                    "the PostgreSQL NUMERIC varlena codec requires a binary rule",
                )),
            };
        }

        if codec.is_numeric_from_int64() {
            return match reader {
                ReaderImpl::I64(array) => Ok(Self::NumericFromInt64(array)),
                _ => Err(ArrowConversionError::InvariantViolated(
                    "the Int64-to-NUMERIC codec requires an Int64 rule",
                )),
            };
        }

        if codec.is_numeric_from_float64() {
            return match reader {
                ReaderImpl::F64(array) => Ok(Self::NumericFromFloat64(array)),
                _ => Err(ArrowConversionError::InvariantViolated(
                    "the Float64-to-NUMERIC codec requires a Float64 rule",
                )),
            };
        }

        Err(ArrowConversionError::InvariantViolated(
            "unknown physical datum codec",
        ))
    }

    fn from_standard(
        reader: ReaderImpl,
        target: ColumnDatumCodec,
    ) -> ArrowConversionResult<Self> {
        Ok(match reader {
            ReaderImpl::Bool(array) => Self::Bool(array, target),
            ReaderImpl::I16(array) => Self::I16(array, target),
            ReaderImpl::I32(array) => Self::I32(array, target),
            ReaderImpl::I64(array) => Self::I64(array, target),
            ReaderImpl::F32(array) => Self::F32(array, target),
            ReaderImpl::F64(array) => Self::F64(array, target),
            ReaderImpl::Utf8(array) => Self::Utf8(array, target),
            ReaderImpl::LargeUtf8(array) => Self::LargeUtf8(array, target),
            ReaderImpl::Binary(array) => Self::Binary(array, target),
            ReaderImpl::LargeBinary(array) => Self::LargeBinary(array, target),
            ReaderImpl::FixedBinary(array) => Self::FixedBinary(array, target),
            ReaderImpl::Uuid(array) => Self::Uuid(array, target),
            ReaderImpl::Date32(array) => Self::Date32(array, target),
            ReaderImpl::Time64Micros(array) => Self::Time64Micros(array, target),
            ReaderImpl::TimestampMicros { arr, tz } => {
                Self::TimestampMicros { arr, tz, target }
            }
            ReaderImpl::TimestampNanos { arr, tz } => {
                Self::TimestampNanos { arr, tz, target }
            }
            ReaderImpl::Decimal128 { arr, codec } => {
                // Direct Datum decoding bypasses Cell's target dispatch, so
                // establish its numeric destination once while binding.
                if target.oid() != pg_sys::NUMERICOID {
                    return Err(ArrowConversionError::InvariantViolated(
                        "the Decimal128 datum reader requires a NUMERIC target",
                    ));
                }
                Self::Decimal128 {
                    arr,
                    target_typmod: codec.typmod(),
                    codec,
                }
            }
            ReaderImpl::List(reader) => Self::List(reader),
        })
    }

    /// Read one datum after the enclosing [`BoundBatch`](super::BoundBatch) established the row
    /// index against every array in the batch.
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
        macro_rules! standard {
            ($array:expr, $target:expr, $cell:expr) => {{
                if unsafe { is_null_unchecked($array, row_idx) } {
                    return Ok(None);
                }
                let cell = $cell;
                unsafe { $target.cell_to_datum(cell) }
                    .map(Some)
                    .map_err(ArrowConversionError::from)
            }};
        }
        match self {
            Self::Recursive(reader) => unsafe { reader.read_datum(row_idx) },
            Self::NestedJsonb(reader) => unsafe { reader.read_datum(row_idx) },
            Self::Bool(array, target) => standard!(
                array,
                *target,
                Cell::Bool(unsafe { array.value_unchecked(row_idx) })
            ),
            Self::I16(array, target) => standard!(
                array,
                *target,
                Cell::I16(unsafe { array.value_unchecked(row_idx) })
            ),
            Self::I32(array, target) => standard!(
                array,
                *target,
                Cell::I32(unsafe { array.value_unchecked(row_idx) })
            ),
            Self::I64(array, target) => standard!(
                array,
                *target,
                Cell::I64(unsafe { array.value_unchecked(row_idx) })
            ),
            Self::F32(array, target) => standard!(
                array,
                *target,
                Cell::F32(unsafe { array.value_unchecked(row_idx) })
            ),
            Self::F64(array, target) => standard!(
                array,
                *target,
                Cell::F64(unsafe { array.value_unchecked(row_idx) })
            ),
            Self::Utf8(array, target) => standard!(
                array,
                *target,
                str_view_cell(unsafe { array.value_unchecked(row_idx) })
            ),
            Self::LargeUtf8(array, target) => standard!(
                array,
                *target,
                str_view_cell(unsafe { array.value_unchecked(row_idx) })
            ),
            Self::Binary(array, target) => standard!(
                array,
                *target,
                bytea_view_cell(unsafe { array.value_unchecked(row_idx) })
            ),
            Self::LargeBinary(array, target) => standard!(
                array,
                *target,
                bytea_view_cell(unsafe { array.value_unchecked(row_idx) })
            ),
            Self::FixedBinary(array, target) => standard!(
                array,
                *target,
                bytea_view_cell(unsafe { array.value_unchecked(row_idx) })
            ),
            Self::Uuid(array, target) => {
                if unsafe { is_null_unchecked(array, row_idx) } {
                    return Ok(None);
                }
                let value = unsafe { array.value_unchecked(row_idx) };
                // SAFETY: this variant is constructed only from
                // FixedSizeBinary(16).
                let bytes: [u8; 16] = unsafe { value.try_into().unwrap_unchecked() };
                unsafe { target.cell_to_datum(Cell::Uuid(Uuid::from_bytes(bytes))) }
                    .map(Some)
                    .map_err(ArrowConversionError::from)
            }
            Self::Date32(array, target) => standard!(
                array,
                *target,
                Cell::Date(temporal::pg_date_from_arrow_days(unsafe {
                    array.value_unchecked(row_idx)
                })?)
            ),
            Self::Time64Micros(array, target) => standard!(
                array,
                *target,
                Cell::Time(temporal::time_from_micros(unsafe {
                    array.value_unchecked(row_idx)
                })?)
            ),
            Self::TimestampMicros { arr, tz, target } => standard!(
                arr,
                *target,
                timestamp_cell(unsafe { arr.value_unchecked(row_idx) }, *tz)?
            ),
            Self::TimestampNanos { arr, tz, target } => {
                standard!(
                    arr,
                    *target,
                    timestamp_cell(
                        temporal::unix_micros_from_nanos(unsafe {
                            arr.value_unchecked(row_idx)
                        }),
                        *tz,
                    )?
                )
            }
            Self::Decimal128 {
                arr,
                codec,
                target_typmod,
            } => {
                if unsafe { is_null_unchecked(arr, row_idx) } {
                    return Ok(None);
                }
                // Keep the numeric_recv result in the destination PG context.
                // The owned Cell reader uses decode(); using it here would
                // copy PG Numeric into AnyNumeric and back, and register
                // per-value error handlers. As in the JSONB decimal reader,
                // PG errors retain their original SQLSTATE at the outer FFI
                // boundary; numeric_recv applies the bound target typmod.
                // SAFETY: binding established the numeric target and codec;
                // the caller supplies a valid row and destination context.
                let datum = unsafe {
                    codec.decode_datum_with_typmod(
                        arr.value_unchecked(row_idx),
                        *target_typmod,
                    )
                };
                Ok(Some(datum))
            }
            Self::List(reader) => unsafe { reader.datum(row_idx) },
            Self::PrevalidatedJsonText(array) => {
                if unsafe { is_null_unchecked(array, row_idx) } {
                    return Ok(None);
                }
                let text: &str = unsafe { array.value_unchecked(row_idx) };
                unsafe { DatumCodec::copy_prevalidated_json_text(text) }.map(Some)
            }
            Self::PrevalidatedLargeJsonText(array) => {
                if unsafe { is_null_unchecked(array, row_idx) } {
                    return Ok(None);
                }
                let text: &str = unsafe { array.value_unchecked(row_idx) };
                unsafe { DatumCodec::copy_prevalidated_json_text(text) }.map(Some)
            }
            Self::PostgresNumericVarlena(array) => {
                if unsafe { is_null_unchecked(array, row_idx) } {
                    return Ok(None);
                }
                let bytes: &[u8] = unsafe { array.value_unchecked(row_idx) };
                unsafe { DatumCodec::copy_postgres_numeric_varlena(bytes) }.map(Some)
            }
            Self::PostgresLargeNumericVarlena(array) => {
                if unsafe { is_null_unchecked(array, row_idx) } {
                    return Ok(None);
                }
                let bytes: &[u8] = unsafe { array.value_unchecked(row_idx) };
                unsafe { DatumCodec::copy_postgres_numeric_varlena(bytes) }.map(Some)
            }
            Self::NumericFromInt64(array) => {
                if unsafe { is_null_unchecked(array, row_idx) } {
                    return Ok(None);
                }
                let value = unsafe { array.value_unchecked(row_idx) };
                DatumCodec::numeric_datum_from_int64(value).map(Some)
            }
            Self::NumericFromFloat64(array) => {
                if unsafe { is_null_unchecked(array, row_idx) } {
                    return Ok(None);
                }
                let value = unsafe { array.value_unchecked(row_idx) };
                DatumCodec::numeric_datum_from_float64(value).map(Some)
            }
        }
    }
}
