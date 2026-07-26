//! Slot-first behavior for a batch-bound datum reader.

use super::*;

impl BoundDatumReader {
    pub(super) fn bind(
        rule: &ColumnRule,
        array: &dyn Array,
        codec: DatumCodec,
    ) -> ArrowConversionResult<Self> {
        let reader = ColumnReader::bind_reader(rule, array)?;
        if let Some(target) = codec.standard_target() {
            if matches!(rule, ColumnRule::PostgresJsonbVarlena) {
                return Err(ArrowConversionError::InvariantViolated(
                    "the PostgreSQL JSONB varlena rule requires its physical codec",
                ));
            }
            return Ok(Self::from_standard(reader, target));
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

        if codec.is_postgres_jsonb_varlena() {
            if !matches!(rule, ColumnRule::PostgresJsonbVarlena) {
                return Err(ArrowConversionError::InvariantViolated(
                    "the PostgreSQL JSONB varlena codec requires its physical rule",
                ));
            }
            return match reader {
                ReaderImpl::Binary(array) => Ok(Self::PostgresJsonbVarlena(array)),
                ReaderImpl::LargeBinary(array) => {
                    Ok(Self::PostgresLargeJsonbVarlena(array))
                }
                _ => Err(ArrowConversionError::InvariantViolated(
                    "the PostgreSQL JSONB varlena codec requires its physical rule",
                )),
            };
        }

        Err(ArrowConversionError::InvariantViolated(
            "unknown provider datum codec",
        ))
    }

    fn from_standard(reader: ReaderImpl, target: ColumnDatumCodec) -> Self {
        match reader {
            ReaderImpl::Bool(array) => Self::Bool(array, target),
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
                Self::Decimal128 { arr, codec, target }
            }
            ReaderImpl::List {
                arr,
                element,
                element_codec,
            } => Self::List {
                arr,
                element,
                element_codec,
            },
        }
    }

    /// Read one datum after the enclosing [`BoundBatch`] established the row
    /// index against every array in the batch.
    ///
    /// # Safety
    ///
    /// `row_idx` must be within the bound RecordBatch. PostgreSQL must be
    /// active with the destination memory context selected.
    #[inline]
    pub(super) unsafe fn read_datum_unchecked(
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
            Self::Bool(array, target) => standard!(
                array,
                *target,
                Cell::Bool(unsafe { array.value_unchecked(row_idx) })
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
            Self::Decimal128 { arr, codec, target } => standard!(
                arr,
                *target,
                Cell::Numeric(codec.decode(unsafe { arr.value_unchecked(row_idx) })?)
            ),
            Self::List {
                arr,
                element,
                element_codec,
            } => {
                if unsafe { is_null_unchecked(arr, row_idx) } {
                    return Ok(None);
                }
                unsafe {
                    list::array_datum_at(arr, row_idx, *element, *element_codec)
                }
                .map(Some)
            }
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
            Self::PostgresJsonbVarlena(array) => {
                if unsafe { is_null_unchecked(array, row_idx) } {
                    return Ok(None);
                }
                let bytes: &[u8] = unsafe { array.value_unchecked(row_idx) };
                unsafe { DatumCodec::copy_postgres_jsonb_varlena(bytes) }.map(Some)
            }
            Self::PostgresLargeJsonbVarlena(array) => {
                if unsafe { is_null_unchecked(array, row_idx) } {
                    return Ok(None);
                }
                let bytes: &[u8] = unsafe { array.value_unchecked(row_idx) };
                unsafe { DatumCodec::copy_postgres_jsonb_varlena(bytes) }.map(Some)
            }
        }
    }
}
