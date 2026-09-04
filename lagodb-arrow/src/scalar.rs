//! Batch-bound scalar conversion for vectorized executors that invoke a
//! PostgreSQL expression once per selected row.
//!
//! Both directions bind the complete PostgreSQL/Arrow pair once. The input
//! row path contains one concrete reader dispatch, one NULL branch, and the
//! selected conversion; it never re-enters the general `Cell`/OID dispatcher.

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array,
    Int64Array, LargeStringArray, StringArray,
};
use arrow_schema::DataType;
use lagodb_core::tuple::{ColumnDatumCodec, ColumnDatumTarget};
use pgrx::{IntoDatum, pg_sys};

use crate::error::{ArrowConversionError, ArrowConversionResult};
use crate::read::{ColumnReader, ReaderImpl};
use crate::rule::{PgColumnType, resolve_column_rule};
use crate::types::{BoundColumnEncoder, BoundEncoderPlan};

enum PgDatumReader {
    Bool(BooleanArray),
    Int2(Int32Array, ColumnDatumCodec),
    Int4(Int32Array),
    Int8(Int64Array),
    Float4(Float32Array),
    Float8(Float64Array),
    Text(StringArray),
    LargeText(LargeStringArray),
    Name(StringArray, ColumnDatumCodec),
    LargeName(LargeStringArray, ColumnDatumCodec),
}

pub struct PgDatumArrayReader {
    reader: PgDatumReader,
}

impl PgDatumArrayReader {
    pub fn bind(
        array: &dyn Array,
        type_oid: pg_sys::Oid,
    ) -> ArrowConversionResult<Self> {
        let pg_type = PgColumnType::from_pg_type(type_oid).ok_or_else(|| {
            ArrowConversionError::UnsupportedColumnType(format!(
                "PostgreSQL type OID {type_oid:?}"
            ))
        })?;
        let rule = resolve_column_rule(array.data_type(), pg_type)?;
        let target = ColumnDatumCodec::bind(ColumnDatumTarget::from_oid(type_oid))?;
        let reader = ColumnReader::bind_reader(&rule, array)?;
        let reader = match (reader, type_oid) {
            (ReaderImpl::Bool(array), pg_sys::BOOLOID) => PgDatumReader::Bool(array),
            (ReaderImpl::I32(array), pg_sys::INT2OID) => {
                PgDatumReader::Int2(array, target)
            }
            (ReaderImpl::I32(array), pg_sys::INT4OID) => PgDatumReader::Int4(array),
            (ReaderImpl::I64(array), pg_sys::INT8OID) => PgDatumReader::Int8(array),
            (ReaderImpl::F32(array), pg_sys::FLOAT4OID) => {
                PgDatumReader::Float4(array)
            }
            (ReaderImpl::F64(array), pg_sys::FLOAT8OID) => {
                PgDatumReader::Float8(array)
            }
            (ReaderImpl::Utf8(array), pg_sys::TEXTOID | pg_sys::VARCHAROID) => {
                PgDatumReader::Text(array)
            }
            (ReaderImpl::LargeUtf8(array), pg_sys::TEXTOID | pg_sys::VARCHAROID) => {
                PgDatumReader::LargeText(array)
            }
            (ReaderImpl::Utf8(array), pg_sys::NAMEOID) => {
                PgDatumReader::Name(array, target)
            }
            (ReaderImpl::LargeUtf8(array), pg_sys::NAMEOID) => {
                PgDatumReader::LargeName(array, target)
            }
            _ => {
                return Err(ArrowConversionError::InvariantViolated(
                    "PostgreSQL expression input has no bound scalar reader",
                ));
            }
        };
        Ok(Self { reader })
    }

    /// # Safety
    ///
    /// `row` must be within the array supplied to [`Self::bind`]. PostgreSQL
    /// must be active on the current backend thread and own the current memory
    /// context used for any by-reference result.
    #[inline]
    pub unsafe fn datum_unchecked(
        &self,
        row: usize,
    ) -> ArrowConversionResult<Option<pg_sys::Datum>> {
        macro_rules! value {
            ($array:expr) => {{
                if $array.nulls().is_some_and(|nulls| {
                    // SAFETY: this method's row contract establishes the bound.
                    !unsafe { nulls.inner().value_unchecked(row) }
                }) {
                    return Ok(None);
                }
                // SAFETY: this method's row contract establishes the bound.
                unsafe { $array.value_unchecked(row) }
            }};
        }

        let datum = match &self.reader {
            PgDatumReader::Bool(array) => pg_sys::Datum::from(value!(array)),
            PgDatumReader::Int2(array, target) => {
                let value = value!(array);
                unsafe { target.int2_datum_from_i32_unchecked(value) }
                    .map_err(ArrowConversionError::from)?
            }
            PgDatumReader::Int4(array) => pg_sys::Datum::from(value!(array)),
            PgDatumReader::Int8(array) => pg_sys::Datum::from(value!(array)),
            PgDatumReader::Float4(array) => {
                pg_sys::Datum::from(value!(array).to_bits())
            }
            PgDatumReader::Float8(array) => {
                pg_sys::Datum::from(value!(array).to_bits())
            }
            PgDatumReader::Text(array) => value!(array)
                .into_datum()
                .expect("&str always materializes a PostgreSQL text datum"),
            PgDatumReader::LargeText(array) => value!(array)
                .into_datum()
                .expect("&str always materializes a PostgreSQL text datum"),
            PgDatumReader::Name(array, target) => {
                let value = value!(array);
                unsafe { target.name_datum_from_str_unchecked(value) }
                    .map_err(ArrowConversionError::from)?
            }
            PgDatumReader::LargeName(array, target) => {
                let value = value!(array);
                unsafe { target.name_datum_from_str_unchecked(value) }
                    .map_err(ArrowConversionError::from)?
            }
        };
        Ok(Some(datum))
    }
}

pub struct PgDatumArrayBuilder {
    encoder: BoundColumnEncoder,
}

impl PgDatumArrayBuilder {
    pub fn bind(
        data_type: &DataType,
        type_oid: pg_sys::Oid,
        capacity: usize,
    ) -> ArrowConversionResult<Self> {
        let pg_type = PgColumnType::from_pg_type(type_oid).ok_or_else(|| {
            ArrowConversionError::UnsupportedColumnType(format!(
                "PostgreSQL type OID {type_oid:?}"
            ))
        })?;
        let rule = resolve_column_rule(data_type, pg_type)?;
        // Utf8Encoder uses from_utf8_unchecked for PostgreSQL text-family and
        // NameData bytes. Match every other bound writer by establishing the
        // PG_UTF8 capability once before materializing the per-batch builder.
        ColumnDatumCodec::bind(ColumnDatumTarget::from_oid(type_oid))?;
        Ok(Self {
            encoder: BoundEncoderPlan::bind(rule, type_oid)?.materialize(capacity)?,
        })
    }

    /// # Safety
    ///
    /// A present datum must be valid for the PostgreSQL OID supplied to
    /// [`Self::bind`]. Any referenced storage must remain valid until this call
    /// returns.
    #[inline]
    pub unsafe fn append(
        &mut self,
        value: Option<pg_sys::Datum>,
    ) -> ArrowConversionResult<()> {
        match value {
            Some(datum) => {
                let _ = unsafe { self.encoder.append(datum) }?;
            }
            None => self.encoder.append_null(),
        }
        Ok(())
    }

    pub fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
        self.encoder.finish()
    }
}
