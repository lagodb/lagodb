//! Batch-bound scalar conversion for vectorized executors that invoke a
//! PostgreSQL expression once per selected row.
//!
//! Both directions bind the complete PostgreSQL/Arrow pair once and reuse the
//! decoder's and writer's bound scalar codecs. The caller owns admission of
//! which PostgreSQL types it evaluates this way.

use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use lagodb_core::datum::ColumnDatumTarget;
use pgrx::pg_sys;

use super::DatumCodec;
use super::encoder::DatumEncoder;
use super::reader::{BoundDatumReader, ScalarRead};
use crate::error::{ArrowConversionError, ArrowConversionResult};
use crate::rule::{CellRule, PgColumnType};

fn resolve_scalar(
    data_type: &DataType,
    type_oid: pg_sys::Oid,
) -> ArrowConversionResult<CellRule> {
    let pg_type = PgColumnType::from_pg_type(type_oid).ok_or_else(|| {
        ArrowConversionError::UnsupportedColumnType(format!(
            "PostgreSQL type OID {type_oid:?}"
        ))
    })?;
    CellRule::resolve(data_type, pg_type)
}

/// One Arrow input array bound to a PostgreSQL expression argument type.
pub struct PgDatumArrayReader {
    reader: BoundDatumReader,
}

impl PgDatumArrayReader {
    pub fn bind(
        array: &dyn Array,
        type_oid: pg_sys::Oid,
    ) -> ArrowConversionResult<Self> {
        let rule = resolve_scalar(array.data_type(), type_oid)?;
        let read = ScalarRead::bind(&rule, DatumCodec::standard(type_oid)?, None)?;
        // SAFETY: the rule was resolved from this array's complete data type.
        let reader = unsafe { read.bind_array(array) };
        Ok(Self { reader })
    }

    /// # Safety
    ///
    /// `row` must be within the array supplied to [`Self::bind`]. PostgreSQL
    /// must be active on the current backend thread and own the current memory
    /// context used for any by-reference result.
    /// Int32 input bound to int2 must preserve the PG int2 value domain;
    /// expression producers, including arithmetic, own its SQL overflow semantics.
    #[inline]
    pub unsafe fn datum_unchecked(
        &self,
        row: usize,
    ) -> ArrowConversionResult<Option<pg_sys::Datum>> {
        unsafe { self.reader.read_datum_unchecked(row) }
    }
}

/// An Arrow output builder bound to a PostgreSQL expression result type.
pub struct PgDatumArrayBuilder {
    encoder: DatumEncoder,
}

impl PgDatumArrayBuilder {
    pub fn bind(
        data_type: &DataType,
        type_oid: pg_sys::Oid,
        capacity: usize,
    ) -> ArrowConversionResult<Self> {
        let rule = resolve_scalar(data_type, type_oid)?;
        // Utf8Encoder uses from_utf8_unchecked for PostgreSQL text-family and
        // NameData bytes. Match every other bound writer by establishing the
        // PG_UTF8 capability once before binding the builder.
        if rule.requires_utf8_server_encoding() {
            ColumnDatumTarget::validate_utf8_server_encoding()?;
        }
        Ok(Self {
            encoder: DatumEncoder::bind(&rule, type_oid, capacity)?,
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
                unsafe { self.encoder.append(datum) }?;
            }
            None => self.encoder.append_null(),
        }
        Ok(())
    }

    pub fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
        self.encoder.finish()
    }
}
