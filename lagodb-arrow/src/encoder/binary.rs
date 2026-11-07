//! Binary-family Arrow builders: `bytea`, fixed-width `bytea` and `uuid`.

use std::slice;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{
    ArrayBuilder, FixedSizeBinaryBuilder, LargeBinaryBuilder,
};
use lagodb_core::datum::DetoastedVarlena;
use pgrx::pg_sys;

use super::ColumnBuilder;
use crate::error::{ArrowConversionError, ArrowConversionResult};

const UUID_LEN: usize = 16;

pub(crate) struct BinaryEncoder {
    builder: LargeBinaryBuilder,
}

impl BinaryEncoder {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: LargeBinaryBuilder::with_capacity(capacity, 1024),
        }
    }

    /// Append a non-NULL PostgreSQL `bytea` datum after detoasting it.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `bytea` varlena Datum.
    pub(crate) unsafe fn append_bytea(
        &mut self,
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<usize> {
        let guard = unsafe { DetoastedVarlena::from_datum(datum) };
        let bytes = guard.bytes();
        self.builder.append_value(bytes);
        Ok(bytes.len())
    }
}

impl ColumnBuilder for BinaryEncoder {
    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }

    fn len(&self) -> usize {
        self.builder.len()
    }
}

pub(crate) struct FixedBinaryEncoder {
    builder: FixedSizeBinaryBuilder,
    len: usize,
}

impl FixedBinaryEncoder {
    pub(crate) fn with_capacity(capacity: usize, len: usize) -> Self {
        Self {
            builder: FixedSizeBinaryBuilder::with_capacity(capacity, len as i32),
            len,
        }
    }

    /// Append a non-NULL PostgreSQL `bytea` datum after validating its width.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `bytea` varlena Datum.
    pub(crate) unsafe fn append_bytea(
        &mut self,
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<usize> {
        let guard = unsafe { DetoastedVarlena::from_datum(datum) };
        let bytes = guard.bytes();
        if bytes.len() != self.len {
            return Err(ArrowConversionError::IncompatibleColumnType(
                format!("fixed[{}]", self.len),
                format!("BYTEA length {}", bytes.len()),
            ));
        }
        self.builder.append_value(bytes)?;
        Ok(bytes.len())
    }
}

impl ColumnBuilder for FixedBinaryEncoder {
    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }

    fn len(&self) -> usize {
        self.builder.len()
    }
}

pub(crate) struct UuidEncoder {
    builder: FixedSizeBinaryBuilder,
}

impl UuidEncoder {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: FixedSizeBinaryBuilder::with_capacity(capacity, UUID_LEN as i32),
        }
    }

    /// Append a non-NULL PostgreSQL `uuid` datum.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `uuid` Datum.
    pub(crate) unsafe fn append_uuid(
        &mut self,
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<usize> {
        // DatumGetUUIDP: a pointer to PostgreSQL's fixed 16-byte pg_uuid_t,
        // stored in RFC 4122 network order as Arrow expects.
        let bytes =
            unsafe { slice::from_raw_parts(datum.cast_mut_ptr::<u8>(), UUID_LEN) };
        self.builder.append_value(bytes)?;
        Ok(UUID_LEN)
    }
}

impl ColumnBuilder for UuidEncoder {
    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }

    fn len(&self) -> usize {
        self.builder.len()
    }
}
