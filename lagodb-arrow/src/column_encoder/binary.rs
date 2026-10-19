//! Binary-family conversion (`bytea` / `uuid` / fixed-width `bytea`):
//! the per-value length guard for `FixedSizeBinary(len)` plus the read
//! (`Arrow → Cell`) and write (bound datum / `Cell` → Arrow builder) paths.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{
    ArrayBuilder, FixedSizeBinaryBuilder, LargeBinaryBuilder,
};
use lagodb_core::datum::{Cell, DetoastedVarlena};
use pgrx::datum::Uuid;
use pgrx::{FromDatum, pg_sys};

use super::{ColumnAppend, cell_type_mismatch, read_bound};
use crate::error::{ArrowConversionError, ArrowConversionResult};

// ---------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------

pub(crate) struct FixedCodec {
    len: usize,
}

impl FixedCodec {
    pub(crate) fn new(len: usize) -> Self {
        Self { len }
    }

    pub(crate) fn validate(&self, actual_len: usize) -> ArrowConversionResult<()> {
        if actual_len == self.len {
            Ok(())
        } else {
            Err(ArrowConversionError::IncompatibleColumnType(
                format!("fixed[{}]", self.len),
                format!("BYTEA length {actual_len}"),
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Binary write encoder (bytea)
// ---------------------------------------------------------------------------

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

impl ColumnAppend for BinaryEncoder {
    fn append_cell(&mut self, cell: &Cell) -> ArrowConversionResult<()> {
        match cell {
            Cell::Bytea(bytes) => self.builder.append_value(bytes),
            // SAFETY: a ByteaView borrows live bytes, copied synchronously.
            Cell::ByteaView(bytes) => {
                self.builder.append_value(unsafe { bytes.as_slice() })
            }
            _ => return Err(cell_type_mismatch("bytea")),
        }
        Ok(())
    }

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

// ---------------------------------------------------------------------------
// Fixed-width binary write encoder
// ---------------------------------------------------------------------------

pub(crate) struct FixedBinaryEncoder {
    builder: FixedSizeBinaryBuilder,
    codec: FixedCodec,
}

impl FixedBinaryEncoder {
    pub(crate) fn with_capacity(capacity: usize, len: usize) -> Self {
        let width = len as i32;
        Self {
            builder: FixedSizeBinaryBuilder::with_capacity(capacity, width),
            codec: FixedCodec::new(len),
        }
    }

    /// Append a non-NULL PostgreSQL `bytea` datum after validating its width.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `bytea` varlena Datum.
    pub(crate) unsafe fn append_bound(
        &mut self,
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<usize> {
        let guard = unsafe { DetoastedVarlena::from_datum(datum) };
        let bytes = guard.bytes();
        self.codec.validate(bytes.len())?;
        self.builder.append_value(bytes)?;
        Ok(bytes.len())
    }
}

impl ColumnAppend for FixedBinaryEncoder {
    fn append_cell(&mut self, cell: &Cell) -> ArrowConversionResult<()> {
        let bytes: &[u8] = match cell {
            Cell::Bytea(b) => b.as_ref(),
            // SAFETY: a `ByteaView` cell borrows live bytes; copied synchronously.
            Cell::ByteaView(b) => unsafe { b.as_slice() },
            _ => return Err(cell_type_mismatch("bytea")),
        };
        self.codec.validate(bytes.len())?;
        self.builder.append_value(bytes)?;
        Ok(())
    }

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

// ---------------------------------------------------------------------------
// UUID write encoder
// ---------------------------------------------------------------------------

pub(crate) struct UuidEncoder {
    builder: FixedSizeBinaryBuilder,
}

impl UuidEncoder {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: FixedSizeBinaryBuilder::with_capacity(capacity, 16),
        }
    }

    /// Append a non-NULL PostgreSQL `uuid` datum.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `uuid` Datum.
    pub(crate) unsafe fn append_bound(
        &mut self,
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<usize> {
        let value = unsafe {
            read_bound(
                datum,
                Uuid::from_datum,
                "Uuid encoder: present uuid datum read as null",
            )
        }?;
        self.builder.append_value(value.as_bytes())?;
        Ok(16)
    }
}

impl ColumnAppend for UuidEncoder {
    fn append_cell(&mut self, cell: &Cell) -> ArrowConversionResult<()> {
        let Cell::Uuid(u) = cell else {
            return Err(cell_type_mismatch("uuid"));
        };
        self.builder.append_value(u.as_bytes())?;
        Ok(())
    }

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

// ---------------------------------------------------------------------------
// Read (Arrow → Cell): handled by the bound `ColumnReader` in `crate::read`.
// ---------------------------------------------------------------------------
