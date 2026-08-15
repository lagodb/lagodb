//! Binary-family conversion (`bytea` / `jsonb` / `uuid` / fixed-width `bytea`):
//! the per-value length guard for `FixedSizeBinary(len)` plus the read
//! (`Arrow → Cell`) and write (bound datum / `Cell` → Arrow builder) paths.

use std::ffi::{CString, c_void};
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{
    ArrayBuilder, FixedSizeBinaryBuilder, LargeBinaryBuilder,
};
use pg_lakebase_core::tuple::{Cell, DetoastedVarlena};
use pgrx::datum::Uuid;
use pgrx::{FromDatum, PgTryBuilder, fcinfo, pg_sys};

use super::{ColumnAppend, cell_type_mismatch, read_bound};
use crate::error::{ArrowConversionError, ArrowConversionResult};
use pg_lakebase_core::diag::PgError;

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
// Binary write encoder (bytea / explicit JSONB internal-varlena)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub(crate) enum BinaryKind {
    Bytea,
    PostgresJsonbVarlena,
}

pub(crate) struct BinaryEncoder {
    builder: LargeBinaryBuilder,
    kind: BinaryKind,
}

impl BinaryEncoder {
    pub(crate) fn with_capacity(capacity: usize, kind: BinaryKind) -> Self {
        Self {
            builder: LargeBinaryBuilder::with_capacity(capacity, 1024),
            kind,
        }
    }

    /// Append a non-NULL PostgreSQL `bytea` datum after detoasting it.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `bytea` varlena Datum.
    pub(super) unsafe fn append_bytea(
        &mut self,
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<usize> {
        let guard = unsafe { DetoastedVarlena::from_datum(datum) };
        let bytes = guard.bytes();
        self.builder.append_value(bytes);
        Ok(bytes.len())
    }

    /// Append a non-NULL PostgreSQL internal `jsonb` varlena datum.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `jsonb` varlena Datum.
    pub(super) unsafe fn append_jsonb(
        &mut self,
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<usize> {
        let guard = unsafe { DetoastedVarlena::from_datum(datum) };
        let bytes = guard.full_varlena_bytes();
        self.builder.append_value(bytes);
        Ok(bytes.len())
    }
}

impl ColumnAppend for BinaryEncoder {
    fn append_cell(&mut self, cell: &Cell) -> ArrowConversionResult<()> {
        match self.kind {
            BinaryKind::Bytea => match cell {
                Cell::Bytea(b) => self.builder.append_value(b),
                // SAFETY: a `ByteaView` cell borrows live bytes; copied synchronously.
                Cell::ByteaView(b) => {
                    self.builder.append_value(unsafe { b.as_slice() })
                }
                _ => return Err(cell_type_mismatch("bytea")),
            },
            BinaryKind::PostgresJsonbVarlena => match cell {
                Cell::Jsonb(value) => {
                    // Row-world input is already a semantic JSONB value. Borrow
                    // its PostgreSQL output text while jsonb_in constructs the
                    // provider's physical representation; do not deep-clone a
                    // generic JSON tree or serialize it through serde_json.
                    let datum =
                        unsafe { JsonbInputDatum::from_text(value.as_str())? };
                    let guard = unsafe { DetoastedVarlena::from_datum(datum.datum) };
                    self.builder.append_value(guard.full_varlena_bytes());
                }
                _ => return Err(cell_type_mismatch("jsonb")),
            },
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

/// Owns the temporary Datum created by the row-world JSONB input path. The
/// Arrow builder copies the complete varlena before this guard is dropped, so
/// the PostgreSQL allocation does not remain in the batch memory context.
struct JsonbInputDatum {
    datum: pg_sys::Datum,
}

impl JsonbInputDatum {
    unsafe fn from_text(text: &str) -> ArrowConversionResult<Self> {
        let input = CString::new(text).map_err(|_| {
            ArrowConversionError::InvalidInput(
                "semantic JSONB text contains an interior NUL".to_string(),
            )
        })?;
        let datum = unsafe {
            PgTryBuilder::new(|| {
                Ok(fcinfo::direct_function_call_as_datum(
                    pg_sys::jsonb_in,
                    &[Some(pg_sys::Datum::from(input.as_ptr()))],
                ))
            })
            .catch_others(|error| Err(PgError::from(error)))
            .execute()
        }
        .map_err(ArrowConversionError::Postgres)?
        .ok_or(ArrowConversionError::InvariantViolated(
            "jsonb_in returned NULL for a non-null semantic value",
        ))?;
        Ok(Self { datum })
    }
}

impl Drop for JsonbInputDatum {
    fn drop(&mut self) {
        unsafe { pg_sys::pfree(self.datum.cast_mut_ptr::<c_void>()) };
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
    pub(super) unsafe fn append_bound(
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
    pub(super) unsafe fn append_bound(
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
