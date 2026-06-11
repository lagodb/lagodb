//! Binary-family conversion (`bytea` / `jsonb` / `uuid` / fixed-width `bytea`):
//! the per-value length guard for `FixedSizeBinary(len)` plus the read
//! (`Arrow → Cell`) and write (datum / `Cell` → Arrow builder) paths.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{
    ArrayBuilder, FixedSizeBinaryBuilder, LargeBinaryBuilder,
};
use pg_lakebase_core::tuple::{Cell, PgDatumRef};
use pgrx::datum::Uuid;
use pgrx::{FromDatum, pg_sys};

use super::{ColumnAppend, cell_type_mismatch, detoasted_payload, read_oid};
use crate::error::{ConvError, ConvResult};

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

    pub(crate) fn validate(&self, actual_len: usize) -> ConvResult<()> {
        if actual_len == self.len {
            Ok(())
        } else {
            Err(ConvError::IncompatibleColumnType(
                format!("fixed[{}]", self.len),
                format!("BYTEA length {actual_len}"),
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Binary write encoder (bytea / jsonb)
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
}

impl ColumnAppend for BinaryEncoder {
    unsafe fn append_datum(&mut self, datum: PgDatumRef<'_>) -> ConvResult<usize> {
        let oid = datum.type_oid();
        if oid == pg_sys::BYTEAOID {
            // `bytea` stores the header-stripped payload: the read side rebuilds
            // a fresh varlena from the raw bytes (`&[u8]` -> datum).
            let guard = unsafe { detoasted_payload(datum.datum()) };
            let bytes = guard.bytes();
            self.builder.append_value(bytes);
            Ok(bytes.len())
        } else if oid == pg_sys::JSONBOID {
            // `jsonb` stores PostgreSQL's internal varlena verbatim, header
            // included: the read side copies the bytes straight back into a
            // datum and treats them as a complete varlena, so the header that
            // `bytes()` strips off must be kept here.
            let guard = unsafe { detoasted_payload(datum.datum()) };
            let bytes = guard.full_varlena_bytes();
            self.builder.append_value(bytes);
            Ok(bytes.len())
        } else {
            Err(ConvError::InvariantViolated(
                "Binary encoder: datum source type is not bytea/jsonb",
            ))
        }
    }

    fn append_cell(&mut self, cell: &Cell) -> ConvResult<()> {
        match cell {
            Cell::Bytea(b) => {
                self.builder.append_value(b);
            }
            // SAFETY: a `ByteaView` cell borrows live bytes; copied synchronously.
            Cell::ByteaView(b) => {
                let bytes = unsafe { b.as_slice() };
                self.builder.append_value(bytes);
            }
            Cell::Json(b) => {
                self.builder.append_value(b);
            }
            _ => return Err(cell_type_mismatch("bytea")),
        }
        Ok(())
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> ConvResult<ArrayRef> {
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
}

impl ColumnAppend for FixedBinaryEncoder {
    unsafe fn append_datum(&mut self, datum: PgDatumRef<'_>) -> ConvResult<usize> {
        if datum.type_oid() != pg_sys::BYTEAOID {
            return Err(ConvError::InvariantViolated(
                "FixedBinary encoder: datum source type is not bytea",
            ));
        }
        let guard = unsafe { detoasted_payload(datum.datum()) };
        let bytes = guard.bytes();
        self.codec.validate(bytes.len())?;
        self.builder.append_value(bytes)?;
        Ok(bytes.len())
    }

    fn append_cell(&mut self, cell: &Cell) -> ConvResult<()> {
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

    fn finish(&mut self) -> ConvResult<ArrayRef> {
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
}

impl ColumnAppend for UuidEncoder {
    unsafe fn append_datum(&mut self, datum: PgDatumRef<'_>) -> ConvResult<usize> {
        // `uuid` is a fixed 16-byte by-reference type, not a varlena, so it is
        // read directly; its bytes are already RFC 4122 network order.
        let u = unsafe {
            read_oid(
                datum,
                pg_sys::UUIDOID,
                Uuid::from_datum,
                "Uuid encoder: datum source type is not uuid",
            )
        }?;
        self.builder.append_value(u.as_bytes())?;
        Ok(16)
    }

    fn append_cell(&mut self, cell: &Cell) -> ConvResult<()> {
        let Cell::Uuid(u) = cell else {
            return Err(cell_type_mismatch("uuid"));
        };
        self.builder.append_value(u.as_bytes())?;
        Ok(())
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> ConvResult<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }

    fn len(&self) -> usize {
        self.builder.len()
    }
}

// ---------------------------------------------------------------------------
// Read (Arrow → Cell): handled by the bound `ColumnReader` in `crate::read`.
// ---------------------------------------------------------------------------
