//! Binary-family conversion (`bytea` / `jsonb` / `uuid` / fixed-width `bytea`):
//! the per-value length guard for `FixedSizeBinary(len)` plus the read
//! (`Arrow → Cell`) and write (datum / `Cell` → Arrow builder) paths.

use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, FixedSizeBinaryBuilder, LargeBinaryBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, FixedSizeBinaryArray};
use arrow_schema::DataType;
use pg_lakebase_core::tuple::{ByteaView, Cell, PgDatumRef};
use pgrx::datum::Uuid;
use pgrx::{FromDatum, pg_sys};

use super::{
    ColumnAppend, cell_type_mismatch, detoasted_payload, downcast, read_oid,
};
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
    bytes: usize,
}

impl BinaryEncoder {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: LargeBinaryBuilder::with_capacity(capacity, 1024),
            bytes: 0,
        }
    }
}

impl ColumnAppend for BinaryEncoder {
    unsafe fn append_datum(&mut self, datum: PgDatumRef<'_>) -> ConvResult<()> {
        let oid = datum.type_oid();
        if oid == pg_sys::BYTEAOID {
            // `bytea` stores the header-stripped payload: the read side rebuilds
            // a fresh varlena from the raw bytes (`&[u8]` -> datum).
            let guard = unsafe { detoasted_payload(datum.datum()) };
            let bytes = guard.bytes();
            self.builder.append_value(bytes);
            self.bytes += bytes.len();
        } else if oid == pg_sys::JSONBOID {
            // `jsonb` stores PostgreSQL's internal varlena verbatim, header
            // included: the read side copies the bytes straight back into a
            // datum and treats them as a complete varlena, so the header that
            // `bytes()` strips off must be kept here.
            let guard = unsafe { detoasted_payload(datum.datum()) };
            let bytes = guard.full_varlena_bytes();
            self.builder.append_value(bytes);
            self.bytes += bytes.len();
        } else {
            return Err(ConvError::InvariantViolated(
                "Binary encoder: datum source type is not bytea/jsonb",
            ));
        }
        Ok(())
    }

    fn append_cell(&mut self, cell: &Cell) -> ConvResult<()> {
        match cell {
            Cell::Bytea(b) => {
                self.builder.append_value(b);
                self.bytes += b.len();
            }
            // SAFETY: a `ByteaView` cell borrows live bytes; copied synchronously.
            Cell::ByteaView(b) => {
                let bytes = unsafe { b.as_slice() };
                self.builder.append_value(bytes);
                self.bytes += bytes.len();
            }
            Cell::Json(b) => {
                self.builder.append_value(b);
                self.bytes += b.len();
            }
            _ => return Err(cell_type_mismatch("bytea")),
        }
        Ok(())
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> ConvResult<ArrayRef> {
        self.bytes = 0;
        Ok(Arc::new(self.builder.finish()))
    }

    fn len(&self) -> usize {
        self.builder.len()
    }

    fn estimated_size(&self) -> usize {
        self.bytes
    }
}

// ---------------------------------------------------------------------------
// Fixed-width binary write encoder
// ---------------------------------------------------------------------------

pub(crate) struct FixedBinaryEncoder {
    builder: FixedSizeBinaryBuilder,
    codec: FixedCodec,
    bytes: usize,
}

impl FixedBinaryEncoder {
    pub(crate) fn with_capacity(capacity: usize, len: usize) -> Self {
        let width = len as i32;
        Self {
            builder: FixedSizeBinaryBuilder::with_capacity(capacity, width),
            codec: FixedCodec::new(len),
            bytes: 0,
        }
    }
}

impl ColumnAppend for FixedBinaryEncoder {
    unsafe fn append_datum(&mut self, datum: PgDatumRef<'_>) -> ConvResult<()> {
        if datum.type_oid() != pg_sys::BYTEAOID {
            return Err(ConvError::InvariantViolated(
                "FixedBinary encoder: datum source type is not bytea",
            ));
        }
        let guard = unsafe { detoasted_payload(datum.datum()) };
        let bytes = guard.bytes();
        self.codec.validate(bytes.len())?;
        self.builder.append_value(bytes)?;
        self.bytes += bytes.len();
        Ok(())
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
        self.bytes += bytes.len();
        Ok(())
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> ConvResult<ArrayRef> {
        self.bytes = 0;
        Ok(Arc::new(self.builder.finish()))
    }

    fn len(&self) -> usize {
        self.builder.len()
    }

    fn estimated_size(&self) -> usize {
        self.bytes
    }
}

// ---------------------------------------------------------------------------
// UUID write encoder
// ---------------------------------------------------------------------------

pub(crate) struct UuidEncoder {
    builder: FixedSizeBinaryBuilder,
    bytes: usize,
}

impl UuidEncoder {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: FixedSizeBinaryBuilder::with_capacity(capacity, 16),
            bytes: 0,
        }
    }
}

impl ColumnAppend for UuidEncoder {
    unsafe fn append_datum(&mut self, datum: PgDatumRef<'_>) -> ConvResult<()> {
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
        self.bytes += 16;
        Ok(())
    }

    fn append_cell(&mut self, cell: &Cell) -> ConvResult<()> {
        let Cell::Uuid(u) = cell else {
            return Err(cell_type_mismatch("uuid"));
        };
        self.builder.append_value(u.as_bytes())?;
        self.bytes += 16;
        Ok(())
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> ConvResult<ArrayRef> {
        self.bytes = 0;
        Ok(Arc::new(self.builder.finish()))
    }

    fn len(&self) -> usize {
        self.builder.len()
    }

    fn estimated_size(&self) -> usize {
        self.bytes
    }
}

// ---------------------------------------------------------------------------
// Read (Arrow → Cell)
// ---------------------------------------------------------------------------

pub(crate) fn extract_binary(column: &dyn Array, row_idx: usize) -> ConvResult<Cell> {
    let bytes = match column.data_type() {
        DataType::Binary => column.as_binary::<i32>().value(row_idx),
        DataType::LargeBinary => column.as_binary::<i64>().value(row_idx),
        other => {
            return Err(ConvError::ArrowTypeMismatch(
                format!("Binary or LargeBinary (actual: {other:?})").into(),
            ));
        }
    };
    Ok(Cell::ByteaView(ByteaView {
        ptr: bytes.as_ptr(),
        len: bytes.len(),
    }))
}

pub(crate) fn extract_fixed_binary(
    column: &dyn Array,
    row_idx: usize,
) -> ConvResult<Cell> {
    let bytes =
        downcast::<FixedSizeBinaryArray>(column, "FixedSizeBinary")?.value(row_idx);
    Ok(Cell::ByteaView(ByteaView {
        ptr: bytes.as_ptr(),
        len: bytes.len(),
    }))
}

pub(crate) fn extract_uuid(column: &dyn Array, row_idx: usize) -> ConvResult<Cell> {
    let bytes = downcast::<FixedSizeBinaryArray>(column, "FixedSizeBinary (UUID)")?
        .value(row_idx);
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| {
        ConvError::ArrowTypeMismatch(std::borrow::Cow::Borrowed(
            "UUID must be 16 bytes",
        ))
    })?;
    // Arrow UUID bytes are RFC 4122 network order, which pgrx::Uuid expects.
    Ok(Cell::Uuid(Uuid::from_bytes(bytes)))
}
