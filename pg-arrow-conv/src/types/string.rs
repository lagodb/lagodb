//! UTF-8 text conversion (`text` / `varchar` / `bpchar` / `json` / `name`):
//! read (`Arrow → Cell`) and write (datum / `Cell` → Arrow builder).

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{ArrayBuilder, StringBuilder};
use pg_lakebase_core::tuple::{Cell, PgDatumRef};
use pgrx::pg_sys;

use super::{ColumnAppend, cell_type_mismatch, detoasted_payload};
use crate::error::{ConvError, ConvResult};

pub(crate) struct Utf8Encoder {
    builder: StringBuilder,
    bytes: usize,
}

impl Utf8Encoder {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: StringBuilder::with_capacity(capacity, 1024),
            bytes: 0,
        }
    }
}

impl ColumnAppend for Utf8Encoder {
    unsafe fn append_datum(&mut self, datum: PgDatumRef<'_>) -> ConvResult<()> {
        let oid = datum.type_oid();
        if oid == pg_sys::TEXTOID
            || oid == pg_sys::VARCHAROID
            || oid == pg_sys::BPCHAROID
            || oid == pg_sys::JSONOID
        {
            // Detoast before reading: an in-line text datum may be compressed or
            // stored out-of-line, so its raw bytes are a toast pointer, not chars.
            let guard = unsafe { detoasted_payload(datum.datum()) };
            let s = std::str::from_utf8(guard.bytes())?;
            self.builder.append_value(s);
            self.bytes += s.len();
        } else if oid == pg_sys::NAMEOID {
            // `name` is not a varlena: the datum points at a fixed `NameData`
            // (a NUL-terminated C string), so it must be read as a cstring
            // rather than detoasted like the text family.
            let s = unsafe {
                let name_ptr = datum.datum().cast_mut_ptr::<pg_sys::NameData>();
                std::ffi::CStr::from_ptr((*name_ptr).data.as_ptr())
            }
            .to_str()?;
            self.builder.append_value(s);
            self.bytes += s.len();
        } else {
            return Err(ConvError::InvariantViolated(
                "Utf8 encoder: datum source type is not text/varchar/bpchar/json/name",
            ));
        }
        Ok(())
    }

    fn append_cell(&mut self, cell: &Cell) -> ConvResult<()> {
        let s = match cell {
            Cell::String(v) => v.as_str(),
            // SAFETY: a `StringView` cell borrows live Arrow/slot bytes; the
            // row-world build copies them into the builder synchronously here.
            Cell::StringView(v) => unsafe { v.as_str() },
            _ => return Err(cell_type_mismatch("text")),
        };
        self.builder.append_value(s);
        self.bytes += s.len();
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
// Read (Arrow → Cell): handled by the bound `ColumnReader` in `crate::read`.
// ---------------------------------------------------------------------------
