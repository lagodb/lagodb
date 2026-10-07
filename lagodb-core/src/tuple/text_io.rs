//! Cached PostgreSQL type I/O for types stored in a portable text encoding.

use std::{cell::RefCell, str};

use pgrx::pg_sys;

use super::PgType;
use super::codec_context::CodecContext;
use crate::wrapper::PgOutputCString;

/// Backend-local I/O state. Function caches live in this codec's context,
/// while returned datums belong to the caller's current row context.
pub struct PgTextIo {
    input: RefCell<pg_sys::FmgrInfo>,
    output: RefCell<pg_sys::FmgrInfo>,
    buffer: RefCell<Vec<u8>>,
    parameter: pg_sys::Oid,
    typmod: i32,
    _context: CodecContext,
}

impl PgTextIo {
    /// Cache I/O functions for a provider-approved text representation.
    /// Storage admission and DDL restrictions belong to the provider.
    pub fn bind(pg_type: PgType) -> Self {
        let context = CodecContext::new(c"lagodb storage type I/O");
        let (mut input_oid, mut output_oid, mut parameter) =
            (pg_sys::InvalidOid, pg_sys::InvalidOid, pg_sys::InvalidOid);
        let mut varlena = false;
        let mut input = pg_sys::FmgrInfo::default();
        let mut output = pg_sys::FmgrInfo::default();
        unsafe {
            pg_sys::getTypeInputInfo(pg_type.oid, &mut input_oid, &mut parameter);
            pg_sys::getTypeOutputInfo(pg_type.oid, &mut output_oid, &mut varlena);
            pg_sys::fmgr_info_cxt(input_oid, &mut input, context.as_ptr());
            pg_sys::fmgr_info_cxt(output_oid, &mut output, context.as_ptr());
        }
        Self {
            input: RefCell::new(input),
            output: RefCell::new(output),
            buffer: RefCell::new(Vec::new()),
            parameter,
            typmod: pg_type.typmod,
            _context: context,
        }
    }

    /// # Safety
    /// The datum has the bound type; the database uses UTF-8. The callback
    /// cannot retain the borrowed output string.
    /// PG errors unwind to the enclosing provider/FFI boundary.
    pub unsafe fn with_output<R>(
        &self,
        datum: pg_sys::Datum,
        consume: impl FnOnce(&str) -> R,
    ) -> R {
        let output = unsafe {
            pg_sys::OutputFunctionCall(&mut *self.output.borrow_mut(), datum)
        };
        let output =
            unsafe { PgOutputCString::from_datum(pg_sys::Datum::from(output)) };
        let text = unsafe { str::from_utf8_unchecked(output.as_cstr().to_bytes()) };
        consume(text)
    }

    /// # Safety
    /// `text` contains no NUL; the current backend row context owns the result.
    pub unsafe fn input(&self, text: &str) -> pg_sys::Datum {
        let mut buffer = self.buffer.borrow_mut();
        buffer.clear();
        buffer.extend_from_slice(text.as_bytes());
        buffer.push(0);
        unsafe {
            pg_sys::InputFunctionCall(
                &mut *self.input.borrow_mut(),
                buffer.as_mut_ptr().cast(),
                self.parameter,
                self.typmod,
            )
        }
    }
}
