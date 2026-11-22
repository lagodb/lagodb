//! Cached PostgreSQL type input/output functions for text representations.

use std::cell::{RefCell, RefMut};
#[cfg(feature = "pg15")]
use std::convert::Infallible;
use std::ffi::c_char;
use std::marker::PhantomData;
use std::ptr;
use std::str;

#[cfg(not(feature = "pg15"))]
use std::ptr::NonNull;

use pgrx::pg_sys;

use super::output_cstring::PgOutputCString;
use super::pg_type::PgType;
use crate::backend::OwnedBackendContext;

/// A non-exclusive handle to a PostgreSQL `ErrorSaveContext` owned by the
/// caller's operation, such as COPY with `ON_ERROR IGNORE`. Copies share the
/// context; PostgreSQL writes it only during synchronous input calls, so input
/// errors saved here do not unwind.
#[cfg(not(feature = "pg15"))]
#[derive(Clone, Copy)]
pub struct SoftErrorContext<'a> {
    node: NonNull<pg_sys::Node>,
    _owner: PhantomData<&'a pg_sys::Node>,
}

/// PostgreSQL 15 has no soft-error input context. Keep the lifetime-bearing
/// handle in the common typed-input API without storing a node that this
/// version can never pass to an input function.
#[cfg(feature = "pg15")]
#[derive(Clone, Copy)]
pub struct SoftErrorContext<'a> {
    never: Infallible,
    _owner: PhantomData<&'a pg_sys::Node>,
}

impl SoftErrorContext<'_> {
    /// # Safety
    /// A non-null `node` is a live `ErrorSaveContext` for the returned lifetime.
    #[cfg(not(feature = "pg15"))]
    pub(crate) unsafe fn from_raw(node: *mut pg_sys::Node) -> Option<Self> {
        NonNull::new(node).map(|node| Self {
            node,
            _owner: PhantomData,
        })
    }

    /// PostgreSQL 15 callers always supply NULL because COPY has no
    /// `ErrorSaveContext`, so this version can never construct the handle.
    #[cfg(feature = "pg15")]
    pub(crate) unsafe fn from_raw(_node: *mut pg_sys::Node) -> Option<Self> {
        None
    }
}

/// Backend-local I/O state. Function caches live in this codec's context,
/// while returned datums belong to the caller's current row context.
pub struct PgTextIo {
    input: RefCell<pg_sys::FmgrInfo>,
    output: RefCell<pg_sys::FmgrInfo>,
    buffer: RefCell<Vec<u8>>,
    parameter: pg_sys::Oid,
    typmod: i32,
    _context: OwnedBackendContext,
}

impl PgTextIo {
    /// Cache the type's input and output functions. Which types a text
    /// representation admits is decided by the consumer, not this codec.
    pub fn bind(pg_type: PgType) -> Self {
        let context = OwnedBackendContext::new(c"lagodb storage type I/O");
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
        let mut buffer = self.terminated(text);
        unsafe { self.input_call(buffer.as_mut_ptr().cast()) }
    }

    /// Convert one field as PostgreSQL's COPY does. `None` text is SQL NULL
    /// and still reaches the input function, which fmgr skips only when it
    /// is strict, so a non-strict `domain_in` checks the domain's constraints.
    /// An input error that the type reports softly is saved in `errors` and
    /// yields `None`; without `errors`, every input error unwinds. A returned
    /// Datum is meaningful only for `Some` text: fmgr requires the result to
    /// be NULL exactly when the input is.
    ///
    /// # Safety
    /// The same contract as [`Self::input`].
    pub unsafe fn input_or_save(
        &self,
        text: Option<&str>,
        errors: Option<SoftErrorContext<'_>>,
    ) -> Option<pg_sys::Datum> {
        let mut buffer = text.map(|text| self.terminated(text));
        let text = buffer
            .as_mut()
            .map_or(ptr::null_mut(), |buffer| buffer.as_mut_ptr().cast());
        match errors {
            None => Some(unsafe { self.input_call(text) }),
            Some(errors) => unsafe { self.input_safe(text, errors) },
        }
    }

    unsafe fn input_call(&self, text: *mut c_char) -> pg_sys::Datum {
        unsafe {
            pg_sys::InputFunctionCall(
                &mut *self.input.borrow_mut(),
                text,
                self.parameter,
                self.typmod,
            )
        }
    }

    #[cfg(not(feature = "pg15"))]
    unsafe fn input_safe(
        &self,
        text: *mut c_char,
        errors: SoftErrorContext<'_>,
    ) -> Option<pg_sys::Datum> {
        let mut result = pg_sys::Datum::from(0);
        unsafe {
            pg_sys::InputFunctionCallSafe(
                &mut *self.input.borrow_mut(),
                text,
                self.parameter,
                self.typmod,
                errors.node.as_ptr(),
                &mut result,
            )
        }
        .then_some(result)
    }

    /// PostgreSQL 15 has neither `ErrorSaveContext` nor
    /// `InputFunctionCallSafe`, so its handle is uninhabited.
    #[cfg(feature = "pg15")]
    unsafe fn input_safe(
        &self,
        _text: *mut c_char,
        errors: SoftErrorContext<'_>,
    ) -> Option<pg_sys::Datum> {
        match errors.never {}
    }

    /// PostgreSQL input functions may write into their argument, as
    /// `array_in` does, so they receive this codec-owned mutable copy.
    fn terminated(&self, text: &str) -> RefMut<'_, Vec<u8>> {
        let mut buffer = self.buffer.borrow_mut();
        buffer.clear();
        buffer.extend_from_slice(text.as_bytes());
        buffer.push(0);
        buffer
    }
}
