//! Stage-neutral error transport for exact-build runtime callbacks.

use std::mem::size_of;
use std::panic::AssertUnwindSafe;
use std::{ptr, slice, str};

use pgrx::prelude::PgSqlErrorCode;
use pgrx::{PgMemoryContexts, PgTryBuilder, pg_sys};

use crate::diag::PgReportError;

pub const FFI_OPERATION_OK: u32 = 0;
pub const FFI_OPERATION_FAILED: u32 = 1;

/// Marker returned after an operation error has been copied into an
/// [`FfiErrorRecord`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("FFI operation failed; see the error record for details")]
pub struct FfiCaptureError;

#[repr(C)]
#[derive(Clone, Copy)]
struct FfiErrorText {
    data: *const u8,
    len: usize,
}

impl Default for FfiErrorText {
    fn default() -> Self {
        Self {
            data: ptr::null(),
            len: 0,
        }
    }
}

impl FfiErrorText {
    unsafe fn copy_from(value: &str, memory_context: pg_sys::MemoryContext) -> Self {
        if value.is_empty() {
            return Self::default();
        }
        let mut context = PgMemoryContexts::For(memory_context);
        Self {
            // SAFETY: `value` is live for this call; PostgreSQL copies exactly
            // `len` bytes into the supplied live memory context.
            data: unsafe {
                context.copy_ptr_into(value.as_ptr().cast_mut(), value.len())
            },
            len: value.len(),
        }
    }

    fn is_valid(self) -> bool {
        self.len == 0 || !self.data.is_null()
    }

    unsafe fn to_owned(self) -> String {
        if self.len == 0 {
            return String::new();
        }
        // SAFETY: the exact-build callback copied bytes from a Rust `str` and
        // the caller guarantees that the allocation remains live.
        unsafe {
            str::from_utf8_unchecked(slice::from_raw_parts(self.data, self.len))
        }
        .to_owned()
    }
}

/// PostgreSQL-owned diagnostic payload shared by all runtime callback stages.
///
/// The callback allocates text in the active PostgreSQL memory context. The
/// runtime consumes it synchronously before that context can be reset, so no
/// Rust allocation or error object crosses the DSO boundary.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FfiErrorRecord {
    struct_size: u32,
    sql_error_code: i32,
    message: FfiErrorText,
    detail: FfiErrorText,
    hint: FfiErrorText,
}

impl Default for FfiErrorRecord {
    fn default() -> Self {
        Self {
            struct_size: size_of::<Self>() as u32,
            sql_error_code: 0,
            message: FfiErrorText::default(),
            detail: FfiErrorText::default(),
            hint: FfiErrorText::default(),
        }
    }
}

impl FfiErrorRecord {
    /// Run one callback without allowing a PostgreSQL error or Rust panic to
    /// cross the exact-build ABI.
    ///
    /// # Safety
    ///
    /// This must run on a PostgreSQL backend thread with a live current memory
    /// context. The runtime must consume the record synchronously.
    pub unsafe fn capture(
        &mut self,
        operation: impl FnOnce() -> Result<(), PgReportError>,
    ) -> u32 {
        // SAFETY: the caller upholds the backend-thread and memory-context
        // requirements documented by this method.
        match unsafe { self.capture_result(operation) } {
            Ok(()) => FFI_OPERATION_OK,
            Err(FfiCaptureError) => FFI_OPERATION_FAILED,
        }
    }

    /// Capture a callback that returns a value while preserving its structured
    /// error in this record.
    ///
    /// # Safety
    ///
    /// This has the same backend-thread and memory-context requirements as
    /// [`Self::capture`].
    pub unsafe fn capture_result<T>(
        &mut self,
        operation: impl FnOnce() -> Result<T, PgReportError>,
    ) -> Result<T, FfiCaptureError> {
        *self = Self::default();
        // Preserve the caller's context across a caught PostgreSQL ERROR;
        // error handling may temporarily switch CurrentMemoryContext.
        let memory_context = unsafe { pg_sys::CurrentMemoryContext };
        let result = PgTryBuilder::new(AssertUnwindSafe(operation))
            .catch_others(|error| Err(PgReportError::from_caught(error)))
            .execute();
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                // SAFETY: `memory_context` was captured while live immediately
                // before the protected operation and this record is writable.
                unsafe { self.write(error, memory_context) };
                Err(FfiCaptureError)
            }
        }
    }

    #[must_use]
    pub const fn is_set(&self) -> bool {
        self.sql_error_code != 0
    }

    /// Reconstruct an owned error in the runtime DSO.
    ///
    /// # Safety
    ///
    /// Non-empty text slices must reference live UTF-8 bytes allocated by the
    /// provider callback in the current PostgreSQL context.
    pub unsafe fn to_error(&self, callback: &'static str) -> PgReportError {
        let expected_size = size_of::<Self>() as u32;
        if self.struct_size != expected_size
            || self.sql_error_code
                == PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION as i32
            || !self.message.is_valid()
            || !self.detail.is_valid()
            || !self.hint.is_valid()
        {
            return PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("{callback} returned an invalid FFI error record"),
            );
        }
        // SAFETY: all three slices were validated above and are covered by the
        // method's exact-build callback contract.
        let message = unsafe { self.message.to_owned() };
        // SAFETY: the non-empty slice has the same validated lifetime and
        // provenance as `message`.
        let detail =
            (self.detail.len != 0).then(|| unsafe { self.detail.to_owned() });
        // SAFETY: the non-empty slice has the same validated lifetime and
        // provenance as `message`.
        let hint = (self.hint.len != 0).then(|| unsafe { self.hint.to_owned() });
        PgReportError::from_parts(self.sql_error_code.into(), message, detail, hint)
    }

    unsafe fn write(
        &mut self,
        error: PgReportError,
        memory_context: pg_sys::MemoryContext,
    ) {
        let sql_error_code = error.sql_error_code();
        let report = error.into_report();
        self.struct_size = size_of::<Self>() as u32;
        self.sql_error_code = sql_error_code as i32;
        // SAFETY: the caller guarantees that `memory_context` is live and this
        // method consumes each borrowed report string synchronously.
        self.message =
            unsafe { FfiErrorText::copy_from(report.message(), memory_context) };
        self.detail = report
            .detail()
            // SAFETY: same live context and synchronous copy as `message`.
            .map_or_else(FfiErrorText::default, |detail| unsafe {
                FfiErrorText::copy_from(detail, memory_context)
            });
        self.hint = report
            .hint()
            // SAFETY: same live context and synchronous copy as `message`.
            .map_or_else(FfiErrorText::default, |hint| unsafe {
                FfiErrorText::copy_from(hint, memory_context)
            });
    }
}
