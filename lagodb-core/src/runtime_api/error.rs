//! Stage-neutral error transport for exact-build runtime callbacks.

use std::mem::size_of;
use std::panic::AssertUnwindSafe;
use std::{ptr, slice, str};

use pgrx::prelude::PgSqlErrorCode;
use pgrx::{PgMemoryContexts, PgTryBuilder, pg_sys};

use crate::diag::PgReportError;

pub const CALLBACK_OK: u32 = 0;
pub const CALLBACK_FAILED: u32 = 1;

/// Marker returned after an operation error has been copied into an
/// [`CallbackErrorReport`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("callback failed; see the error report for details")]
pub struct CallbackFailed;

#[repr(C)]
#[derive(Clone, Copy)]
struct ErrorReportText {
    data: *const u8,
    len: usize,
}

impl Default for ErrorReportText {
    fn default() -> Self {
        Self {
            data: ptr::null(),
            len: 0,
        }
    }
}

impl ErrorReportText {
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
pub struct CallbackErrorReport {
    struct_size: u32,
    sql_error_code: i32,
    message: ErrorReportText,
    detail: ErrorReportText,
    hint: ErrorReportText,
}

impl Default for CallbackErrorReport {
    fn default() -> Self {
        Self {
            struct_size: size_of::<Self>() as u32,
            sql_error_code: 0,
            message: ErrorReportText::default(),
            detail: ErrorReportText::default(),
            hint: ErrorReportText::default(),
        }
    }
}

impl CallbackErrorReport {
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
            Ok(()) => CALLBACK_OK,
            Err(CallbackFailed) => CALLBACK_FAILED,
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
    ) -> Result<T, CallbackFailed> {
        *self = Self::default();
        // Preserve the caller's context across a caught PostgreSQL ERROR;
        // error handling may temporarily switch CurrentMemoryContext.
        let memory_context = unsafe { pg_sys::CurrentMemoryContext };
        // There are two materially different panic paths here:
        //
        // * `pgrx::error!` starts as an ErrorReport panic. If caught here, it
        //   has not yet entered PostgreSQL's ERROR handler.
        // * An ERROR raised by a pgrx-wrapped `pg_sys` call first passes
        //   through PostgreSQL's error handler, which resets
        //   InterruptHoldoffCount, and is then resumed by pgrx as
        //   CaughtError::PostgresError.
        //
        // This is the provider callback's C ABI error boundary. PgTryBuilder
        // catches both as Rust panics, performs pgrx's PostgreSQL error-state
        // cleanup, and lets this function return the error as data. Downstream
        // query code therefore receives a PgReportError through Result and
        // must not add another PgTryBuilder merely to propagate it.
        //
        // Callers must not span this boundary with a RAII interrupt hold whose
        // Drop blindly performs RESUME_INTERRUPTS; the owner of such a hold
        // must also own restoration after the second path. Rust unwinding does
        // run Drop—the subtlety is that PostgreSQL has already changed the
        // underlying counter before unwinding begins.
        let result = PgTryBuilder::new(AssertUnwindSafe(operation))
            .catch_others(|error| Err(PgReportError::from_caught(error)))
            .execute();
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                // SAFETY: `memory_context` was captured while live immediately
                // before the protected operation and this record is writable.
                unsafe { self.write(error, memory_context) };
                Err(CallbackFailed)
            }
        }
    }

    #[must_use]
    pub const fn is_set(&self) -> bool {
        self.sql_error_code != 0
    }

    /// Decode a synchronous callback status without reporting to PostgreSQL.
    ///
    /// # Safety
    ///
    /// On failure, this record must contain the callback's diagnostic payload
    /// and its PostgreSQL-owned text must remain live until this method returns.
    pub unsafe fn into_result(
        self,
        status: u32,
        callback: &'static str,
    ) -> Result<(), PgReportError> {
        if status == CALLBACK_OK {
            Ok(())
        } else {
            // SAFETY: the caller keeps the synchronous callback payload live.
            Err(unsafe { self.to_error(callback) })
        }
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
                format!("{callback} returned an invalid callback error report"),
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
            unsafe { ErrorReportText::copy_from(report.message(), memory_context) };
        self.detail = report
            .detail()
            // SAFETY: same live context and synchronous copy as `message`.
            .map_or_else(ErrorReportText::default, |detail| unsafe {
                ErrorReportText::copy_from(detail, memory_context)
            });
        self.hint = report
            .hint()
            // SAFETY: same live context and synchronous copy as `message`.
            .map_or_else(ErrorReportText::default, |hint| unsafe {
                ErrorReportText::copy_from(hint, memory_context)
            });
    }
}
