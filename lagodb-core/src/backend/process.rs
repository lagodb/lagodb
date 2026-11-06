//! PostgreSQL backend-process lifecycle operations.

use core::ffi::c_int;

use pgrx::pg_sys;

use crate::diag::PgReportError;

/// Lifecycle operations for the current PostgreSQL backend process.
pub struct BackendProcess;

impl BackendProcess {
    /// PostgreSQL's process number that identifies no backend.
    #[cfg(feature = "pg16")]
    pub const INVALID_PROC_NUMBER: c_int = pg_sys::INVALID_PGPROCNO as c_int;
    /// PostgreSQL's process number that identifies no backend.
    #[cfg(any(feature = "pg17", feature = "pg18"))]
    pub const INVALID_PROC_NUMBER: c_int = pg_sys::INVALID_PROC_NUMBER;

    /// This backend's index in PostgreSQL's `PGPROC` array.
    #[inline]
    pub fn proc_number() -> c_int {
        // SAFETY: PostgreSQL publishes this identity before it enters extension
        // code and keeps it for the life of the backend.
        #[cfg(feature = "pg16")]
        unsafe {
            (*pg_sys::MyProc).pgprocno
        }
        // SAFETY: as above.
        #[cfg(any(feature = "pg17", feature = "pg18"))]
        unsafe {
            pg_sys::MyProcNumber
        }
    }

    /// Whether a query cancel or backend termination request is waiting.
    #[inline]
    pub fn query_interrupt_pending() -> bool {
        // SAFETY: signal flags are read only on the backend main thread.
        unsafe { pg_sys::QueryCancelPending != 0 || pg_sys::ProcDiePending != 0 }
    }

    /// Whether `CHECK_FOR_INTERRUPTS` currently defers every interrupt.
    #[inline]
    pub fn interrupts_held() -> bool {
        // SAFETY: the counter is backend-local and read on the backend thread.
        unsafe { pg_sys::InterruptHoldoffCount > 0 }
    }

    /// Service pending interrupts, returning a cancel as a value so callers on
    /// a spawned runtime task do not unwind through it.
    pub fn check_interrupts() -> Result<(), PgReportError> {
        PgReportError::capture(|| {
            pg_sys::check_for_interrupts!();
        })
    }

    /// Run PostgreSQL's registered process-exit callbacks and terminate the
    /// current backend with `code`.
    pub fn exit(code: c_int) -> ! {
        #[cfg(any(feature = "pg16", feature = "pg17"))]
        unsafe {
            // SAFETY: PostgreSQL owns the current backend process and exposes
            // proc_exit as its normal callback-aware termination path.
            pg_sys::proc_exit(code)
        }

        #[cfg(feature = "pg18")]
        {
            // SAFETY: PostgreSQL owns the current backend process and exposes
            // proc_exit as its normal callback-aware termination path. PG18
            // still declares it pg_noreturn, although bindgen loses that fact.
            unsafe { pg_sys::proc_exit(code) };
            unreachable!("PostgreSQL proc_exit returned despite pg_noreturn")
        }
    }
}
