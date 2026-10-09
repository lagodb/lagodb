use super::PgWrapper;
use crate::diag::PgError;
use pgrx::pg_sys::ffi::pg_guard_ffi_boundary;
use pgrx::{PgTryBuilder, pg_sys};
use std::ffi::c_int;

// postmaster/bgwriter.h is not included in pgrx's generated bindings.
unsafe extern "C-unwind" {
    fn RequestCheckpoint(flags: c_int);
}

impl PgWrapper {
    pub(crate) fn request_checkpoint(flags: c_int) -> Result<(), PgError> {
        PgTryBuilder::new(move || {
            // SAFETY: called on the PostgreSQL backend thread. The guard
            // converts PG longjmp into Rust unwinding before it is caught.
            unsafe { pg_guard_ffi_boundary(|| RequestCheckpoint(flags)) };
            Ok(())
        })
        .catch_others(|error| Err(PgError::from_caught(error)))
        .execute()
    }

    /// Check if WAL logging is needed for general operations.
    ///
    /// This is equivalent to PostgreSQL's `XLogIsNeeded()` macro.
    pub(crate) fn xlog_is_needed() -> bool {
        unsafe { pg_sys::wal_level >= pg_sys::WalLevel::WAL_LEVEL_REPLICA as i32 }
    }

    /// True when the system is in recovery and the current recovery mode is
    /// crash recovery rather than archive or standby recovery.
    pub(crate) fn is_crash_recovery_only() -> bool {
        unsafe {
            pg_sys::RecoveryInProgress()
                && !pg_sys::ArchiveRecoveryRequested
                && !pg_sys::StandbyMode
        }
    }
}
