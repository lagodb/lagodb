use super::PgWrapper;
use pgrx::pg_sys;

impl PgWrapper {
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
