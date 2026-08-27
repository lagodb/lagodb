//! Custom WAL Resource Manager Framework
//!
//! This module provides a safe, ergonomic API for implementing custom WAL (Write-Ahead Logging)
//! resource managers in PostgreSQL extensions.
//!
//! # Overview
//!
//! PostgreSQL's WAL system ensures durability and crash recovery. Custom Resource Managers
//! allow extensions to integrate their own data structures into this mechanism.
//! The core WAL layer supports multiple custom resource managers in one PostgreSQL
//! process, so different table access methods can each register their own custom
//! resource manager ID.
//!
//! # Quick Start
//!
//! Custom WAL resource managers must be registered while PostgreSQL is loading
//! the extension through `shared_preload_libraries`.
//!
//! ```rust,no_run
//! use lagodb_core::wal::{WalResourceManager, WalRecord, RmgrId, register_wal_rmgr, WalRmgrError};
//!
//! const MY_RMGR_ID_U8: u8 = 128; // Custom IDs start at 128
//! const MY_RMGR_ID: RmgrId = RmgrId::new(MY_RMGR_ID_U8);
//!
//! struct MyRmgr;
//!
//! impl WalResourceManager for MyRmgr {
//!     fn rmgr_id(&self) -> RmgrId { MY_RMGR_ID }
//!     fn name(&self) -> &'static str { "my_rmgr" }
//!
//!     fn redo(&self, record: &WalRecord) -> Result<(), WalRmgrError> {
//!         // Replay the WAL record during recovery
//!         Ok(())
//!     }
//! }
//!
//! // In _PG_init, from an extension loaded via shared_preload_libraries:
//! register_wal_rmgr::<MY_RMGR_ID_U8>(Box::new(MyRmgr));
//! ```
//!

mod record;
mod rmgr;

pub use record::{
    WalRecord, WalRecordBuilder, XLogRecPtr, buffer_flags, record_flags,
};
pub use rmgr::{
    RmgrId, WalResourceManager, WalRmgrError, flush_wal, get_current_lsn,
    is_in_recovery, register_wal_rmgr,
};

/// Check if WAL logging is needed for general operations.
///
/// This is equivalent to PostgreSQL's `XLogIsNeeded()` macro.
pub fn xlog_is_needed() -> bool {
    crate::wrapper::PgWrapper::xlog_is_needed()
}

/// True when the system is in recovery and the current recovery mode is crash
/// recovery rather than archive or standby recovery.
pub fn is_crash_recovery_only() -> bool {
    crate::wrapper::PgWrapper::is_crash_recovery_only()
}
