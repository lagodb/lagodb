//! Custom WAL Resource Manager Framework
//!
//! This module provides a safe, ergonomic API for implementing custom WAL (Write-Ahead Logging)
//! resource managers in PostgreSQL extensions.
//!
//! # Overview
//!
//! PostgreSQL's WAL system ensures durability and crash recovery. Custom Resource Managers
//! allow extensions to integrate their own data structures into this mechanism.
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use pg_lakehouse_core::wal::{WalResourceManager, WalRecord, RmgrId, register_wal_rmgr, WalRmgrError};
//!
//! const MY_RMGR_ID: RmgrId = RmgrId::new(128); // Custom IDs start at 128
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
//! // In _PG_init:
//! register_wal_rmgr(Box::new(MyRmgr));
//! ```
//!

mod record;
mod rmgr;

pub use record::{WalRecord, WalRecordBuilder, XLogRecPtr, record_flags};
pub use rmgr::{
    flush_wal, get_current_lsn, is_in_recovery, register_wal_rmgr, RmgrId, WalRmgrError,
    WalResourceManager,
};
