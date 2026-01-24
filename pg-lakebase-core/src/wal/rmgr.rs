//! WAL Resource Manager trait and registration
//!
//! This module provides the core trait for implementing custom WAL resource managers
//! and the registration mechanism to hook them into PostgreSQL.

use crate::diag::{self, ReportableError};
use crate::wal::record::WalRecord;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;
use pgrx::{pg_guard, pg_sys};
use std::collections::HashMap;
use std::ffi::CString;
use std::sync::{Arc, OnceLock, RwLock};
use thiserror::Error;

/// Custom Resource Manager ID
///
/// PostgreSQL reserves IDs 0-127 for built-in resource managers.
/// Custom resource managers must use IDs 128-255.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RmgrId(u8);

impl RmgrId {
    /// Create a new RmgrId
    ///
    /// # Panics
    /// Panics if the ID is less than 128 (reserved for built-in resource managers)
    pub const fn new(id: u8) -> Self {
        assert!(id >= 128, "Custom resource manager IDs must be >= 128");
        Self(id)
    }

    /// Create a new RmgrId without validation
    ///
    /// # Safety
    /// The caller must ensure the ID is >= 128
    pub const unsafe fn new_unchecked(id: u8) -> Self {
        Self(id)
    }

    /// Get the raw ID value
    pub const fn as_u8(self) -> u8 {
        self.0
    }
}

impl From<RmgrId> for u8 {
    fn from(id: RmgrId) -> Self {
        id.0
    }
}

/// Error type for WAL resource manager operations
#[derive(Error, Debug)]
pub enum WalRmgrError {
    #[error("WAL redo failed: {0}")]
    RedoFailed(String),

    #[error("Invalid WAL record: {0}")]
    InvalidRecord(String),

    #[error("Resource manager already registered: {0}")]
    AlreadyRegistered(u8),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<WalRmgrError> for ErrorReport {
    fn from(value: WalRmgrError) -> Self {
        let error_message = format!("{value}");
        ErrorReport::new(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, error_message, "")
    }
}

/// Trait for implementing custom WAL resource managers
///
/// This trait defines the interface that PostgreSQL uses to handle
/// WAL records during recovery and replication.
///
/// # Required Methods
///
/// - `rmgr_id`: Returns the unique resource manager ID
/// - `name`: Returns the human-readable name
/// - `redo`: Called during recovery to replay a WAL record
///
/// # Optional Methods
///
/// - `desc`: Describe a WAL record (for pg_waldump and debugging)
/// - `identify`: Identify the record type by info byte
/// - `startup`: Called when the resource manager starts up
/// - `cleanup`: Called when the resource manager shuts down
/// - `mask`: Mask a page for backup consistency checks
pub trait WalResourceManager: Send + Sync + 'static {
    /// Returns the unique resource manager ID (must be >= 128)
    fn rmgr_id(&self) -> RmgrId;

    /// Returns the human-readable name for this resource manager
    fn name(&self) -> &'static str;

    /// Redo (replay) a WAL record during recovery
    ///
    /// This is the core recovery function. It must be idempotent -
    /// replaying the same record multiple times should have the same
    /// effect as replaying it once.
    fn redo(&self, record: &WalRecord) -> Result<(), WalRmgrError>;

    /// Describe a WAL record for debugging (pg_waldump)
    ///
    /// The default implementation returns a generic description.
    fn desc(&self, record: &WalRecord, buf: &mut String) {
        let _ = std::fmt::write(
            buf,
            format_args!("{} record, info={:#04x}", self.name(), record.info()),
        );
    }

    /// Identify the record type by info byte
    ///
    /// Returns a human-readable name for the operation type.
    fn identify(&self, _info: u8) -> Option<&'static str> {
        None
    }

    /// Called during resource manager startup
    ///
    /// This is invoked once when PostgreSQL starts or during recovery initialization.
    fn startup(&self) -> Result<(), WalRmgrError> {
        Ok(())
    }

    /// Called during resource manager cleanup
    ///
    /// This is invoked when PostgreSQL shuts down or recovery ends.
    fn cleanup(&self) -> Result<(), WalRmgrError> {
        Ok(())
    }

    /// Mask a page for backup consistency checks
    ///
    /// Used by pg_checksums and WAL consistency checking.
    /// Override if your pages have fields that change without WAL logging.
    fn mask(&self, _page: &mut [u8], _blkno: pg_sys::BlockNumber) {
        // Default: no masking
    }
}

// ============================================================================
// Thread-safe wrapper for RmgrData
// ============================================================================

/// Wrapper to make RmgrData Send + Sync
///
/// This is safe because:
/// 1. The rm_name pointer points to a CString we keep alive
/// 2. The function pointers are static and never change
/// 3. PostgreSQL only accesses this from a single backend process
struct RmgrDataWrapper {
    data: pg_sys::RmgrData,
}

// Safety: RmgrData contains only function pointers and a const char*
// that we ensure stays alive. PostgreSQL backends are single-threaded.
unsafe impl Send for RmgrDataWrapper {}
unsafe impl Sync for RmgrDataWrapper {}

// ============================================================================
// Registry and C callback trampolines
// ============================================================================

/// Internal storage for registered resource managers
struct RmgrRegistry {
    managers: HashMap<u8, Arc<dyn WalResourceManager>>,
    rmgr_data: HashMap<u8, RmgrDataWrapper>,
    name_storage: HashMap<u8, CString>,
}

impl RmgrRegistry {
    fn new() -> Self {
        Self {
            managers: HashMap::new(),
            rmgr_data: HashMap::new(),
            name_storage: HashMap::new(),
        }
    }
}

static REGISTRY: OnceLock<RwLock<RmgrRegistry>> = OnceLock::new();

fn get_registry() -> &'static RwLock<RmgrRegistry> {
    REGISTRY.get_or_init(|| RwLock::new(RmgrRegistry::new()))
}

/// Register a custom WAL resource manager
///
/// This function registers your resource manager with PostgreSQL's WAL system.
/// It should be called from your extension's `_PG_init` function.
///
/// # Arguments
/// * `manager` - Boxed instance of your resource manager
///
/// # Panics
/// Panics if registration with PostgreSQL fails
///
/// # Example
/// ```rust,no_run
/// use pg_lakebase_core::wal::{WalResourceManager, WalRecord, RmgrId, WalRmgrError, register_wal_rmgr};
///
/// const MY_RMGR_ID: RmgrId = RmgrId::new(128);
///
/// struct MyRmgr;
///
/// impl WalResourceManager for MyRmgr {
///     fn rmgr_id(&self) -> RmgrId { MY_RMGR_ID }
///     fn name(&self) -> &'static str { "my_rmgr" }
///
///     fn redo(&self, record: &WalRecord) -> Result<(), WalRmgrError> {
///         Ok(())
///     }
/// }
///
/// // In _PG_init:
/// register_wal_rmgr(Box::new(MyRmgr));
/// ```
pub fn register_wal_rmgr(manager: Box<dyn WalResourceManager>) {
    let rmgr_id = manager.rmgr_id().as_u8();
    let name = manager.name();

    let mut registry = get_registry().write().unwrap();

    if registry.managers.contains_key(&rmgr_id) {
        diag::report_error(
            PgSqlErrorCode::ERRCODE_DUPLICATE_OBJECT,
            &format!("WAL resource manager ID {} is already registered", rmgr_id),
        );
    }

    let manager_arc: Arc<dyn WalResourceManager> = Arc::from(manager);
    registry.managers.insert(rmgr_id, manager_arc);

    let c_name =
        CString::new(name).expect("Resource manager name contains null byte");
    registry.name_storage.insert(rmgr_id, c_name);

    let rmgr_data = RmgrDataWrapper {
        data: pg_sys::RmgrData {
            rm_name: registry.name_storage.get(&rmgr_id).unwrap().as_ptr(),
            rm_redo: Some(rmgr_redo_trampoline),
            rm_desc: Some(rmgr_desc_trampoline),
            rm_identify: Some(rmgr_identify_trampoline),
            rm_startup: Some(rmgr_startup_trampoline),
            rm_cleanup: Some(rmgr_cleanup_trampoline),
            rm_mask: Some(rmgr_mask_trampoline),
            rm_decode: None, // Logical decoding is complex, skip for now
        },
    };

    let rmgr_data_ptr = &rmgr_data.data as *const pg_sys::RmgrData;
    registry.rmgr_data.insert(rmgr_id, rmgr_data);

    unsafe {
        pg_sys::RegisterCustomRmgr(rmgr_id as pg_sys::RmgrId, rmgr_data_ptr);
    }

    diag::log_debug1(&format!(
        "Registered WAL resource manager '{}' with ID {}",
        name, rmgr_id
    ));
}

/// Get a registered resource manager by ID
fn get_manager(rmgr_id: u8) -> Option<Arc<dyn WalResourceManager>> {
    get_registry().read().ok()?.managers.get(&rmgr_id).cloned()
}

// ============================================================================
// C callback trampolines
// ============================================================================

/// Get the resource manager ID from the XLogReaderState
unsafe fn get_rmgr_id_from_record(record: *mut pg_sys::XLogReaderState) -> u8 {
    unsafe {
        let decoded = (*record).record;
        if decoded.is_null() {
            return 0;
        }
        (*decoded).header.xl_rmid
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn rmgr_redo_trampoline(
    record: *mut pg_sys::XLogReaderState,
) {
    unsafe {
        let rmgr_id = get_rmgr_id_from_record(record);
        if let Some(manager) = get_manager(rmgr_id) {
            let wal_record = WalRecord::from_raw(record);
            manager.redo(&wal_record).report_unwrap();
        } else {
            diag::report_error(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                &format!("No WAL resource manager registered for ID {}", rmgr_id),
            );
        }
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn rmgr_desc_trampoline(
    buf: *mut pg_sys::StringInfoData,
    record: *mut pg_sys::XLogReaderState,
) {
    unsafe {
        let rmgr_id = get_rmgr_id_from_record(record);
        if let Some(manager) = get_manager(rmgr_id) {
            let wal_record = WalRecord::from_raw(record);
            let mut desc = String::new();
            manager.desc(&wal_record, &mut desc);

            if !desc.is_empty() {
                if let Ok(c_desc) = CString::new(desc) {
                    pg_sys::appendStringInfoString(buf, c_desc.as_ptr());
                }
            }
        }
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn rmgr_identify_trampoline(
    _info: u8,
) -> *const std::ffi::c_char {
    // We need the rmgr_id here but it's not passed to identify.
    // PostgreSQL calls this after looking up the rmgr, so we need to track context.
    // For simplicity, we'll return a generic response.
    // A more complete implementation would use thread-local storage.
    static GENERIC: &[u8] = b"Generic\0";
    GENERIC.as_ptr() as *const std::ffi::c_char
}

#[pg_guard]
unsafe extern "C-unwind" fn rmgr_startup_trampoline() {
    // Startup is called for each registered rmgr
    // We iterate through all our managers
    if let Ok(registry) = get_registry().read() {
        for manager in registry.managers.values() {
            manager.startup().report_unwrap();
        }
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn rmgr_cleanup_trampoline() {
    if let Ok(registry) = get_registry().read() {
        for manager in registry.managers.values() {
            manager.cleanup().report_unwrap();
        }
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn rmgr_mask_trampoline(
    page: *mut std::ffi::c_char,
    blkno: pg_sys::BlockNumber,
) {
    // Mask is also called without rmgr context, need to handle this differently
    // For now, we'll iterate through all managers (not ideal but works)
    unsafe {
        if let Ok(registry) = get_registry().read() {
            for manager in registry.managers.values() {
                let page_slice = std::slice::from_raw_parts_mut(
                    page as *mut u8,
                    pg_sys::BLCKSZ as usize,
                );
                manager.mask(page_slice, blkno);
            }
        }
    }
}

// ============================================================================
// Helper functions for WAL operations
// ============================================================================

/// Check if we're currently in recovery mode
pub fn is_in_recovery() -> bool {
    unsafe { pg_sys::RecoveryInProgress() }
}

/// Get the current WAL insert position
pub fn get_current_lsn() -> crate::wal::XLogRecPtr {
    unsafe { pg_sys::GetXLogInsertRecPtr() }
}

/// Flush WAL up to the specified LSN
///
/// This ensures that all WAL records up to and including the given LSN
/// are written to disk.
pub fn flush_wal(lsn: crate::wal::XLogRecPtr) {
    unsafe {
        pg_sys::XLogFlush(lsn);
    }
}
