//! WAL Resource Manager trait and registration
//!
//! This module provides the core trait for implementing custom WAL resource managers
//! and the registration mechanism to hook them into PostgreSQL.
//!
//! # Callback error contract
//!
//! `redo`, `startup`, and `cleanup` cannot return a `Result` through PostgreSQL's
//! `RmgrData` C callbacks. The registry therefore reports every returned
//! [`WalRmgrError`] as a PostgreSQL `ERROR` through `report_unwrap`.
//! PostgreSQL invokes these callbacks during recovery without a query-level
//! exception handler; in that context PostgreSQL promotes the `ERROR` to
//! `FATAL`, which terminates the recovery process. Providers must handle
//! explicitly lossy conditions themselves, record the warning, and return
//! `Ok(())`; the core layer does not reinterpret provider errors as `PANIC`.

use crate::diag::{self, ReportableError};
use crate::wal::record::WalRecord;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;
use pgrx::{pg_guard, pg_sys};
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{CString, c_char};
use std::sync::OnceLock;
use thiserror::Error;

const MIN_CUSTOM_RMGR_ID: u8 = pg_sys::RM_MIN_CUSTOM_ID as u8;
const MAX_CUSTOM_RMGRS: usize = pg_sys::RM_N_CUSTOM_IDS as usize;

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
        assert!(
            id >= MIN_CUSTOM_RMGR_ID,
            "Custom resource manager IDs must be in PostgreSQL's custom rmgr range"
        );
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
///
/// The core WAL registry reports this error as PostgreSQL `ERROR`. When the
/// callback runs in PostgreSQL recovery, the absence of a query-level
/// exception handler promotes that `ERROR` to `FATAL` and terminates the
/// recovery process. Conditions that are explicitly lossy must be handled by
/// the provider and returned as `Ok(())` instead of this error.
#[derive(Error, Debug)]
pub enum WalRmgrError {
    #[error("WAL redo failed: {0}")]
    RedoFailed(String),

    #[error("Invalid WAL record: {0}")]
    InvalidRecord(String),

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
/// This framework supports multiple custom resource managers in one extension
/// process. Each manager owns one PostgreSQL custom resource manager ID.
/// PostgreSQL's `RmgrData` callbacks for `identify`, `startup`, `cleanup`, and
/// `mask` do not receive the resource manager ID, so this module registers
/// per-ID static trampolines and keeps the unsafe dispatch boundary inside the
/// core WAL layer.
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
    ///
    /// # Errors
    ///
    /// Returning an error causes the framework to report PostgreSQL `ERROR`.
    /// During recovery, PostgreSQL promotes that error to `FATAL` because the
    /// callback is not inside a query-level exception handler.
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
    ///
    /// # Errors
    ///
    /// Returning an error causes the framework to report PostgreSQL `ERROR`.
    /// During recovery, PostgreSQL promotes that error to `FATAL` because the
    /// callback is not inside a query-level exception handler.
    fn startup(&self) -> Result<(), WalRmgrError> {
        Ok(())
    }

    /// Called during resource manager cleanup
    ///
    /// This is invoked when PostgreSQL shuts down or recovery ends.
    ///
    /// # Errors
    ///
    /// Returning an error causes the framework to report PostgreSQL `ERROR`.
    /// During recovery, PostgreSQL promotes that error to `FATAL` because the
    /// callback is not inside a query-level exception handler.
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

impl RmgrDataWrapper {
    fn as_ptr(&self) -> *const pg_sys::RmgrData {
        &self.data
    }
}

// ============================================================================
// Registry and C callback trampolines
// ============================================================================

#[derive(Clone, Copy)]
struct RmgrTrampolines {
    redo: unsafe extern "C-unwind" fn(*mut pg_sys::XLogReaderState),
    desc: unsafe extern "C-unwind" fn(
        *mut pg_sys::StringInfoData,
        *mut pg_sys::XLogReaderState,
    ),
    identify: unsafe extern "C-unwind" fn(u8) -> *const c_char,
    startup: unsafe extern "C-unwind" fn(),
    cleanup: unsafe extern "C-unwind" fn(),
    mask: unsafe extern "C-unwind" fn(*mut c_char, pg_sys::BlockNumber),
}

impl RmgrTrampolines {
    const fn for_id<const RMGR_ID: u8>() -> Self {
        Self {
            redo: rmgr_redo_trampoline::<RMGR_ID>,
            desc: rmgr_desc_trampoline::<RMGR_ID>,
            identify: rmgr_identify_trampoline::<RMGR_ID>,
            startup: rmgr_startup_trampoline::<RMGR_ID>,
            cleanup: rmgr_cleanup_trampoline::<RMGR_ID>,
            mask: rmgr_mask_trampoline::<RMGR_ID>,
        }
    }
}

struct RegisteredRmgr {
    manager: Box<dyn WalResourceManager>,
    rmgr_id: u8,
    rmgr_data: RmgrDataWrapper,
    _name_storage: CString,
}

impl RegisteredRmgr {
    fn new(
        manager: Box<dyn WalResourceManager>,
        rmgr_id: u8,
        trampolines: RmgrTrampolines,
    ) -> Self {
        let name = manager.name();
        let name_storage =
            CString::new(name).expect("Resource manager name contains null byte");
        let rmgr_data = RmgrDataWrapper {
            data: pg_sys::RmgrData {
                rm_name: name_storage.as_ptr(),
                rm_redo: Some(trampolines.redo),
                rm_desc: Some(trampolines.desc),
                rm_identify: Some(trampolines.identify),
                rm_startup: Some(trampolines.startup),
                rm_cleanup: Some(trampolines.cleanup),
                rm_mask: Some(trampolines.mask),
                rm_decode: None, // Logical decoding is complex, skip for now
            },
        };

        Self {
            manager,
            rmgr_id,
            rmgr_data,
            _name_storage: name_storage,
        }
    }

    fn identify_ptr(&self, info: u8) -> *const c_char {
        let Some(name) = self.manager.identify(info) else {
            return std::ptr::null();
        };

        IDENTIFY_NAMES.with_borrow_mut(|identify_names| {
            use std::collections::hash_map::Entry;
            match identify_names.entry((self.rmgr_id, info)) {
                Entry::Occupied(entry) => entry.get().as_ptr(),
                Entry::Vacant(entry) => {
                    let Ok(c_name) = CString::new(name) else {
                        return std::ptr::null();
                    };
                    entry.insert(c_name).as_ptr()
                }
            }
        })
    }
}

thread_local! {
    // PostgreSQL WAL callbacks execute on one thread per backend/recovery
    // process. CString allocations remain alive for that process lifetime.
    static IDENTIFY_NAMES: RefCell<HashMap<(u8, u8), CString>> = RefCell::new(HashMap::new());
}

struct WalRmgrRegistry {
    slots: [OnceLock<RegisteredRmgr>; MAX_CUSTOM_RMGRS],
}

impl WalRmgrRegistry {
    const fn new() -> Self {
        Self {
            slots: [const { OnceLock::new() }; MAX_CUSTOM_RMGRS],
        }
    }

    fn slot(&self, rmgr_id: u8) -> Option<&OnceLock<RegisteredRmgr>> {
        let index = custom_rmgr_index(rmgr_id)?;
        self.slots.get(index)
    }

    fn get(&self, rmgr_id: u8) -> Option<&RegisteredRmgr> {
        self.slot(rmgr_id)?.get()
    }

    fn register(
        &'static self,
        registered: RegisteredRmgr,
    ) -> Result<&'static RegisteredRmgr, u8> {
        let rmgr_id = registered.rmgr_id;
        let Some(slot) = self.slot(rmgr_id) else {
            return Err(rmgr_id);
        };

        if slot.set(registered).is_err() {
            return Err(rmgr_id);
        }

        Ok(slot
            .get()
            .expect("registered WAL resource manager must be initialized"))
    }

    unsafe fn redo(&'static self, rmgr_id: u8, record: *mut pg_sys::XLogReaderState) {
        unsafe {
            let record_rmgr_id = get_rmgr_id_from_record(record);
            if record_rmgr_id != rmgr_id {
                diag::report_error(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format_args!(
                        "WAL record manager ID {} reached trampoline for ID {}",
                        record_rmgr_id, rmgr_id
                    ),
                );
                return;
            }

            if let Some(registered) = self.get(rmgr_id) {
                let wal_record = WalRecord::from_raw(record);
                registered.manager.redo(&wal_record).report_unwrap();
            } else {
                diag::report_error(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format_args!(
                        "No WAL resource manager registered for ID {}",
                        rmgr_id
                    ),
                );
            }
        }
    }

    unsafe fn desc(
        &'static self,
        rmgr_id: u8,
        buf: *mut pg_sys::StringInfoData,
        record: *mut pg_sys::XLogReaderState,
    ) {
        unsafe {
            if get_rmgr_id_from_record(record) != rmgr_id {
                return;
            }

            if let Some(registered) = self.get(rmgr_id) {
                let wal_record = WalRecord::from_raw(record);
                let mut desc = String::new();
                registered.manager.desc(&wal_record, &mut desc);

                if !desc.is_empty()
                    && let Ok(c_desc) = CString::new(desc)
                {
                    pg_sys::appendStringInfoString(buf, c_desc.as_ptr());
                }
            }
        }
    }

    fn identify(&'static self, rmgr_id: u8, info: u8) -> *const c_char {
        self.get(rmgr_id)
            .map_or(std::ptr::null(), |registered| registered.identify_ptr(info))
    }

    fn startup(&'static self, rmgr_id: u8) {
        if let Some(registered) = self.get(rmgr_id) {
            registered.manager.startup().report_unwrap();
        }
    }

    fn cleanup(&'static self, rmgr_id: u8) {
        if let Some(registered) = self.get(rmgr_id) {
            registered.manager.cleanup().report_unwrap();
        }
    }

    unsafe fn mask(
        &'static self,
        rmgr_id: u8,
        page: *mut c_char,
        blkno: pg_sys::BlockNumber,
    ) {
        unsafe {
            if let Some(registered) = self.get(rmgr_id)
                && !page.is_null()
            {
                let page_slice = std::slice::from_raw_parts_mut(
                    page as *mut u8,
                    pg_sys::BLCKSZ as usize,
                );
                registered.manager.mask(page_slice, blkno);
            }
        }
    }
}

static RMGR_REGISTRY: WalRmgrRegistry = WalRmgrRegistry::new();

fn custom_rmgr_index(rmgr_id: u8) -> Option<usize> {
    if rmgr_id < MIN_CUSTOM_RMGR_ID {
        return None;
    }

    Some((rmgr_id - MIN_CUSTOM_RMGR_ID) as usize)
}

fn registered_rmgr(rmgr_id: u8) -> Option<&'static RegisteredRmgr> {
    RMGR_REGISTRY.get(rmgr_id)
}

/// Register a custom WAL resource manager
///
/// This function registers your resource manager with PostgreSQL's WAL system.
/// It must be called from `_PG_init` while PostgreSQL is loading the extension
/// through `shared_preload_libraries`. PostgreSQL will ERROR if a custom WAL
/// resource manager is registered outside that initialization window.
///
/// This framework supports multiple custom WAL resource managers per extension
/// process. Each manager must use a distinct custom resource manager ID.
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
/// const MY_RMGR_ID_U8: u8 = 128;
/// const MY_RMGR_ID: RmgrId = RmgrId::new(MY_RMGR_ID_U8);
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
/// // In _PG_init, from an extension loaded via shared_preload_libraries:
/// register_wal_rmgr::<MY_RMGR_ID_U8>(Box::new(MyRmgr));
/// ```
pub fn register_wal_rmgr<const RMGR_ID: u8>(manager: Box<dyn WalResourceManager>) {
    const {
        assert!(
            RMGR_ID >= MIN_CUSTOM_RMGR_ID,
            "Custom resource manager IDs must be in PostgreSQL's custom rmgr range"
        );
    }

    let rmgr_id = RMGR_ID;

    let manager_rmgr_id = manager.rmgr_id().as_u8();
    if manager_rmgr_id != rmgr_id {
        diag::report_error(
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            format_args!(
                "WAL resource manager '{}' declared ID {} but was registered for ID {}",
                manager.name(),
                manager_rmgr_id,
                rmgr_id
            ),
        );
        return;
    }

    if registered_rmgr(rmgr_id).is_some() {
        diag::report_error(
            PgSqlErrorCode::ERRCODE_DUPLICATE_OBJECT,
            format_args!("WAL resource manager ID {} is already registered", rmgr_id),
        );
        return;
    }

    let registered =
        RegisteredRmgr::new(manager, rmgr_id, RmgrTrampolines::for_id::<RMGR_ID>());
    let name = registered.manager.name();

    let Ok(registered) = RMGR_REGISTRY.register(registered) else {
        diag::report_error(
            PgSqlErrorCode::ERRCODE_DUPLICATE_OBJECT,
            format_args!("WAL resource manager ID {} is already registered", rmgr_id),
        );
        return;
    };

    unsafe {
        pg_sys::RegisterCustomRmgr(
            rmgr_id as pg_sys::RmgrId,
            registered.rmgr_data.as_ptr(),
        );
    }

    diag::log_debug1(format_args!(
        "Registered WAL resource manager '{}' with ID {}",
        name, rmgr_id
    ));
}

// ============================================================================
// C callback trampolines
// ============================================================================

/// Get the resource manager ID from the XLogReaderState
unsafe fn get_rmgr_id_from_record(record: *mut pg_sys::XLogReaderState) -> u8 {
    unsafe {
        if record.is_null() {
            return 0;
        }
        let decoded = (*record).record;
        if decoded.is_null() {
            return 0;
        }
        (*decoded).header.xl_rmid
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn rmgr_redo_trampoline<const RMGR_ID: u8>(
    record: *mut pg_sys::XLogReaderState,
) {
    unsafe {
        RMGR_REGISTRY.redo(RMGR_ID, record);
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn rmgr_desc_trampoline<const RMGR_ID: u8>(
    buf: *mut pg_sys::StringInfoData,
    record: *mut pg_sys::XLogReaderState,
) {
    unsafe {
        RMGR_REGISTRY.desc(RMGR_ID, buf, record);
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn rmgr_identify_trampoline<const RMGR_ID: u8>(
    info: u8,
) -> *const c_char {
    RMGR_REGISTRY.identify(RMGR_ID, info)
}

#[pg_guard]
unsafe extern "C-unwind" fn rmgr_startup_trampoline<const RMGR_ID: u8>() {
    RMGR_REGISTRY.startup(RMGR_ID);
}

#[pg_guard]
unsafe extern "C-unwind" fn rmgr_cleanup_trampoline<const RMGR_ID: u8>() {
    RMGR_REGISTRY.cleanup(RMGR_ID);
}

#[pg_guard]
unsafe extern "C-unwind" fn rmgr_mask_trampoline<const RMGR_ID: u8>(
    page: *mut c_char,
    blkno: pg_sys::BlockNumber,
) {
    unsafe {
        RMGR_REGISTRY.mask(RMGR_ID, page, blkno);
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
