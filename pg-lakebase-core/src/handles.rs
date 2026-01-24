//! Safe wrapper types for PostgreSQL FFI types
//!
//! This module provides safe Rust wrappers around raw C pointers and types
//! from PostgreSQL. These wrappers ensure memory safety and provide a clean
//! API for interacting with PostgreSQL's C API.

use crate::pg_wrapper::{PgWrapper, PgWrapperError};
use pgrx::pg_sys;

#[derive(Debug)]
pub struct RelationHandle<'a> {
    inner: pg_sys::Relation,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> RelationHandle<'a> {
    #[inline]
    pub unsafe fn from_raw(ptr: pg_sys::Relation) -> Self {
        Self {
            inner: ptr,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn as_raw(&self) -> pg_sys::Relation {
        self.inner
    }

    #[inline]
    pub fn tablespace_oid(&self) -> pg_sys::Oid {
        unsafe { (*(*self.inner).rd_rel).reltablespace }
    }

    #[inline]
    pub fn namespace_oid(&self) -> pg_sys::Oid {
        unsafe { (*(*self.inner).rd_rel).relnamespace }
    }

    #[inline]
    pub fn relation_name(&self) -> String {
        unsafe {
            let name_ptr = (*(*self.inner).rd_rel).relname.data.as_ptr();
            std::ffi::CStr::from_ptr(name_ptr)
                .to_string_lossy()
                .to_string()
        }
    }

    #[inline]
    pub fn relkind(&self) -> i8 {
        unsafe { (*(*self.inner).rd_rel).relkind }
    }

    /// Check if the relation needs WAL logging.
    ///
    /// This is equivalent to PostgreSQL's `RelationNeedsWAL(rel)` macro.
    /// A relation needs WAL if its persistence is set to permanent.
    #[inline]
    pub fn needs_wal(&self) -> bool {
        unsafe { PgWrapper::relation_needs_wal(self.inner) }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RelFileLocator {
    pub spc_oid: pg_sys::Oid,
    pub db_oid: pg_sys::Oid,
    pub rel_number: pg_sys::RelFileNumber,
}

impl RelFileLocator {
    #[inline]
    pub unsafe fn from_raw_unchecked(ptr: *const pg_sys::RelFileLocator) -> Self {
        unsafe {
            let sys = &*ptr;
            Self {
                spc_oid: sys.spcOid,
                db_oid: sys.dbOid,
                rel_number: sys.relNumber,
            }
        }
    }

    #[inline]
    pub unsafe fn from_raw(ptr: *const pg_sys::RelFileLocator) -> Option<Self> {
        unsafe {
            if ptr.is_null() {
                None
            } else {
                Some(Self::from_raw_unchecked(ptr))
            }
        }
    }
}

/// Safe wrapper for PostgreSQL Snapshot
#[derive(Debug)]
pub struct SnapshotHandle<'a> {
    inner: pg_sys::Snapshot,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> SnapshotHandle<'a> {
    #[inline]
    pub unsafe fn from_raw(ptr: pg_sys::Snapshot) -> Self {
        Self {
            inner: ptr,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn as_raw(&self) -> pg_sys::Snapshot {
        self.inner
    }

    #[inline]
    pub fn xmin(&self) -> pg_sys::TransactionId {
        unsafe { (*self.inner).xmin }
    }

    #[inline]
    pub fn xmax(&self) -> pg_sys::TransactionId {
        unsafe { (*self.inner).xmax }
    }
}

/// Safe wrapper for PostgreSQL TupleTableSlot
#[derive(Debug)]
pub struct TupleTableSlotHandle<'a> {
    inner: *mut pg_sys::TupleTableSlot,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> TupleTableSlotHandle<'a> {
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::TupleTableSlot) -> Self {
        Self {
            inner: ptr,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn as_raw(&self) -> *mut pg_sys::TupleTableSlot {
        self.inner
    }
}

/// Safe wrapper for PostgreSQL ItemPointer (TID)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ItemPointer {
    pub block_number: u32,
    pub offset: u16,
}

impl ItemPointer {
    #[inline]
    pub unsafe fn from_raw(ptr: pg_sys::ItemPointer) -> Self {
        unsafe {
            let sys = *ptr;
            let block_number =
                (sys.ip_blkid.bi_hi as u32) << 16 | (sys.ip_blkid.bi_lo as u32);
            Self {
                block_number,
                offset: sys.ip_posid,
            }
        }
    }

    #[inline]
    pub fn to_pg_sys(&self) -> pg_sys::ItemPointerData {
        pg_sys::ItemPointerData {
            ip_blkid: pg_sys::BlockIdData {
                bi_hi: (self.block_number >> 16) as u16,
                bi_lo: (self.block_number & 0xFFFF) as u16,
            },
            ip_posid: self.offset,
        }
    }
}

impl Default for ItemPointer {
    fn default() -> Self {
        Self {
            block_number: 0,
            offset: 0,
        }
    }
}

/// Safe wrapper for PostgreSQL BufferAccessStrategy
#[derive(Debug)]
pub struct BufferAccessStrategyHandle<'a> {
    inner: pg_sys::BufferAccessStrategy,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> BufferAccessStrategyHandle<'a> {
    #[inline]
    pub unsafe fn from_raw(ptr: pg_sys::BufferAccessStrategy) -> Self {
        Self {
            inner: ptr,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn as_raw(&self) -> pg_sys::BufferAccessStrategy {
        self.inner
    }
}

/// Safe wrapper for PostgreSQL VacuumParams
#[derive(Debug)]
pub struct VacuumParamsHandle<'a> {
    inner: &'a mut pg_sys::VacuumParams,
}

impl<'a> VacuumParamsHandle<'a> {
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::VacuumParams) -> Self {
        unsafe { Self { inner: &mut *ptr } }
    }

    #[inline]
    pub fn as_mut(&mut self) -> &mut pg_sys::VacuumParams {
        self.inner
    }
}

/// Safe wrapper for attribute widths array
#[derive(Debug)]
pub struct AttrWidthsHandle<'a> {
    inner: &'a mut [i32],
}

impl<'a> AttrWidthsHandle<'a> {
    #[inline]
    pub unsafe fn from_raw(ptr: *mut i32, len: usize) -> Option<Self> {
        unsafe {
            if ptr.is_null() {
                None
            } else {
                Some(Self {
                    inner: std::slice::from_raw_parts_mut(ptr, len),
                })
            }
        }
    }

    #[inline]
    pub fn as_slice_mut(&mut self) -> &mut [i32] {
        self.inner
    }
}

/// Safe wrapper for varlena pointer
#[derive(Debug)]
pub struct VarlenaHandle<'a> {
    inner: *mut pg_sys::varlena,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> VarlenaHandle<'a> {
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::varlena) -> Self {
        Self {
            inner: ptr,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn as_raw(&self) -> *mut pg_sys::varlena {
        self.inner
    }
}

/// Safe wrapper for PostgreSQL IndexInfo
#[derive(Debug)]
pub struct IndexInfoHandle<'a> {
    inner: *mut pg_sys::IndexInfo,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> IndexInfoHandle<'a> {
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::IndexInfo) -> Self {
        Self {
            inner: ptr,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn as_raw(&self) -> *mut pg_sys::IndexInfo {
        self.inner
    }
}

/// Safe wrapper for PostgreSQL ValidateIndexState
#[derive(Debug)]
pub struct ValidateIndexStateHandle<'a> {
    inner: *mut pg_sys::ValidateIndexState,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> ValidateIndexStateHandle<'a> {
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::ValidateIndexState) -> Self {
        Self {
            inner: ptr,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn as_raw(&self) -> *mut pg_sys::ValidateIndexState {
        self.inner
    }
}

/// Safe wrapper for PostgreSQL TableScanDesc
#[derive(Debug)]
pub struct TableScanDescHandle<'a> {
    inner: pg_sys::TableScanDesc,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> TableScanDescHandle<'a> {
    #[inline]
    pub unsafe fn from_raw(ptr: pg_sys::TableScanDesc) -> Self {
        Self {
            inner: ptr,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn as_raw(&self) -> pg_sys::TableScanDesc {
        self.inner
    }
}

/// Safe wrapper for PostgreSQL IndexBuildCallback
#[derive(Debug)]
pub struct IndexBuildCallbackHandle<'a> {
    inner: pg_sys::IndexBuildCallback,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> IndexBuildCallbackHandle<'a> {
    #[inline]
    pub unsafe fn from_raw(callback: pg_sys::IndexBuildCallback) -> Self {
        Self {
            inner: callback,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn as_raw(&self) -> pg_sys::IndexBuildCallback {
        self.inner
    }
}

/// Safe wrapper for callback state pointer
#[derive(Debug)]
pub struct CallbackStateHandle<'a> {
    inner: *mut ::core::ffi::c_void,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> CallbackStateHandle<'a> {
    #[inline]
    pub unsafe fn from_raw(ptr: *mut ::core::ffi::c_void) -> Self {
        Self {
            inner: ptr,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn as_raw(&self) -> *mut ::core::ffi::c_void {
        self.inner
    }
}

/// Safe wrapper for PostgreSQL ScanKey
#[derive(Debug)]
pub struct ScanKeyHandle<'a> {
    inner: *mut pg_sys::ScanKeyData,
    nkeys: i32,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> ScanKeyHandle<'a> {
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::ScanKeyData, nkeys: i32) -> Self {
        Self {
            inner: ptr,
            nkeys,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn as_raw(&self) -> *mut pg_sys::ScanKeyData {
        self.inner
    }

    #[inline]
    pub fn nkeys(&self) -> i32 {
        self.nkeys
    }
}

/// Safe wrapper for PostgreSQL TBMIterateResult
#[derive(Debug)]
pub struct TBMIterateResultHandle<'a> {
    inner: *mut pg_sys::TBMIterateResult,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> TBMIterateResultHandle<'a> {
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::TBMIterateResult) -> Self {
        Self {
            inner: ptr,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn as_raw(&self) -> *mut pg_sys::TBMIterateResult {
        self.inner
    }
}

/// Safe wrapper for PostgreSQL SampleScanState
#[derive(Debug)]
pub struct SampleScanStateHandle<'a> {
    inner: *mut pg_sys::SampleScanState,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> SampleScanStateHandle<'a> {
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::SampleScanState) -> Self {
        Self {
            inner: ptr,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn as_raw(&self) -> *mut pg_sys::SampleScanState {
        self.inner
    }
}

/// Safe wrapper for PostgreSQL ReadStream
#[derive(Debug)]
pub struct ReadStreamHandle<'a> {
    inner: *mut pg_sys::ReadStream,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> ReadStreamHandle<'a> {
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::ReadStream) -> Self {
        Self {
            inner: ptr,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn as_raw(&self) -> *mut pg_sys::ReadStream {
        self.inner
    }
}

/// Safe wrapper for PostgreSQL ParallelTableScanDesc
#[derive(Debug)]
pub struct ParallelTableScanDescHandle<'a> {
    inner: pg_sys::ParallelTableScanDesc,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> ParallelTableScanDescHandle<'a> {
    #[inline]
    pub unsafe fn from_raw(ptr: pg_sys::ParallelTableScanDesc) -> Self {
        Self {
            inner: ptr,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn as_raw(&self) -> pg_sys::ParallelTableScanDesc {
        self.inner
    }
}

/// Safe wrapper for PostgreSQL ScanDirection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Backward,
    NoMovement,
}

impl ScanDirection {
    #[inline]
    pub unsafe fn from_raw(direction: pg_sys::ScanDirection::Type) -> Self {
        match direction {
            pg_sys::ScanDirection::ForwardScanDirection => ScanDirection::Forward,
            pg_sys::ScanDirection::BackwardScanDirection => ScanDirection::Backward,
            _ => ScanDirection::NoMovement,
        }
    }

    #[inline]
    pub fn to_raw(&self) -> pg_sys::ScanDirection::Type {
        match self {
            ScanDirection::Forward => pg_sys::ScanDirection::ForwardScanDirection,
            ScanDirection::Backward => pg_sys::ScanDirection::BackwardScanDirection,
            ScanDirection::NoMovement => {
                pg_sys::ScanDirection::NoMovementScanDirection
            }
        }
    }
}

/// Safe wrapper for PostgreSQL BulkInsertStateData
#[derive(Debug)]
pub struct BulkInsertStateHandle<'a> {
    inner: *mut pg_sys::BulkInsertStateData,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> BulkInsertStateHandle<'a> {
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::BulkInsertStateData) -> Self {
        Self {
            inner: ptr,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn as_raw(&self) -> *mut pg_sys::BulkInsertStateData {
        self.inner
    }
}

/// Safe wrapper for PostgreSQL TM_FailureData
#[derive(Debug, Clone, Copy, Default)]
#[allow(non_camel_case_types)]
pub struct TM_FailureData {
    pub ctid: ItemPointer,
    pub xmax: pg_sys::TransactionId,
    pub cmax: pg_sys::CommandId,
    pub traversed: bool,
}

impl TM_FailureData {
    #[inline]
    pub unsafe fn write_to_ptr(&self, ptr: *mut pg_sys::TM_FailureData) {
        unsafe {
            if !ptr.is_null() {
                (*ptr).ctid = self.ctid.to_pg_sys();
                (*ptr).xmax = self.xmax;
                (*ptr).cmax = self.cmax;
                (*ptr).traversed = self.traversed;
            }
        }
    }
}

/// RAII guard for table handle - automatically closes table when dropped.
///
/// This struct provides safe resource management for PostgreSQL table handles.
/// When the guard goes out of scope, it automatically closes the table with
/// the appropriate lock mode.
///
/// # Example
///
/// ```rust,no_run
/// use pg_lakebase_core::handles::TableGuard;
/// use pgrx::pg_sys;
///
/// let guard = TableGuard::open(oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE)?;
/// let handle = guard.as_handle();
/// // ... use handle ...
/// // Table is automatically closed when guard goes out of scope
/// ```
#[derive(Debug)]
pub struct TableGuard<'a> {
    rel: pg_sys::Relation,
    lock_mode: pg_sys::LOCKMODE,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> TableGuard<'a> {
    /// Opens a table with the specified lock mode.
    ///
    /// # Arguments
    /// * `oid` - The OID of the table to open
    /// * `lock_mode` - The lock mode to use when opening the table
    ///
    /// # Returns
    /// A `TableGuard` that will automatically close the table when dropped
    pub fn open(
        oid: pg_sys::Oid,
        lock_mode: pg_sys::LOCKMODE,
    ) -> Result<Self, PgWrapperError> {
        let rel = PgWrapper::table_open(oid, lock_mode)?;
        Ok(Self {
            rel,
            lock_mode,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Get a `RelationHandle` from this guard.
    ///
    /// The returned handle borrows from this guard, ensuring the table
    /// remains open while the handle is in use.
    #[inline]
    pub fn as_handle(&self) -> RelationHandle<'_> {
        unsafe { RelationHandle::from_raw(self.rel) }
    }

    /// Get the raw relation pointer.
    #[inline]
    pub fn as_raw(&self) -> pg_sys::Relation {
        self.rel
    }
}

impl Drop for TableGuard<'_> {
    fn drop(&mut self) {
        let _ = PgWrapper::table_close(self.rel, self.lock_mode);
    }
}

/// RAII guard for system table scan - automatically ends scan when dropped.
#[derive(Debug)]
pub struct SysScanGuard<'a> {
    scan: pg_sys::SysScanDesc,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> SysScanGuard<'a> {
    /// Create a new guard from a raw scan descriptor.
    /// The guard takes ownership of the scan descriptor and will close it on drop.
    pub unsafe fn from_raw(scan: pg_sys::SysScanDesc) -> Self {
        Self {
            scan,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Get the raw scan descriptor.
    #[inline]
    pub fn as_raw(&self) -> pg_sys::SysScanDesc {
        self.scan
    }
}

impl Drop for SysScanGuard<'_> {
    fn drop(&mut self) {
        let _ = PgWrapper::systable_endscan(self.scan);
    }
}
