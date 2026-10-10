//! Exact-build COPY bindings. Handles are allocated and released by the AM DSO.
//! Callback errors are data; only the executor-facing bridge reports PG ERROR.

use std::ffi::c_void;

use pgrx::pg_sys;

use super::CallbackErrorReport;

/// Backend-lifetime callbacks for one TableProvider's explicit COPY writes.
/// `bind` captures a relation borrow used only until executor teardown. The
/// driver retains handles through input shutdown; complete/release must not
/// access that borrow. All errors are returned in the supplied report, including
/// panics and PostgreSQL ERRORs. No Rust-owned value crosses this ABI.
#[repr(C)]
pub struct TableCopyCallbacks {
    pub bind: unsafe extern "C-unwind" fn(
        pg_sys::Relation,
        pg_sys::CommandId,
        *mut *mut c_void,
        *mut CallbackErrorReport,
    ) -> u32,
    pub insert: unsafe extern "C-unwind" fn(
        *mut c_void,
        *mut pg_sys::TupleTableSlot,
        i32,
        *mut CallbackErrorReport,
    ) -> u32,
    pub insert_batch: unsafe extern "C-unwind" fn(
        *mut c_void,
        *mut *mut pg_sys::TupleTableSlot,
        i32,
        i32,
        *mut CallbackErrorReport,
    ) -> u32,
    pub finish_bulk: unsafe extern "C-unwind" fn(
        *mut c_void,
        i32,
        *mut CallbackErrorReport,
    ) -> u32,
    pub publish:
        unsafe extern "C-unwind" fn(*mut c_void, *mut CallbackErrorReport) -> u32,
    pub complete:
        unsafe extern "C-unwind" fn(*mut c_void, *mut CallbackErrorReport) -> u32,
    /// Consumes the handle even on error. Uncompleted bindings abort without
    /// dereferencing PostgreSQL objects already reclaimed by ResourceOwner.
    pub release:
        unsafe extern "C-unwind" fn(*mut c_void, *mut CallbackErrorReport) -> u32,
}
