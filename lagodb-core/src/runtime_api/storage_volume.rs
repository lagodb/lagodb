//! Call-scoped ABI for the runtime-owned tablespace binding snapshot.

use std::ffi::c_char;
use std::ptr;

use pgrx::pg_sys;

use super::{CallbackErrorReport, RuntimeClient};

/// One Bound volume's routing metadata; credentials never cross this ABI.
/// Strings are valid UTF-8, allocated in the caller's PostgreSQL memory context.
#[repr(C)]
pub struct TablespaceBindingEntry {
    pub tablespace_oid: pg_sys::Oid,
    pub volume_id: u64,
    pub object_namespace: *const c_char,
    pub effective_base_uri: *const c_char,
}

/// Complete Bound binding set from one validated configuration snapshot.
///
/// The array and its strings belong to the caller's PostgreSQL memory context.
/// On CALLBACK_OK, entries have unique tablespace OIDs and validated routes.
/// A zero count permits a null pointer and means every user tablespace is local,
/// including before initial config bootstrap. Retiring volumes are excluded
/// from this lookup snapshot, not from the storage worker's registry.
#[repr(C)]
pub struct TablespaceBindingsOutput {
    pub entries: *const TablespaceBindingEntry,
    pub count: usize,
    pub error: CallbackErrorReport,
}

impl Default for TablespaceBindingsOutput {
    fn default() -> Self {
        Self {
            entries: ptr::null(),
            count: 0,
            error: CallbackErrorReport::default(),
        }
    }
}

pub type LoadTablespaceBindingsCallback =
    unsafe extern "C-unwind" fn(output: *mut TablespaceBindingsOutput) -> u32;

impl RuntimeClient {
    pub(crate) fn load_tablespace_bindings(
        self,
        output: &mut TablespaceBindingsOutput,
    ) -> u32 {
        // SAFETY: synchronous call with a live output reference.
        unsafe { (self.api.load_tablespace_bindings)(output) }
    }
}
