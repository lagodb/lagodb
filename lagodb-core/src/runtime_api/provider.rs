//! Table-provider descriptor published by an access-method DSO.

use std::ffi::{CStr, c_char};
use std::ptr;

use pgrx::pg_sys;

use super::{
    ExecuteMaintenanceCallback, InspectMaintenanceCallback, TableCopyCallbacks,
};

pub type AccessMethodOidCallback = unsafe extern "C-unwind" fn() -> pg_sys::Oid;

/// Exact-build table-provider descriptor published by an AM DSO.
///
/// This is the single runtime identity and operation record for one table AM.
/// Operation-specific callbacks remain grouped by name rather than being
/// exposed through parallel provider registries.
///
/// Fields are private: the constructor takes only `'static` borrows, and
/// PostgreSQL never unloads a library after `_PG_init`, so every pointer stays
/// valid for the backend lifetime in whichever DSO holds a copy.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableProvider {
    name: *const c_char,
    /// Stable catalog name of the table access method owned by this provider.
    ///
    /// Runtime registration uses this identity because access-method OIDs are
    /// database-local and may not be resolvable while extensions are preloaded.
    access_method_name: *const c_char,
    /// `None` for AMs that use their native TableAM insertion callbacks.
    copy: Option<&'static TableCopyCallbacks>,
    access_method_oid: AccessMethodOidCallback,
    execute_maintenance: ExecuteMaintenanceCallback,
    inspect_maintenance: InspectMaintenanceCallback,
}

impl TableProvider {
    #[must_use]
    pub const fn new(
        name: &'static CStr,
        access_method_name: &'static CStr,
        copy: Option<&'static TableCopyCallbacks>,
        access_method_oid: AccessMethodOidCallback,
        execute_maintenance: ExecuteMaintenanceCallback,
        inspect_maintenance: InspectMaintenanceCallback,
    ) -> Self {
        Self {
            name: name.as_ptr(),
            access_method_name: access_method_name.as_ptr(),
            copy,
            access_method_oid,
            execute_maintenance,
            inspect_maintenance,
        }
    }

    #[must_use]
    pub fn name(&self) -> &'static CStr {
        // SAFETY: `new` stores only `&'static CStr` pointers.
        unsafe { CStr::from_ptr(self.name) }
    }

    #[must_use]
    pub fn access_method_name(&self) -> &'static CStr {
        // SAFETY: `new` stores only `&'static CStr` pointers.
        unsafe { CStr::from_ptr(self.access_method_name) }
    }

    #[must_use]
    pub const fn copy(&self) -> Option<&'static TableCopyCallbacks> {
        self.copy
    }

    #[must_use]
    pub const fn access_method_oid(&self) -> AccessMethodOidCallback {
        self.access_method_oid
    }

    #[must_use]
    pub const fn execute_maintenance(&self) -> ExecuteMaintenanceCallback {
        self.execute_maintenance
    }

    #[must_use]
    pub const fn inspect_maintenance(&self) -> InspectMaintenanceCallback {
        self.inspect_maintenance
    }

    /// Whether `other` publishes the same identity and callbacks, as when one
    /// DSO registers the same provider again.
    #[must_use]
    pub fn is_same_provider(&self, other: &Self) -> bool {
        self.name() == other.name()
            && self.access_method_name() == other.access_method_name()
            && self.copy.map(ptr::from_ref) == other.copy.map(ptr::from_ref)
            && ptr::fn_addr_eq(self.access_method_oid, other.access_method_oid)
            && ptr::fn_addr_eq(self.execute_maintenance, other.execute_maintenance)
            && ptr::fn_addr_eq(self.inspect_maintenance, other.inspect_maintenance)
    }
}
