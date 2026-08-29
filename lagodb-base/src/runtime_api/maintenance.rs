//! Runtime-owned maintenance-provider directory and callbacks.

use std::cell::RefCell;
use std::ffi::{CStr, CString};

use lagodb_core::runtime_api::{
    AbiHeader, MaintenanceProvider, PROVIDER_CAPABILITIES_KNOWN,
    REGISTER_DUPLICATE_ACCESS_METHOD, REGISTER_DUPLICATE_NAME,
    RuntimeMaintenanceConfig, provider_access_method_name, provider_name,
};
use pgrx::{pg_guard, pg_sys};

use crate::gucs;

thread_local! {
    static MAINTENANCE_PROVIDERS: RefCell<MaintenanceProviderDirectory> =
        const { RefCell::new(MaintenanceProviderDirectory::new()) };
}

struct StoredMaintenanceProvider {
    descriptor: Box<MaintenanceProvider>,
    _name: CString,
    _access_method_name: CString,
}

impl StoredMaintenanceProvider {
    fn new(
        descriptor: &MaintenanceProvider,
        name: &CStr,
        access_method_name: &CStr,
    ) -> Self {
        let name = name.to_owned();
        let access_method_name = access_method_name.to_owned();
        let mut descriptor = Box::new(*descriptor);
        descriptor.name = name.as_ptr();
        descriptor.access_method_name = access_method_name.as_ptr();
        Self {
            descriptor,
            _name: name,
            _access_method_name: access_method_name,
        }
    }
}

struct MaintenanceProviderDirectory {
    providers: Vec<StoredMaintenanceProvider>,
}

impl MaintenanceProviderDirectory {
    const fn new() -> Self {
        Self {
            providers: Vec::new(),
        }
    }

    fn prepare(
        &mut self,
        descriptor: &MaintenanceProvider,
        name: &CStr,
        access_method_name: &CStr,
    ) -> Result<Option<StoredMaintenanceProvider>, u32> {
        for existing in &self.providers {
            let existing_descriptor = existing.descriptor.as_ref();
            // SAFETY: stored descriptors were validated on registration and
            // point at the `CString`s owned by `StoredMaintenanceProvider`.
            let existing_name = unsafe { provider_name(existing_descriptor) }
                .expect("validated provider name");
            // SAFETY: same invariant as `existing_name` above.
            let existing_access_method_name =
                unsafe { provider_access_method_name(existing_descriptor) }
                    .expect("validated access-method name");
            if existing_name == name {
                let same_descriptor = existing_access_method_name
                    == access_method_name
                    && existing_descriptor.capability_flags
                        == descriptor.capability_flags
                    && std::ptr::fn_addr_eq(
                        existing_descriptor.access_method_oid,
                        descriptor.access_method_oid,
                    )
                    && std::ptr::fn_addr_eq(
                        existing_descriptor.execute,
                        descriptor.execute,
                    )
                    && std::ptr::fn_addr_eq(
                        existing_descriptor.inspect,
                        descriptor.inspect,
                    );
                return if same_descriptor {
                    Ok(None)
                } else {
                    Err(REGISTER_DUPLICATE_NAME)
                };
            }
            if existing_access_method_name == access_method_name {
                return Err(REGISTER_DUPLICATE_ACCESS_METHOD);
            }
        }
        // Reserve before any runtime directory is changed. The later commit is
        // therefore allocation-free and cannot leave provider and hook
        // directories partially published.
        self.providers.reserve(1);
        Ok(Some(StoredMaintenanceProvider::new(
            descriptor,
            name,
            access_method_name,
        )))
    }

    fn commit(&mut self, provider: Option<StoredMaintenanceProvider>) {
        if let Some(provider) = provider {
            debug_assert!(self.providers.len() < self.providers.capacity());
            self.providers.push(provider);
        }
    }

    fn len(&self) -> usize {
        self.providers.len()
    }

    fn descriptor(&self, index: usize) -> *const MaintenanceProvider {
        self.providers[index].descriptor.as_ref()
    }
}

pub(super) struct ValidatedProvider<'a> {
    descriptor: &'a MaintenanceProvider,
    name: &'a CStr,
    access_method_name: &'a CStr,
}

impl<'a> ValidatedProvider<'a> {
    /// Validate one exact-build maintenance-provider descriptor.
    ///
    /// # Safety
    ///
    /// `descriptor` must satisfy the trusted internal ABI pointer contract
    /// documented by `lagodb_core::runtime_api`.
    pub(super) unsafe fn from_raw(
        descriptor: *const MaintenanceProvider,
    ) -> Option<Self> {
        // SAFETY: callers uphold the module's trusted internal-ABI pointer and
        // alignment contract; `as_ref` handles the permitted null input.
        let header = unsafe { descriptor.cast::<AbiHeader>().as_ref() }?;
        let expected_size = u32::try_from(std::mem::size_of::<MaintenanceProvider>())
            .expect("maintenance descriptor size exceeds u32");
        if header.struct_size != expected_size {
            return None;
        }
        // SAFETY: the validated header states that the caller supplied the full
        // exact descriptor layout expected by this build.
        let descriptor = unsafe { &*descriptor };
        if descriptor.name.is_null()
            || descriptor.access_method_name.is_null()
            || descriptor.capability_flags & !PROVIDER_CAPABILITIES_KNOWN != 0
        {
            return None;
        }
        // SAFETY: the trusted ABI requires each validated non-null pointer to
        // reference a live NUL-terminated string for this synchronous call.
        let name = unsafe { CStr::from_ptr(descriptor.name) };
        // SAFETY: the same trusted string-pointer contract applies here.
        let access_method_name =
            unsafe { CStr::from_ptr(descriptor.access_method_name) };
        if name.is_empty()
            || access_method_name.is_empty()
            || access_method_name.to_bytes().len()
                >= usize::try_from(pg_sys::NAMEDATALEN)
                    .expect("PostgreSQL NAMEDATALEN fits usize")
        {
            return None;
        }
        Some(Self {
            descriptor,
            name,
            access_method_name,
        })
    }
}

pub(super) struct PreparedRegistration {
    provider: Option<StoredMaintenanceProvider>,
}

impl PreparedRegistration {
    pub(super) fn prepare(
        provider: Option<ValidatedProvider<'_>>,
    ) -> Result<Self, u32> {
        let provider = match provider {
            Some(provider) => MAINTENANCE_PROVIDERS.with_borrow_mut(|providers| {
                providers.prepare(
                    provider.descriptor,
                    provider.name,
                    provider.access_method_name,
                )
            })?,
            None => None,
        };
        Ok(Self { provider })
    }

    pub(super) fn commit(self) {
        MAINTENANCE_PROVIDERS
            .with_borrow_mut(|providers| providers.commit(self.provider));
    }
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn has_providers() -> u8 {
    // Registration happens during shared preload, before database-local access
    // method OIDs necessarily exist. Resolve the callbacks only when routing a
    // command in a connected database, and never invoke provider code while a
    // RefCell borrow is live.
    let provider_count =
        MAINTENANCE_PROVIDERS.with_borrow(MaintenanceProviderDirectory::len);
    for index in 0..provider_count {
        let descriptor = MAINTENANCE_PROVIDERS
            .with_borrow(|providers| providers.descriptor(index));
        // SAFETY: directory entries own validated, backend-lifetime descriptor
        // allocations, and the RefCell borrow was released before this access.
        let descriptor = unsafe { &*descriptor };
        // SAFETY: the callback was validated as part of the exact-build
        // descriptor and executes after the directory borrow is released.
        if unsafe { (descriptor.access_method_oid)() } != pg_sys::InvalidOid {
            return 1;
        }
    }
    0
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn provider_for_am(
    access_method_oid: pg_sys::Oid,
) -> *const MaintenanceProvider {
    // AM OIDs are database-local and do not exist yet during shared-preload
    // registration. Copy one stable descriptor pointer at a time, release the
    // RefCell borrow, and only then invoke catalog-reading provider callbacks.
    let provider_count =
        MAINTENANCE_PROVIDERS.with_borrow(MaintenanceProviderDirectory::len);
    let mut matched: *const MaintenanceProvider = std::ptr::null();
    for index in 0..provider_count {
        let descriptor = MAINTENANCE_PROVIDERS
            .with_borrow(|providers| providers.descriptor(index));
        // SAFETY: directory entries own validated, backend-lifetime descriptor
        // allocations, and the RefCell borrow was released before this access.
        let descriptor = unsafe { &*descriptor };
        // SAFETY: the callback was validated as part of the exact-build
        // descriptor and executes after the directory borrow is released.
        if unsafe { (descriptor.access_method_oid)() } != access_method_oid {
            continue;
        }
        if !matched.is_null() {
            panic!(
                "multiple maintenance providers resolved to access method OID {access_method_oid}"
            );
        }
        matched = descriptor;
    }
    matched
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn maintenance_config(
    config: *mut RuntimeMaintenanceConfig,
) {
    // SAFETY: `as_mut` validates the permitted null input before the output is
    // initialized; the runtime ABI gives exclusive access for this call.
    let Some(config) = (unsafe { config.as_mut() }) else {
        panic!("runtime maintenance config output pointer is null");
    };
    *config = gucs::maintenance_config();
}

#[cfg(test)]
mod tests {
    use lagodb_core::runtime_api::{
        MaintenanceReport, MaintenanceRequest, MaintenanceStats,
    };

    use super::*;

    unsafe extern "C-unwind" fn access_method_oid() -> pg_sys::Oid {
        pg_sys::InvalidOid
    }

    unsafe extern "C-unwind" fn execute(
        _request: *const MaintenanceRequest,
        _report: *mut MaintenanceReport,
    ) {
    }

    unsafe extern "C-unwind" fn inspect(
        _relation: pg_sys::Relation,
        _stats: *mut MaintenanceStats,
    ) {
    }

    fn descriptor(
        name: &'static CStr,
        access_method_name: &'static CStr,
    ) -> MaintenanceProvider {
        MaintenanceProvider {
            struct_size: std::mem::size_of::<MaintenanceProvider>() as u32,
            name: name.as_ptr(),
            access_method_name: access_method_name.as_ptr(),
            capability_flags: 0,
            access_method_oid,
            execute,
            inspect,
        }
    }

    #[test]
    fn rejects_distinct_providers_for_the_same_access_method() {
        let mut directory = MaintenanceProviderDirectory::new();
        let iceberg = descriptor(c"iceberg", c"iceberg");
        let competing = descriptor(c"other", c"iceberg");

        let prepared = directory
            .prepare(&iceberg, c"iceberg", c"iceberg")
            .expect("first provider is valid");
        directory.commit(prepared);
        assert_eq!(
            directory.prepare(&competing, c"other", c"iceberg").err(),
            Some(REGISTER_DUPLICATE_ACCESS_METHOD)
        );
    }

    #[test]
    fn provider_validation_requires_exact_size() {
        let mut provider = descriptor(c"iceberg", c"iceberg");
        assert!(unsafe { ValidatedProvider::from_raw(&provider) }.is_some());
        provider.struct_size = std::mem::size_of::<MaintenanceProvider>() as u32 + 1;
        assert!(unsafe { ValidatedProvider::from_raw(&provider) }.is_none());
    }

    #[test]
    fn exact_registration_is_idempotent() {
        let mut directory = MaintenanceProviderDirectory::new();
        let iceberg = descriptor(c"iceberg", c"iceberg");

        let prepared = directory
            .prepare(&iceberg, c"iceberg", c"iceberg")
            .expect("first provider is valid");
        directory.commit(prepared);
        let prepared = directory
            .prepare(&iceberg, c"iceberg", c"iceberg")
            .expect("identical registration is valid");
        assert!(prepared.is_none());
        directory.commit(prepared);
        assert_eq!(directory.len(), 1);
    }

    #[test]
    fn rejects_one_provider_name_claiming_two_access_methods() {
        let mut directory = MaintenanceProviderDirectory::new();
        let iceberg = descriptor(c"shared", c"iceberg");
        let delta = descriptor(c"shared", c"delta");

        let prepared = directory
            .prepare(&iceberg, c"shared", c"iceberg")
            .expect("first provider is valid");
        directory.commit(prepared);
        assert_eq!(
            directory.prepare(&delta, c"shared", c"delta").err(),
            Some(REGISTER_DUPLICATE_NAME)
        );
    }
}
