//! Backend-local owner of the cross-DSO table-maintenance provider directory.

use std::cell::RefCell;
use std::ffi::{CStr, CString, c_void};

use pg_lakebase_core::table_maintenance::abi::{
    MAINTENANCE_PROVIDER_VERSION, MaintenanceProviderV2,
    REGISTER_DUPLICATE_ACCESS_METHOD, REGISTER_DUPLICATE_NAME,
    REGISTER_INVALID_DESCRIPTOR, REGISTER_OK, RUNTIME_API_VERSION, RuntimeApiV1,
    RuntimeMaintenanceConfigV1, provider_access_method_name, provider_name,
    rendezvous_slot,
};
use pgrx::pg_sys;

thread_local! {
    static PROVIDERS: RefCell<ProviderDirectory> =
        const { RefCell::new(ProviderDirectory::new()) };
}

struct StoredProvider {
    descriptor: Box<MaintenanceProviderV2>,
    _name: CString,
    _access_method_name: CString,
}

impl StoredProvider {
    fn new(
        descriptor: &MaintenanceProviderV2,
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

struct ProviderDirectory {
    providers: Vec<StoredProvider>,
}

impl ProviderDirectory {
    const fn new() -> Self {
        Self {
            providers: Vec::new(),
        }
    }

    fn register(
        &mut self,
        descriptor: &MaintenanceProviderV2,
        name: &CStr,
        access_method_name: &CStr,
    ) -> u32 {
        for existing in &self.providers {
            let existing_descriptor = existing.descriptor.as_ref();
            let existing_name =
                provider_name(existing_descriptor).expect("validated provider name");
            let existing_access_method_name =
                provider_access_method_name(existing_descriptor)
                    .expect("validated access-method name");
            if existing_name == name {
                let same_descriptor = existing_access_method_name
                    == access_method_name
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
                    REGISTER_OK
                } else {
                    REGISTER_DUPLICATE_NAME
                };
            }
            if existing_access_method_name == access_method_name {
                return REGISTER_DUPLICATE_ACCESS_METHOD;
            }
        }
        self.providers.push(StoredProvider::new(
            descriptor,
            name,
            access_method_name,
        ));
        REGISTER_OK
    }

    fn len(&self) -> usize {
        self.providers.len()
    }

    fn descriptor(&self, index: usize) -> *const MaintenanceProviderV2 {
        self.providers[index].descriptor.as_ref()
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn register_provider(
    descriptor: *const MaintenanceProviderV2,
) -> u32 {
    let Some(descriptor) = (unsafe { descriptor.as_ref() }) else {
        return REGISTER_INVALID_DESCRIPTOR;
    };
    let expected_size = u32::try_from(std::mem::size_of::<MaintenanceProviderV2>())
        .expect("maintenance descriptor size exceeds u32");
    if descriptor.abi_version != MAINTENANCE_PROVIDER_VERSION
        || descriptor.struct_size < expected_size
        || descriptor.name.is_null()
        || descriptor.access_method_name.is_null()
    {
        return REGISTER_INVALID_DESCRIPTOR;
    }
    let name = unsafe { CStr::from_ptr(descriptor.name) };
    let access_method_name =
        unsafe { CStr::from_ptr(descriptor.access_method_name) };
    if name.is_empty()
        || access_method_name.is_empty()
        || access_method_name.to_bytes().len()
            >= usize::try_from(pg_sys::NAMEDATALEN)
                .expect("PostgreSQL NAMEDATALEN fits usize")
    {
        return REGISTER_INVALID_DESCRIPTOR;
    }
    PROVIDERS.with_borrow_mut(|providers| {
        providers.register(descriptor, name, access_method_name)
    })
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn has_providers() -> u8 {
    // Registration happens during shared preload, before database-local access
    // method OIDs necessarily exist. Resolve the callbacks only when routing a
    // command in a connected database, and never invoke provider code while a
    // RefCell borrow is live.
    let provider_count = PROVIDERS.with_borrow(ProviderDirectory::len);
    for index in 0..provider_count {
        let descriptor =
            PROVIDERS.with_borrow(|providers| providers.descriptor(index));
        let descriptor = unsafe { &*descriptor };
        if unsafe { (descriptor.access_method_oid)() } != pg_sys::InvalidOid {
            return 1;
        }
    }
    0
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn provider_for_am(
    access_method_oid: pg_sys::Oid,
) -> *const MaintenanceProviderV2 {
    // AM OIDs are database-local and do not exist yet during shared-preload
    // registration. Copy one stable descriptor pointer at a time, release the
    // RefCell borrow, and only then invoke catalog-reading provider callbacks.
    let provider_count = PROVIDERS.with_borrow(ProviderDirectory::len);
    let mut matched: *const MaintenanceProviderV2 = std::ptr::null();
    for index in 0..provider_count {
        let descriptor =
            PROVIDERS.with_borrow(|providers| providers.descriptor(index));
        let descriptor = unsafe { &*descriptor };
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

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn customscan_mode() -> u32 {
    crate::gucs::customscan_mode_code()
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn maintenance_config(
    config: *mut RuntimeMaintenanceConfigV1,
) {
    let Some(config) = (unsafe { config.as_mut() }) else {
        panic!("runtime maintenance config output pointer is null");
    };
    *config = crate::gucs::maintenance_config();
}

static RUNTIME_API: RuntimeApiV1 = RuntimeApiV1 {
    abi_version: RUNTIME_API_VERSION,
    struct_size: std::mem::size_of::<RuntimeApiV1>() as u32,
    register_provider,
    has_providers,
    provider_for_am,
    customscan_mode,
    maintenance_config,
};

pub(crate) fn init() {
    let slot = unsafe { rendezvous_slot() };
    assert!(!slot.is_null(), "PostgreSQL returned a null rendezvous slot");
    let published = unsafe { *slot };
    if !published.is_null()
        && published
            != (&RUNTIME_API as *const RuntimeApiV1)
                .cast_mut()
                .cast::<c_void>()
    {
        panic!("a different pg_lakebase runtime API is already published");
    }
    unsafe {
        *slot = (&RUNTIME_API as *const RuntimeApiV1).cast_mut().cast::<c_void>();
    }
    pg_lakebase_core::table_maintenance::install_runtime_router();
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_lakebase_core::table_maintenance::abi::{
        MaintenanceReportV1, MaintenanceRequestV1, MaintenanceStatsV1,
    };

    unsafe extern "C-unwind" fn access_method_oid() -> pg_sys::Oid {
        pg_sys::InvalidOid
    }

    unsafe extern "C-unwind" fn execute(
        _request: *const MaintenanceRequestV1,
        _report: *mut MaintenanceReportV1,
    ) {
    }

    unsafe extern "C-unwind" fn inspect(
        _relation: pg_sys::Relation,
        _stats: *mut MaintenanceStatsV1,
    ) {
    }

    fn descriptor(
        name: &'static CStr,
        access_method_name: &'static CStr,
    ) -> MaintenanceProviderV2 {
        MaintenanceProviderV2 {
            abi_version: MAINTENANCE_PROVIDER_VERSION,
            struct_size: std::mem::size_of::<MaintenanceProviderV2>() as u32,
            name: name.as_ptr(),
            access_method_name: access_method_name.as_ptr(),
            access_method_oid,
            execute,
            inspect,
        }
    }

    #[test]
    fn rejects_distinct_providers_for_the_same_access_method() {
        let mut directory = ProviderDirectory::new();
        let iceberg = descriptor(c"iceberg", c"iceberg");
        let competing = descriptor(c"other", c"iceberg");

        assert_eq!(
            directory.register(&iceberg, c"iceberg", c"iceberg"),
            REGISTER_OK
        );
        assert_eq!(
            directory.register(&competing, c"other", c"iceberg"),
            REGISTER_DUPLICATE_ACCESS_METHOD
        );
    }

    #[test]
    fn exact_registration_is_idempotent() {
        let mut directory = ProviderDirectory::new();
        let iceberg = descriptor(c"iceberg", c"iceberg");

        assert_eq!(
            directory.register(&iceberg, c"iceberg", c"iceberg"),
            REGISTER_OK
        );
        assert_eq!(
            directory.register(&iceberg, c"iceberg", c"iceberg"),
            REGISTER_OK
        );
        assert_eq!(directory.len(), 1);
    }

    #[test]
    fn rejects_one_provider_name_claiming_two_access_methods() {
        let mut directory = ProviderDirectory::new();
        let iceberg = descriptor(c"shared", c"iceberg");
        let delta = descriptor(c"shared", c"delta");

        assert_eq!(
            directory.register(&iceberg, c"shared", c"iceberg"),
            REGISTER_OK
        );
        assert_eq!(
            directory.register(&delta, c"shared", c"delta"),
            REGISTER_DUPLICATE_NAME
        );
    }
}
