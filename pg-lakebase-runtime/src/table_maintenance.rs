//! Backend-local owner of the cross-DSO table-maintenance provider directory.

use std::cell::RefCell;
use std::ffi::{CStr, CString, c_void};

use pg_lakebase_core::table_maintenance::abi::{
    MAINTENANCE_PROVIDER_VERSION, MaintenanceProviderV1, REGISTER_DUPLICATE_NAME,
    REGISTER_INVALID_DESCRIPTOR, REGISTER_OK,
    RUNTIME_API_VERSION, RuntimeApiV1, RuntimeMaintenanceConfigV1, provider_name,
    rendezvous_slot,
};
use pgrx::pg_sys;

thread_local! {
    static PROVIDERS: RefCell<Vec<StoredProvider>> = const { RefCell::new(Vec::new()) };
}

struct StoredProvider {
    descriptor: Box<MaintenanceProviderV1>,
    _name: CString,
}

impl StoredProvider {
    fn new(descriptor: &MaintenanceProviderV1, name: &CStr) -> Self {
        let name = name.to_owned();
        let mut descriptor = Box::new(*descriptor);
        descriptor.name = name.as_ptr();
        Self {
            descriptor,
            _name: name,
        }
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn register_provider(
    descriptor: *const MaintenanceProviderV1,
) -> u32 {
    let Some(descriptor) = (unsafe { descriptor.as_ref() }) else {
        return REGISTER_INVALID_DESCRIPTOR;
    };
    let expected_size = u32::try_from(std::mem::size_of::<MaintenanceProviderV1>())
        .expect("maintenance descriptor size exceeds u32");
    if descriptor.abi_version != MAINTENANCE_PROVIDER_VERSION
        || descriptor.struct_size < expected_size
        || descriptor.name.is_null()
    {
        return REGISTER_INVALID_DESCRIPTOR;
    }
    let name = unsafe { CStr::from_ptr(descriptor.name) };
    PROVIDERS.with_borrow_mut(|providers| {
        for existing in providers.iter() {
            let existing_descriptor = existing.descriptor.as_ref();
            let existing_name =
                provider_name(existing_descriptor).expect("validated provider name");
            if existing_name == name {
                let same_descriptor = std::ptr::fn_addr_eq(
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
        }
        providers.push(StoredProvider::new(descriptor, name));
        REGISTER_OK
    })
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn has_providers() -> u8 {
    // Registration happens during shared preload, before database-local access
    // method OIDs necessarily exist. Resolve the callbacks only when routing a
    // command in a connected database, and never invoke provider code while a
    // RefCell borrow is live.
    let provider_count = PROVIDERS.with_borrow(Vec::len);
    for index in 0..provider_count {
        let descriptor = PROVIDERS.with_borrow(|providers| {
            providers[index].descriptor.as_ref() as *const MaintenanceProviderV1
        });
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
) -> *const MaintenanceProviderV1 {
    // AM OIDs are database-local and do not exist yet during shared-preload
    // registration. Copy one stable descriptor pointer at a time, release the
    // RefCell borrow, and only then invoke catalog-reading provider callbacks.
    let provider_count = PROVIDERS.with_borrow(Vec::len);
    let mut matched: *const MaintenanceProviderV1 = std::ptr::null();
    for index in 0..provider_count {
        let descriptor = PROVIDERS.with_borrow(|providers| {
            providers[index].descriptor.as_ref() as *const MaintenanceProviderV1
        });
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
