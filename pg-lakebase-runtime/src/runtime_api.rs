//! Publisher of the unified runtime API and owner of the provider directory.

use std::cell::RefCell;
use std::ffi::{CStr, CString, c_char, c_void};

use pg_lakebase_core::runtime_api::{
    AM_REGISTRATION_VERSION, AbiHeader, AmRegistrationV1,
    MAINTENANCE_PROVIDER_VERSION, MaintenanceProviderV1, PROVIDER_CAPABILITIES_KNOWN,
    REGISTER_DUPLICATE_ACCESS_METHOD, REGISTER_DUPLICATE_NAME,
    REGISTER_INVALID_DESCRIPTOR, REGISTER_OK, RUNTIME_API_VERSION, RuntimeApiV1,
    RuntimeMaintenanceConfigV1, STAGE_WORKER_WAKEUP_EXTENSION_NOT_FOUND,
    STAGE_WORKER_WAKEUP_INVALID_REQUEST, STAGE_WORKER_WAKEUP_OK,
    STAGE_WORKER_WAKEUP_RUNTIME_NOT_PRELOADED,
    provider_access_method_name, provider_name, rendezvous_slot,
};
use pgrx::pg_sys;

thread_local! {
    static PROVIDERS: RefCell<ProviderDirectory> =
        const { RefCell::new(ProviderDirectory::new()) };
}

struct StoredProvider {
    descriptor: Box<MaintenanceProviderV1>,
    _name: CString,
    _access_method_name: CString,
}

impl StoredProvider {
    fn new(
        descriptor: &MaintenanceProviderV1,
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

struct PreparedProviderRegistration {
    provider: Option<StoredProvider>,
}

impl ProviderDirectory {
    const fn new() -> Self {
        Self {
            providers: Vec::new(),
        }
    }

    fn prepare(
        &mut self,
        descriptor: &MaintenanceProviderV1,
        name: &CStr,
        access_method_name: &CStr,
    ) -> Result<PreparedProviderRegistration, u32> {
        for existing in &self.providers {
            let existing_descriptor = existing.descriptor.as_ref();
            // SAFETY: stored descriptors were validated on registration and
            // point at the `CString`s owned by `StoredProvider`.
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
                    Ok(PreparedProviderRegistration { provider: None })
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
        Ok(PreparedProviderRegistration {
            provider: Some(StoredProvider::new(descriptor, name, access_method_name)),
        })
    }

    fn commit(&mut self, prepared: PreparedProviderRegistration) {
        if let Some(provider) = prepared.provider {
            debug_assert!(self.providers.len() < self.providers.capacity());
            self.providers.push(provider);
        }
    }

    #[cfg(test)]
    fn register(
        &mut self,
        descriptor: &MaintenanceProviderV1,
        name: &CStr,
        access_method_name: &CStr,
    ) -> u32 {
        match self.prepare(descriptor, name, access_method_name) {
            Ok(prepared) => {
                self.commit(prepared);
                REGISTER_OK
            }
            Err(status) => status,
        }
    }

    fn len(&self) -> usize {
        self.providers.len()
    }

    fn descriptor(&self, index: usize) -> *const MaintenanceProviderV1 {
        self.providers[index].descriptor.as_ref()
    }
}

struct ValidatedProvider<'a> {
    descriptor: &'a MaintenanceProviderV1,
    name: &'a CStr,
    access_method_name: &'a CStr,
}

unsafe fn validate_provider<'a>(
    descriptor: *const MaintenanceProviderV1,
) -> Option<ValidatedProvider<'a>> {
    // SAFETY: callers uphold the module's trusted internal-ABI pointer and
    // alignment contract; `as_ref` handles the permitted null input.
    let header = unsafe { descriptor.cast::<AbiHeader>().as_ref() }?;
    let expected_size = u32::try_from(std::mem::size_of::<MaintenanceProviderV1>())
        .expect("maintenance descriptor size exceeds u32");
    if header.abi_version != MAINTENANCE_PROVIDER_VERSION
        || header.struct_size < expected_size
    {
        return None;
    }
    // SAFETY: the validated header states that the caller supplied the full
    // descriptor layout for this ABI version.
    let descriptor = unsafe { &*descriptor };
    if descriptor.name.is_null()
        || descriptor.access_method_name.is_null()
        || descriptor.capability_flags & !PROVIDER_CAPABILITIES_KNOWN != 0
    {
        return None;
    }
    let name = unsafe { CStr::from_ptr(descriptor.name) };
    let access_method_name = unsafe { CStr::from_ptr(descriptor.access_method_name) };
    if name.is_empty()
        || access_method_name.is_empty()
        || access_method_name.to_bytes().len()
            >= usize::try_from(pg_sys::NAMEDATALEN)
                .expect("PostgreSQL NAMEDATALEN fits usize")
    {
        return None;
    }
    Some(ValidatedProvider {
        descriptor,
        name,
        access_method_name,
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
) -> *const MaintenanceProviderV1 {
    // AM OIDs are database-local and do not exist yet during shared-preload
    // registration. Copy one stable descriptor pointer at a time, release the
    // RefCell borrow, and only then invoke catalog-reading provider callbacks.
    let provider_count = PROVIDERS.with_borrow(ProviderDirectory::len);
    let mut matched: *const MaintenanceProviderV1 = std::ptr::null();
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

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn stage_worker_wakeup(
    extension_name: *const c_char,
    worker_name: *const c_char,
) -> u32 {
    if crate::runtime::ensure_preloaded().is_err() {
        return STAGE_WORKER_WAKEUP_RUNTIME_NOT_PRELOADED;
    }
    if extension_name.is_null() || worker_name.is_null() {
        return STAGE_WORKER_WAKEUP_INVALID_REQUEST;
    }
    let extension_name = unsafe { CStr::from_ptr(extension_name) };
    let worker_name = unsafe { CStr::from_ptr(worker_name) };
    if extension_name.is_empty()
        || worker_name.is_empty()
        || worker_name.to_bytes().len() > crate::state::MAX_WORKER_NAME_BYTES
    {
        return STAGE_WORKER_WAKEUP_INVALID_REQUEST;
    }
    let Ok(worker_name) = worker_name.to_str() else {
        return STAGE_WORKER_WAKEUP_INVALID_REQUEST;
    };
    // Resolve the extension through PostgreSQL's syscache rather than scanning
    // lakebase.workers. Lifecycle publishes the wake only after commit. A
    // missing shared-memory slot triggers conservative database reconciliation.
    let extension_oid =
        unsafe { pg_sys::get_extension_oid(extension_name.as_ptr(), true) };
    if extension_oid == pg_sys::InvalidOid {
        return STAGE_WORKER_WAKEUP_EXTENSION_NOT_FOUND;
    }
    crate::lifecycle::request_wakeup(u32::from(extension_oid), worker_name);
    STAGE_WORKER_WAKEUP_OK
}

struct AmRegistrationRef<'a> {
    maintenance_provider: Option<ValidatedProvider<'a>>,
    utility: &'a [pg_lakebase_core::runtime_api::UtilityHookDescriptorV1],
    object_access:
        &'a [pg_lakebase_core::runtime_api::ObjectAccessHookDescriptorV1],
    object_access_str:
        &'a [pg_lakebase_core::runtime_api::ObjectAccessStrHookDescriptorV1],
}

impl<'a> AmRegistrationRef<'a> {
    unsafe fn descriptor_slice<T>(pointer: *const T, count: u32) -> Option<&'a [T]> {
        if count == 0 {
            return Some(&[]);
        }
        if pointer.is_null() {
            return None;
        }
        let count = usize::try_from(count).ok()?;
        Some(unsafe { std::slice::from_raw_parts(pointer, count) })
    }

    unsafe fn from_raw(registration: *const AmRegistrationV1) -> Option<Self> {
        let header = unsafe { registration.cast::<AbiHeader>().as_ref() }?;
        let expected_size =
            u32::try_from(std::mem::size_of::<AmRegistrationV1>()).ok()?;
        if header.abi_version != AM_REGISTRATION_VERSION
            || header.struct_size < expected_size
        {
            return None;
        }
        let registration = unsafe { &*registration };
        let maintenance_provider = if registration.maintenance_provider.is_null() {
            None
        } else {
            Some(unsafe { validate_provider(registration.maintenance_provider) }?)
        };
        Some(Self {
            maintenance_provider,
            utility: unsafe {
                Self::descriptor_slice(
                    registration.utility_hooks,
                    registration.utility_hook_count,
                )?
            },
            object_access: unsafe {
                Self::descriptor_slice(
                    registration.object_access_hooks,
                    registration.object_access_hook_count,
                )?
            },
            object_access_str: unsafe {
                Self::descriptor_slice(
                    registration.object_access_str_hooks,
                    registration.object_access_str_hook_count,
                )?
            },
        })
    }
}

struct PreparedAmRegistration {
    maintenance_provider: PreparedProviderRegistration,
    utility: crate::process_utility::PreparedUtilityHooks,
    object_access: crate::object_access::PreparedObjectAccessHooks,
}

impl PreparedAmRegistration {
    fn new(registration: AmRegistrationRef<'_>) -> Result<Self, u32> {
        // Every module finishes validation and all heap allocation before this
        // value can be committed. Returning an error therefore leaves every
        // logical runtime directory and PostgreSQL hook pointer unchanged.
        let maintenance_provider = match registration.maintenance_provider {
            Some(provider) => PROVIDERS.with_borrow_mut(|providers| {
                providers.prepare(
                    provider.descriptor,
                    provider.name,
                    provider.access_method_name,
                )
            })?,
            None => PreparedProviderRegistration { provider: None },
        };
        let utility = crate::process_utility::prepare_hooks(registration.utility)
            .ok_or(REGISTER_INVALID_DESCRIPTOR)?;
        let object_access = crate::object_access::prepare_hooks(
            registration.object_access,
            registration.object_access_str,
        )
        .ok_or(REGISTER_INVALID_DESCRIPTOR)?;
        Ok(Self {
            maintenance_provider,
            utility,
            object_access,
        })
    }

    fn commit(self) {
        PROVIDERS
            .with_borrow_mut(|providers| providers.commit(self.maintenance_provider));
        crate::process_utility::commit_hooks(self.utility);
        crate::object_access::commit_hooks(self.object_access);
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn register_am(
    registration: *const AmRegistrationV1,
) -> u32 {
    let Some(registration) = (unsafe { AmRegistrationRef::from_raw(registration) })
    else {
        return REGISTER_INVALID_DESCRIPTOR;
    };
    let prepared = match PreparedAmRegistration::new(registration) {
        Ok(prepared) => prepared,
        Err(status) => return status,
    };
    prepared.commit();
    REGISTER_OK
}

static RUNTIME_API: RuntimeApiV1 = RuntimeApiV1 {
    abi_version: RUNTIME_API_VERSION,
    struct_size: std::mem::size_of::<RuntimeApiV1>() as u32,
    register_am,
    has_providers,
    provider_for_am,
    customscan_mode,
    maintenance_config,
    stage_worker_wakeup,
};

pub(crate) fn init() {
    let slot = unsafe { rendezvous_slot() };
    assert!(
        !slot.is_null(),
        "PostgreSQL returned a null rendezvous slot"
    );
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
        *slot = (&RUNTIME_API as *const RuntimeApiV1)
            .cast_mut()
            .cast::<c_void>();
    }
    crate::process_utility::init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_lakebase_core::runtime_api::{
        HOOK_DESCRIPTOR_VERSION, MaintenanceReportV1, MaintenanceRequestV1,
        MaintenanceStatsV1, UtilityHookDescriptorV1,
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

    unsafe extern "C-unwind" fn utility_pre(
        _context: *mut c_void,
        _node: *mut pg_sys::Node,
    ) {
    }

    fn descriptor(
        name: &'static CStr,
        access_method_name: &'static CStr,
    ) -> MaintenanceProviderV1 {
        MaintenanceProviderV1 {
            abi_version: MAINTENANCE_PROVIDER_VERSION,
            struct_size: std::mem::size_of::<MaintenanceProviderV1>() as u32,
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

    #[test]
    fn invalid_hook_preparation_does_not_publish_any_am_state() {
        let before_providers = PROVIDERS.with_borrow(ProviderDirectory::len);
        let before_utility = crate::process_utility::registered_hook_count();
        let before_object = crate::object_access::registered_hook_counts();
        let provider = descriptor(c"atomic-invalid", c"atomic-invalid-am");
        let mut context = 0_u8;
        let invalid_utility = UtilityHookDescriptorV1 {
            abi_version: HOOK_DESCRIPTOR_VERSION,
            struct_size: std::mem::size_of::<UtilityHookDescriptorV1>() as u32,
            tag: pg_sys::NodeTag::T_CommentStmt as u32,
            context: std::ptr::from_mut(&mut context).cast(),
            on_pre: Some(utility_pre),
            on_post: None,
        };
        let registration = AmRegistrationV1 {
            abi_version: AM_REGISTRATION_VERSION,
            struct_size: std::mem::size_of::<AmRegistrationV1>() as u32,
            maintenance_provider: &provider,
            utility_hooks: &invalid_utility,
            utility_hook_count: 1,
            object_access_hooks: std::ptr::null(),
            object_access_hook_count: 0,
            object_access_str_hooks: std::ptr::null(),
            object_access_str_hook_count: 0,
        };

        let registration = unsafe { AmRegistrationRef::from_raw(&registration) }
            .expect("registration header and pointers are valid");
        assert_eq!(
            PreparedAmRegistration::new(registration).err(),
            Some(REGISTER_INVALID_DESCRIPTOR)
        );
        assert_eq!(
            PROVIDERS.with_borrow(ProviderDirectory::len),
            before_providers
        );
        assert_eq!(
            crate::process_utility::registered_hook_count(),
            before_utility
        );
        assert_eq!(
            crate::object_access::registered_hook_counts(),
            before_object
        );
    }

    #[test]
    fn registration_rejects_nonzero_count_with_null_pointer() {
        let registration = AmRegistrationV1 {
            abi_version: AM_REGISTRATION_VERSION,
            struct_size: std::mem::size_of::<AmRegistrationV1>() as u32,
            maintenance_provider: std::ptr::null(),
            utility_hooks: std::ptr::null(),
            utility_hook_count: 1,
            object_access_hooks: std::ptr::null(),
            object_access_hook_count: 0,
            object_access_str_hooks: std::ptr::null(),
            object_access_str_hook_count: 0,
        };

        assert!(unsafe { AmRegistrationRef::from_raw(&registration) }.is_none());
    }
}
