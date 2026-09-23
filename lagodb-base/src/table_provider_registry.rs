//! Runtime registry and C-bridge callbacks for table-provider descriptors.

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::ptr;

use lagodb_core::handles::RelationHandle;
use lagodb_core::runtime_api::{
    AbiHeader, REGISTER_DUPLICATE_ACCESS_METHOD, REGISTER_DUPLICATE_NAME,
    TableProvider, provider_access_method_name, provider_name,
};
use pgrx::{pg_guard, pg_sys};

thread_local! {
    static TABLE_PROVIDERS: RefCell<TableProviderRegistry> =
        const { RefCell::new(TableProviderRegistry::new()) };
}

struct StoredTableProvider {
    descriptor: Box<TableProvider>,
    _name: CString,
    _access_method_name: CString,
}

impl StoredTableProvider {
    fn new(
        descriptor: &TableProvider,
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

struct TableProviderRegistry {
    providers: Vec<StoredTableProvider>,
}

impl TableProviderRegistry {
    const fn new() -> Self {
        Self {
            providers: Vec::new(),
        }
    }

    fn prepare(
        &mut self,
        descriptor: &TableProvider,
        name: &CStr,
        access_method_name: &CStr,
    ) -> Result<Option<StoredTableProvider>, u32> {
        for existing in &self.providers {
            let existing_descriptor = existing.descriptor.as_ref();
            // SAFETY: stored descriptors were validated on registration and
            // point at the `CString`s owned by `StoredTableProvider`.
            let existing_name = unsafe { provider_name(existing_descriptor) }
                .expect("validated provider name");
            // SAFETY: same invariant as `existing_name` above.
            let existing_access_method_name =
                unsafe { provider_access_method_name(existing_descriptor) }
                    .expect("validated access-method name");
            if existing_name == name {
                let same_descriptor = existing_access_method_name
                    == access_method_name
                    && existing_descriptor.owns_partitioned_table
                        == descriptor.owns_partitioned_table
                    && existing_descriptor.supports_analyze
                        == descriptor.supports_analyze
                    && std::ptr::fn_addr_eq(
                        existing_descriptor.access_method_oid,
                        descriptor.access_method_oid,
                    )
                    && std::ptr::fn_addr_eq(
                        existing_descriptor.truncate_partitioned_table,
                        descriptor.truncate_partitioned_table,
                    )
                    && std::ptr::fn_addr_eq(
                        existing_descriptor.execute_maintenance,
                        descriptor.execute_maintenance,
                    )
                    && std::ptr::fn_addr_eq(
                        existing_descriptor.inspect_maintenance,
                        descriptor.inspect_maintenance,
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
        // Reserve before any runtime registry is changed. The later commit is
        // therefore allocation-free and cannot leave provider and hook
        // registries partially published.
        self.providers.reserve(1);
        Ok(Some(StoredTableProvider::new(
            descriptor,
            name,
            access_method_name,
        )))
    }

    fn commit(&mut self, provider: Option<StoredTableProvider>) {
        if let Some(provider) = provider {
            debug_assert!(self.providers.len() < self.providers.capacity());
            self.providers.push(provider);
        }
    }

    fn len(&self) -> usize {
        self.providers.len()
    }

    fn descriptor(&self, index: usize) -> *const TableProvider {
        self.providers[index].descriptor.as_ref()
    }

    fn has_partitioned_table_capability(&self) -> bool {
        self.providers
            .iter()
            .any(|provider| provider.descriptor.owns_partitioned_table)
    }
}

pub(crate) struct ValidatedTableProvider<'a> {
    descriptor: &'a TableProvider,
    name: &'a CStr,
    access_method_name: &'a CStr,
}

impl<'a> ValidatedTableProvider<'a> {
    /// Validate one exact-build table-provider descriptor.
    ///
    /// # Safety
    ///
    /// `descriptor` must satisfy the trusted internal ABI pointer contract
    /// documented by `lagodb_core::runtime_api`.
    pub(crate) unsafe fn from_raw(descriptor: *const TableProvider) -> Option<Self> {
        // SAFETY: callers uphold the module's trusted internal-ABI pointer and
        // alignment contract; `as_ref` handles the permitted null input.
        let header = unsafe { descriptor.cast::<AbiHeader>().as_ref() }?;
        let expected_size = u32::try_from(std::mem::size_of::<TableProvider>())
            .expect("table-provider descriptor size exceeds u32");
        if header.struct_size != expected_size {
            return None;
        }
        // SAFETY: the validated header states that the caller supplied the full
        // exact descriptor layout expected by this build.
        let descriptor = unsafe { &*descriptor };
        if descriptor.name.is_null() || descriptor.access_method_name.is_null() {
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

pub(crate) struct PreparedTableProviderRegistration {
    provider: Option<StoredTableProvider>,
}

impl PreparedTableProviderRegistration {
    pub(crate) fn prepare(
        provider: Option<ValidatedTableProvider<'_>>,
    ) -> Result<Self, u32> {
        let provider = match provider {
            Some(provider) => TABLE_PROVIDERS.with_borrow_mut(|providers| {
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

    pub(crate) fn commit(self) {
        TABLE_PROVIDERS.with_borrow_mut(|providers| providers.commit(self.provider));
    }
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn has_providers() -> u8 {
    // Registration happens during shared preload, before database-local access
    // method OIDs necessarily exist. Resolve the callbacks only when routing a
    // command in a connected database, and never invoke provider code while a
    // RefCell borrow is live.
    let provider_count = TABLE_PROVIDERS.with_borrow(TableProviderRegistry::len);
    for index in 0..provider_count {
        let descriptor =
            TABLE_PROVIDERS.with_borrow(|providers| providers.descriptor(index));
        // SAFETY: registry entries own validated, backend-lifetime descriptor
        // allocations, and the RefCell borrow was released before this access.
        let descriptor = unsafe { &*descriptor };
        // SAFETY: the callback was validated as part of the exact-build
        // descriptor and executes after the registry borrow is released.
        if unsafe { (descriptor.access_method_oid)() } != pg_sys::InvalidOid {
            return 1;
        }
    }
    0
}

fn lookup_provider_for_am(access_method_oid: pg_sys::Oid) -> *const TableProvider {
    // InvalidOid means no database-local AM, both for an unavailable provider
    // and for native partitioned tables created without USING. It is not an identity.
    if access_method_oid == pg_sys::InvalidOid {
        return ptr::null();
    }
    // AM OIDs are database-local and do not exist yet during shared-preload
    // registration. Copy one stable descriptor pointer at a time, release the
    // RefCell borrow, and only then invoke catalog-reading provider callbacks.
    let provider_count = TABLE_PROVIDERS.with_borrow(TableProviderRegistry::len);
    let mut matched: *const TableProvider = ptr::null();
    for index in 0..provider_count {
        let descriptor =
            TABLE_PROVIDERS.with_borrow(|providers| providers.descriptor(index));
        // SAFETY: registry entries own validated, backend-lifetime descriptor
        // allocations, and the RefCell borrow was released before this access.
        let descriptor = unsafe { &*descriptor };
        // SAFETY: the callback was validated as part of the exact-build
        // descriptor and executes after the registry borrow is released.
        let resolved_oid = unsafe { (descriptor.access_method_oid)() };
        // Unavailable providers cannot match the valid requested AM identity.
        if resolved_oid != access_method_oid {
            continue;
        }
        if !matched.is_null() {
            panic!(
                "multiple table providers resolved to access method OID {access_method_oid}"
            );
        }
        matched = descriptor;
    }
    matched
}

pub(crate) fn provider_owns_partitioned_table(
    access_method_oid: pg_sys::Oid,
) -> Option<bool> {
    let descriptor = lookup_provider_for_am(access_method_oid);
    // SAFETY: registry descriptors have stable backend lifetime; null means
    // that this access method has no provider registered in this backend.
    unsafe { descriptor.as_ref() }.map(|descriptor| descriptor.owns_partitioned_table)
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn provider_for_am(
    access_method_oid: pg_sys::Oid,
) -> *const TableProvider {
    lookup_provider_for_am(access_method_oid)
}

/// Registered planning capability, independent of database-local AM OIDs.
/// Actual relation ownership is still resolved from its AM during preparation.
pub(crate) fn has_partitioned_table_capability() -> bool {
    TABLE_PROVIDERS
        .with_borrow(TableProviderRegistry::has_partitioned_table_capability)
}

pub(crate) fn has_partitioned_table_provider() -> bool {
    let provider_count = TABLE_PROVIDERS.with_borrow(TableProviderRegistry::len);
    for index in 0..provider_count {
        let descriptor =
            TABLE_PROVIDERS.with_borrow(|providers| providers.descriptor(index));
        // SAFETY: the registry owns this validated descriptor for the backend
        // lifetime, and no RefCell borrow remains while callbacks run.
        let descriptor = unsafe { &*descriptor };
        if descriptor.owns_partitioned_table
            && unsafe { (descriptor.access_method_oid)() } != pg_sys::InvalidOid
        {
            return true;
        }
    }
    false
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn owns_partitioned_table(
    relation: pg_sys::Relation,
) -> bool {
    // SAFETY: the C route planner owns this live, AccessExclusiveLock-held
    // relation for the duration of the synchronous ownership query.
    let relation_handle = unsafe { RelationHandle::from_raw(relation) };
    let access_method_oid = relation_handle.access_method_oid();
    let descriptor = lookup_provider_for_am(access_method_oid);
    // SAFETY: registry descriptors have stable backend lifetime; null means no
    // provider owns this access method.
    let Some(descriptor) = (unsafe { descriptor.as_ref() }) else {
        return false;
    };
    descriptor.owns_partitioned_table
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn truncate_partitioned_table(
    relation: pg_sys::Relation,
) {
    // SAFETY: the C executor calls this action only for a live, locked relation
    // that the ownership callback classified as a provider-owned partitioned table.
    let relation_handle = unsafe { RelationHandle::from_raw(relation) };
    let descriptor = lookup_provider_for_am(relation_handle.access_method_oid());
    // SAFETY: the ownership callback already resolved this stable backend-
    // lifetime descriptor, and registration is immutable during execution.
    let descriptor = unsafe { descriptor.as_ref() }
        .expect("partitioned table TRUNCATE action lost its owning table provider");
    debug_assert!(descriptor.owns_partitioned_table);
    // SAFETY: exact-build registration validated the callback, and the C
    // bridge keeps `relation` live and locked until the callback returns.
    unsafe { (descriptor.truncate_partitioned_table)(relation) };
}
