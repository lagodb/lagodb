//! Runtime registration transaction and callback descriptors.

use std::ffi::{CStr, c_char, c_void};

use pgrx::pg_sys;

use super::{
    AbiSlice, ModifyPlannerDescriptor, RelationScanPlannerDescriptor, TableProvider,
    TableScanDescriptor, TableScanWorkerDescriptor,
};

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderKind {
    AccessMethod = 1,
    ForeignDataWrapper = 2,
}

/// Stable identity of one runtime-loaded LagoDB provider.
///
/// Every AM and FDW DSO publishes this identity together with its complete
/// hook batch. The runtime copies the strings during registration, so the
/// descriptor itself only needs to live for the synchronous registration call.
///
/// Fields are private: the only constructors take `&'static CStr`, so every
/// string pointer names a live NUL-terminated string for the backend lifetime.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ProviderIdentity {
    name: *const c_char,
    extension_name: *const c_char,
    library_name: *const c_char,
    kind: ProviderKind,
}

impl ProviderIdentity {
    #[must_use]
    pub fn access_method(
        name: &'static CStr,
        extension_name: &'static CStr,
        library_name: &'static CStr,
    ) -> Self {
        Self::new(
            name,
            extension_name,
            library_name,
            ProviderKind::AccessMethod,
        )
    }

    #[must_use]
    pub fn foreign_data_wrapper(
        name: &'static CStr,
        extension_name: &'static CStr,
        library_name: &'static CStr,
    ) -> Self {
        Self::new(
            name,
            extension_name,
            library_name,
            ProviderKind::ForeignDataWrapper,
        )
    }

    fn new(
        name: &'static CStr,
        extension_name: &'static CStr,
        library_name: &'static CStr,
        kind: ProviderKind,
    ) -> Self {
        Self {
            name: name.as_ptr(),
            extension_name: extension_name.as_ptr(),
            library_name: library_name.as_ptr(),
            kind,
        }
    }

    #[must_use]
    pub fn name(&self) -> &'static CStr {
        // SAFETY: constructors store only `&'static CStr` pointers.
        unsafe { CStr::from_ptr(self.name) }
    }

    #[must_use]
    pub fn extension_name(&self) -> &'static CStr {
        // SAFETY: constructors store only `&'static CStr` pointers.
        unsafe { CStr::from_ptr(self.extension_name) }
    }

    #[must_use]
    pub fn library_name(&self) -> &'static CStr {
        // SAFETY: constructors store only `&'static CStr` pointers.
        unsafe { CStr::from_ptr(self.library_name) }
    }

    #[must_use]
    pub const fn kind(&self) -> ProviderKind {
        self.kind
    }
}

pub type RoutedUtilityPreHook =
    unsafe extern "C-unwind" fn(*mut c_void, *mut pg_sys::PlannedStmt, *const c_char);
pub type RoutedUtilityPostHook =
    unsafe extern "C-unwind" fn(*mut c_void, *mut pg_sys::Node);

#[repr(C)]
#[derive(Clone, Copy)]
pub struct UtilityHookDescriptor {
    pub tag: u32,
    pub context: *mut c_void,
    pub on_pre: Option<RoutedUtilityPreHook>,
    pub on_post: Option<RoutedUtilityPostHook>,
}

pub type RoutedUtilityPredicate = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const c_char,
    read_only_tree: bool,
    process_context: pg_sys::ProcessUtilityContext::Type,
    params: *mut pg_sys::ParamListInfoData,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    completion_tag: *mut pg_sys::QueryCompletion,
) -> u8;

pub type RoutedUtilityConsumer = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const c_char,
    read_only_tree: bool,
    process_context: pg_sys::ProcessUtilityContext::Type,
    params: *mut pg_sys::ParamListInfoData,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    completion_tag: *mut pg_sys::QueryCompletion,
) -> u8;

/// A utility consumer may take ownership of a statement instead of allowing
/// PostgreSQL's standard `ProcessUtility` path to execute it.
///
/// Consumers are deliberately separate from [`UtilityHookDescriptor`]. A
/// hook observes a command before or after execution; a consumer decides
/// whether the parent utility implementation runs at all.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct UtilityConsumerDescriptor {
    pub tag: u32,
    pub context: *mut c_void,
    pub on_match: Option<RoutedUtilityPredicate>,
    pub on_consume: Option<RoutedUtilityConsumer>,
}

pub type RoutedObjectAccessHook = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    access: pg_sys::ObjectAccessType::Type,
    class_id: pg_sys::Oid,
    object_id: pg_sys::Oid,
    sub_id: i32,
    arg: *mut c_void,
);

pub type RoutedObjectAccessStrHook = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    access: pg_sys::ObjectAccessType::Type,
    class_id: pg_sys::Oid,
    object_name: *const c_char,
    sub_id: i32,
    arg: *mut c_void,
);

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ObjectAccessHookDescriptor {
    pub event_mask: u32,
    /// `InvalidOid` matches every PostgreSQL object class.
    pub class_id: pg_sys::Oid,
    pub context: *mut c_void,
    pub on_access: Option<RoutedObjectAccessHook>,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ObjectAccessStrHookDescriptor {
    pub event_mask: u32,
    /// `InvalidOid` matches every PostgreSQL object class.
    pub class_id: pg_sys::Oid,
    pub context: *mut c_void,
    pub on_access: Option<RoutedObjectAccessStrHook>,
}

/// One provider's complete runtime registration transaction.
///
/// The runtime validates and prepares the optional table-provider facet and
/// every hook descriptor before publishing any of them. Descriptor storage is
/// borrowed only for the registration call. Fields are private so every
/// reference and descriptor array is valid by construction.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ProviderRegistration<'a> {
    provider: &'a ProviderIdentity,
    table_provider: Option<&'a TableProvider>,
    utility_hooks: AbiSlice<'a, UtilityHookDescriptor>,
    utility_consumers: AbiSlice<'a, UtilityConsumerDescriptor>,
    object_access_hooks: AbiSlice<'a, ObjectAccessHookDescriptor>,
    object_access_str_hooks: AbiSlice<'a, ObjectAccessStrHookDescriptor>,
    relation_scan_planner: Option<&'a RelationScanPlannerDescriptor>,
    modify_planner: Option<&'a ModifyPlannerDescriptor>,
    table_scan: Option<&'a TableScanDescriptor>,
    /// Worker reconstruction facet for `table_scan`. `None` means the
    /// registered routes support only direct stream execution.
    table_scan_worker: Option<&'a TableScanWorkerDescriptor>,
}

impl<'a> ProviderRegistration<'a> {
    /// Start a registration that carries only the provider identity.
    #[must_use]
    pub const fn new(provider: &'a ProviderIdentity) -> Self {
        Self {
            provider,
            table_provider: None,
            utility_hooks: AbiSlice::new(&[]),
            utility_consumers: AbiSlice::new(&[]),
            object_access_hooks: AbiSlice::new(&[]),
            object_access_str_hooks: AbiSlice::new(&[]),
            relation_scan_planner: None,
            modify_planner: None,
            table_scan: None,
            table_scan_worker: None,
        }
    }

    #[must_use]
    pub const fn with_table_provider(
        mut self,
        table_provider: Option<&'a TableProvider>,
    ) -> Self {
        self.table_provider = table_provider;
        self
    }

    #[must_use]
    pub const fn with_utility_hooks(
        mut self,
        hooks: &'a [UtilityHookDescriptor],
    ) -> Self {
        self.utility_hooks = AbiSlice::new(hooks);
        self
    }

    #[must_use]
    pub const fn with_utility_consumers(
        mut self,
        consumers: &'a [UtilityConsumerDescriptor],
    ) -> Self {
        self.utility_consumers = AbiSlice::new(consumers);
        self
    }

    #[must_use]
    pub const fn with_object_access_hooks(
        mut self,
        hooks: &'a [ObjectAccessHookDescriptor],
        str_hooks: &'a [ObjectAccessStrHookDescriptor],
    ) -> Self {
        self.object_access_hooks = AbiSlice::new(hooks);
        self.object_access_str_hooks = AbiSlice::new(str_hooks);
        self
    }

    #[must_use]
    pub const fn with_planners(
        mut self,
        relation_scan: Option<&'a RelationScanPlannerDescriptor>,
        modify: Option<&'a ModifyPlannerDescriptor>,
    ) -> Self {
        self.relation_scan_planner = relation_scan;
        self.modify_planner = modify;
        self
    }

    #[must_use]
    pub const fn with_table_scan(
        mut self,
        table_scan: Option<&'a TableScanDescriptor>,
        worker: Option<&'a TableScanWorkerDescriptor>,
    ) -> Self {
        self.table_scan = table_scan;
        self.table_scan_worker = worker;
        self
    }

    #[must_use]
    pub const fn provider(&self) -> &'a ProviderIdentity {
        self.provider
    }

    #[must_use]
    pub const fn table_provider(&self) -> Option<&'a TableProvider> {
        self.table_provider
    }

    #[must_use]
    pub fn utility_hooks(&self) -> &'a [UtilityHookDescriptor] {
        self.utility_hooks.as_slice()
    }

    #[must_use]
    pub fn utility_consumers(&self) -> &'a [UtilityConsumerDescriptor] {
        self.utility_consumers.as_slice()
    }

    #[must_use]
    pub fn object_access_hooks(&self) -> &'a [ObjectAccessHookDescriptor] {
        self.object_access_hooks.as_slice()
    }

    #[must_use]
    pub fn object_access_str_hooks(&self) -> &'a [ObjectAccessStrHookDescriptor] {
        self.object_access_str_hooks.as_slice()
    }

    #[must_use]
    pub const fn relation_scan_planner(
        &self,
    ) -> Option<&'a RelationScanPlannerDescriptor> {
        self.relation_scan_planner
    }

    #[must_use]
    pub const fn modify_planner(&self) -> Option<&'a ModifyPlannerDescriptor> {
        self.modify_planner
    }

    #[must_use]
    pub const fn table_scan(&self) -> Option<&'a TableScanDescriptor> {
        self.table_scan
    }

    #[must_use]
    pub const fn table_scan_worker(&self) -> Option<&'a TableScanWorkerDescriptor> {
        self.table_scan_worker
    }
}
