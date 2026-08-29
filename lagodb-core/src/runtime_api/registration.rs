//! Runtime registration transaction and callback descriptors.

use std::ffi::{CStr, c_char, c_void};
use std::mem::size_of;

use pgrx::pg_sys;

use super::{
    MaintenanceProvider, ModifyPlannerDescriptor, RelationScanPlannerDescriptor,
};

pub const PROVIDER_KIND_ACCESS_METHOD: u32 = 1;
pub const PROVIDER_KIND_FOREIGN_DATA_WRAPPER: u32 = 2;

/// Stable identity of one runtime-loaded LagoDB provider.
///
/// Every AM and FDW DSO publishes this identity together with its complete
/// hook batch. The runtime copies the strings during registration, so the
/// descriptor itself only needs to live for the synchronous registration call.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ProviderIdentity {
    pub struct_size: u32,
    pub name: *const c_char,
    pub extension_name: *const c_char,
    pub library_name: *const c_char,
    pub kind: u32,
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
            PROVIDER_KIND_ACCESS_METHOD,
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
            PROVIDER_KIND_FOREIGN_DATA_WRAPPER,
        )
    }

    fn new(
        name: &'static CStr,
        extension_name: &'static CStr,
        library_name: &'static CStr,
        kind: u32,
    ) -> Self {
        Self {
            struct_size: u32::try_from(size_of::<Self>())
                .expect("provider identity size exceeds u32"),
            name: name.as_ptr(),
            extension_name: extension_name.as_ptr(),
            library_name: library_name.as_ptr(),
            kind,
        }
    }
}

pub type RoutedUtilityPreHook =
    unsafe extern "C-unwind" fn(*mut c_void, *mut pg_sys::PlannedStmt, *const c_char);
pub type RoutedUtilityPostHook =
    unsafe extern "C-unwind" fn(*mut c_void, *mut pg_sys::Node);

#[repr(C)]
#[derive(Clone, Copy)]
pub struct UtilityHookDescriptor {
    pub struct_size: u32,
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
    pub struct_size: u32,
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
    pub struct_size: u32,
    pub event_mask: u32,
    /// `InvalidOid` matches every PostgreSQL object class.
    pub class_id: pg_sys::Oid,
    pub context: *mut c_void,
    pub on_access: Option<RoutedObjectAccessHook>,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ObjectAccessStrHookDescriptor {
    pub struct_size: u32,
    pub event_mask: u32,
    /// `InvalidOid` matches every PostgreSQL object class.
    pub class_id: pg_sys::Oid,
    pub context: *mut c_void,
    pub on_access: Option<RoutedObjectAccessStrHook>,
}

/// One provider's complete runtime registration transaction.
///
/// The runtime validates and prepares the optional maintenance provider and
/// every hook descriptor before publishing any of them. Pointer fields may be
/// null only when the corresponding count is zero; descriptor storage only
/// needs to live for the registration call.
///
/// The raw pointers are part of the trusted internal ABI described in the
/// parent module's trust model. Construct registrations through the core
/// hook/provider APIs rather than assembling this type in application code.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ProviderRegistration {
    pub struct_size: u32,
    /// Required identity of the registering AM or FDW.
    pub provider: *const ProviderIdentity,
    /// Optional maintenance provider staged by the same provider. Null means none.
    pub maintenance_provider: *const MaintenanceProvider,
    pub utility_hooks: *const UtilityHookDescriptor,
    pub utility_hook_count: u32,
    pub utility_consumers: *const UtilityConsumerDescriptor,
    pub utility_consumer_count: u32,
    pub object_access_hooks: *const ObjectAccessHookDescriptor,
    pub object_access_hook_count: u32,
    pub object_access_str_hooks: *const ObjectAccessStrHookDescriptor,
    pub object_access_str_hook_count: u32,
    /// Optional relation CustomScan planning facet. Null means none.
    pub relation_scan_planner: *const RelationScanPlannerDescriptor,
    /// Optional ModifyTable planning facet. Null means none.
    pub modify_planner: *const ModifyPlannerDescriptor,
}
