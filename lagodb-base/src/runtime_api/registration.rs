//! Atomic provider-registration transaction.
//!
//! Every participating directory validates and allocates during prepare. No
//! directory is published until the complete provider batch is ready to
//! commit, preserving the runtime's all-or-nothing registration contract.

use std::mem::size_of;
use std::slice;

use lagodb_core::runtime_api::{
    ModifyPlannerDescriptor, ObjectAccessHookDescriptor,
    ObjectAccessStrHookDescriptor, ProviderRegistration, QuerySourceDescriptor,
    REGISTER_INVALID_DESCRIPTOR, REGISTER_OK, RelationScanPlannerDescriptor,
    UtilityConsumerDescriptor, UtilityHookDescriptor,
};
use pgrx::{pg_guard, pg_sys};

use crate::object_access::{self, PreparedObjectAccessHooks};
use crate::planning_hooks::{self, PreparedPlanningHooks};
use crate::process_utility::{self, PreparedUtilityHooks};
use crate::provider_bootstrap::{
    self, PreparedProviderIdentity, ValidatedProviderIdentity,
};
use crate::utility_consumer::{self, PreparedUtilityConsumers};

use super::maintenance;
use super::source_directory::PreparedQuerySource;

struct ProviderRegistrationRef<'a> {
    provider: ValidatedProviderIdentity<'a>,
    maintenance_provider: Option<maintenance::ValidatedProvider<'a>>,
    utility: &'a [UtilityHookDescriptor],
    utility_consumers: &'a [UtilityConsumerDescriptor],
    object_access: &'a [ObjectAccessHookDescriptor],
    object_access_str: &'a [ObjectAccessStrHookDescriptor],
    relation_scan_planner: Option<&'a RelationScanPlannerDescriptor>,
    modify_planner: Option<&'a ModifyPlannerDescriptor>,
    query_source: Option<&'a QuerySourceDescriptor>,
}

impl<'a> ProviderRegistrationRef<'a> {
    unsafe fn descriptor_slice<T>(pointer: *const T, count: u32) -> Option<&'a [T]> {
        if count == 0 {
            return Some(&[]);
        }
        if pointer.is_null() {
            return None;
        }
        let count = usize::try_from(count).ok()?;
        // SAFETY: the trusted registration ABI requires a non-null pointer to
        // `count` live descriptors; the returned view cannot outlive the input
        // registration's synchronous prepare call.
        Some(unsafe { slice::from_raw_parts(pointer, count) })
    }

    unsafe fn from_raw(registration: *const ProviderRegistration) -> Option<Self> {
        // SAFETY: callers uphold the trusted internal-ABI pointer and alignment
        // contract; `as_ref` handles the permitted null input.
        let registration = unsafe { registration.as_ref() }?;
        let expected_size = u32::try_from(size_of::<ProviderRegistration>()).ok()?;
        if registration.struct_size != expected_size {
            return None;
        }
        // SAFETY: the registration pointer is governed by the same trusted
        // internal ABI contract validated by this constructor.
        let provider =
            unsafe { ValidatedProviderIdentity::from_raw(registration.provider) }?;
        let maintenance_provider = if registration.maintenance_provider.is_null() {
            None
        } else {
            // SAFETY: the containing exact-build registration guarantees a
            // live maintenance descriptor for this synchronous validation.
            Some(unsafe {
                maintenance::ValidatedProvider::from_raw(
                    registration.maintenance_provider,
                )
            }?)
        };
        Some(Self {
            provider,
            maintenance_provider,
            // SAFETY: every pointer/count pair is covered by the containing
            // registration's trusted synchronous ABI contract.
            utility: unsafe {
                Self::descriptor_slice(
                    registration.utility_hooks,
                    registration.utility_hook_count,
                )?
            },
            // SAFETY: same pointer/count contract as `utility` above.
            utility_consumers: unsafe {
                Self::descriptor_slice(
                    registration.utility_consumers,
                    registration.utility_consumer_count,
                )?
            },
            // SAFETY: same pointer/count contract as `utility` above.
            object_access: unsafe {
                Self::descriptor_slice(
                    registration.object_access_hooks,
                    registration.object_access_hook_count,
                )?
            },
            // SAFETY: same pointer/count contract as `utility` above.
            object_access_str: unsafe {
                Self::descriptor_slice(
                    registration.object_access_str_hooks,
                    registration.object_access_str_hook_count,
                )?
            },
            // SAFETY: optional facet pointers follow the containing trusted
            // synchronous registration ABI contract.
            relation_scan_planner: unsafe {
                registration.relation_scan_planner.as_ref()
            },
            // SAFETY: same optional exact-build facet contract as above.
            modify_planner: unsafe { registration.modify_planner.as_ref() },
            // SAFETY: same optional exact-build facet contract as above.
            query_source: unsafe { registration.query_source.as_ref() },
        })
    }
}

struct PreparedProviderRegistration {
    maintenance: maintenance::PreparedRegistration,
    provider: PreparedProviderIdentity,
    utility: PreparedUtilityHooks,
    utility_consumers: PreparedUtilityConsumers,
    object_access: PreparedObjectAccessHooks,
    planning: PreparedPlanningHooks,
    query_source: PreparedQuerySource,
}

impl PreparedProviderRegistration {
    fn prepare(registration: ProviderRegistrationRef<'_>) -> Result<Self, u32> {
        // Every module finishes validation and all heap allocation before this
        // value can be committed. Returning an error therefore leaves every
        // logical runtime directory and PostgreSQL hook pointer unchanged.
        let maintenance = maintenance::PreparedRegistration::prepare(
            registration.maintenance_provider,
        )?;
        let utility = process_utility::prepare_hooks(registration.utility)
            .ok_or(REGISTER_INVALID_DESCRIPTOR)?;
        let utility_consumers =
            utility_consumer::prepare_consumers(registration.utility_consumers)
                .ok_or(REGISTER_INVALID_DESCRIPTOR)?;
        let object_access = object_access::prepare_hooks(
            registration.object_access,
            registration.object_access_str,
        )
        .ok_or(REGISTER_INVALID_DESCRIPTOR)?;
        let planning = planning_hooks::prepare(
            registration.relation_scan_planner,
            registration.modify_planner,
        )
        .ok_or(REGISTER_INVALID_DESCRIPTOR)?;
        let query_source = PreparedQuerySource::validate(registration.query_source)
            .ok_or(REGISTER_INVALID_DESCRIPTOR)?;
        // Validate bootstrap ownership only after the complete batch has been
        // validated. This preserves the more specific duplicate-provider and
        // invalid-descriptor results while still preventing every directory
        // from being committed outside the bootstrap window.
        let provider = provider_bootstrap::prepare_identity(registration.provider)?;
        let query_source = PreparedQuerySource::prepare(
            provider.provider_id(),
            provider.provider_name(),
            query_source,
        );
        Ok(Self {
            maintenance,
            provider,
            utility,
            utility_consumers,
            object_access,
            planning,
            query_source,
        })
    }

    fn commit(self) {
        // Every directory has finished validation and allocation before any
        // registration becomes visible.
        planning_hooks::commit(self.planning);
        self.query_source.commit();
        self.maintenance.commit();
        process_utility::commit_hooks(self.utility);
        utility_consumer::commit_consumers(self.utility_consumers);
        object_access::commit_hooks(self.object_access);
        provider_bootstrap::commit_identity(self.provider);
    }
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn register_provider(
    registration: *const ProviderRegistration,
) -> u32 {
    // SAFETY: PostgreSQL exposes binary-upgrade state as a backend-global flag.
    if unsafe { pg_sys::IsBinaryUpgrade } {
        return REGISTER_OK;
    }
    // SAFETY: callers of this runtime ABI entry point must supply a live exact-
    // build registration descriptor as documented by the core API.
    let Some(registration) =
        (unsafe { ProviderRegistrationRef::from_raw(registration) })
    else {
        return REGISTER_INVALID_DESCRIPTOR;
    };
    let prepared = match PreparedProviderRegistration::prepare(registration) {
        Ok(prepared) => prepared,
        Err(status) => return status,
    };
    prepared.commit();
    REGISTER_OK
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, c_char, c_void};
    use std::ptr;

    use lagodb_core::runtime_api::{
        FFI_OPERATION_OK, FfiErrorRecord, MaintenanceProvider, MaintenanceReport,
        MaintenanceRequest, MaintenanceStats, OBJECT_ACCESS_DROP,
        ObjectAccessHookDescriptor, ObjectAccessStrHookDescriptor, ProviderIdentity,
        RelationScanPlannerDescriptor, UtilityHookDescriptor,
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

    unsafe extern "C-unwind" fn utility_pre(
        _context: *mut c_void,
        _planned_stmt: *mut pg_sys::PlannedStmt,
        _query_string: *const c_char,
    ) {
    }

    unsafe extern "C-unwind" fn utility_post(
        _context: *mut c_void,
        _node: *mut pg_sys::Node,
    ) {
    }

    unsafe extern "C-unwind" fn object_access_callback(
        _context: *mut c_void,
        _access: pg_sys::ObjectAccessType::Type,
        _class_id: pg_sys::Oid,
        _object_id: pg_sys::Oid,
        _sub_id: i32,
        _arg: *mut c_void,
    ) {
    }

    unsafe extern "C-unwind" fn object_access_str_callback(
        _context: *mut c_void,
        _access: pg_sys::ObjectAccessType::Type,
        _class_id: pg_sys::Oid,
        _object_name: *const c_char,
        _sub_id: i32,
        _arg: *mut c_void,
    ) {
    }

    unsafe extern "C-unwind" fn plan_relation(
        _context: *mut c_void,
        _root: *mut pg_sys::PlannerInfo,
        _rel: *mut pg_sys::RelOptInfo,
        _rti: pg_sys::Index,
        _rte: *mut pg_sys::RangeTblEntry,
        _error: *mut FfiErrorRecord,
    ) -> u32 {
        FFI_OPERATION_OK
    }

    fn maintenance_descriptor(
        name: &'static CStr,
        access_method_name: &'static CStr,
    ) -> MaintenanceProvider {
        MaintenanceProvider {
            struct_size: size_of::<MaintenanceProvider>() as u32,
            name: name.as_ptr(),
            access_method_name: access_method_name.as_ptr(),
            capability_flags: 0,
            access_method_oid,
            execute,
            inspect,
        }
    }

    fn identity() -> ProviderIdentity {
        ProviderIdentity::access_method(
            c"runtime-api-test",
            c"lagodb_base",
            c"lagodb_base",
        )
    }

    #[test]
    fn invalid_hook_preparation_does_not_publish_maintenance_provider() {
        let provider =
            maintenance_descriptor(c"atomic-invalid", c"atomic-invalid-am");
        let mut context = 0_u8;
        let invalid_utility = UtilityHookDescriptor {
            struct_size: size_of::<UtilityHookDescriptor>() as u32,
            tag: pg_sys::NodeTag::T_CommentStmt as u32,
            context: ptr::from_mut(&mut context).cast(),
            on_pre: Some(utility_pre),
            on_post: None,
        };
        let identity = identity();
        let registration = ProviderRegistration {
            struct_size: size_of::<ProviderRegistration>() as u32,
            provider: &identity,
            maintenance_provider: &provider,
            utility_hooks: &invalid_utility,
            utility_hook_count: 1,
            utility_consumers: ptr::null(),
            utility_consumer_count: 0,
            object_access_hooks: ptr::null(),
            object_access_hook_count: 0,
            object_access_str_hooks: ptr::null(),
            object_access_str_hook_count: 0,
            relation_scan_planner: ptr::null(),
            modify_planner: ptr::null(),
            query_source: ptr::null(),
        };

        // SAFETY: all local descriptors and pointer/count pairs remain live
        // for this synchronous validation and preparation.
        let registration =
            unsafe { ProviderRegistrationRef::from_raw(&registration) }
                .expect("registration header and pointers are valid");
        assert_eq!(
            PreparedProviderRegistration::prepare(registration).err(),
            Some(REGISTER_INVALID_DESCRIPTOR)
        );

        let competing = maintenance_descriptor(c"atomic-invalid", c"atomic-retry-am");
        // SAFETY: the local descriptor and its static string pointers remain
        // live for this synchronous validation and preparation.
        let competing =
            unsafe { maintenance::ValidatedProvider::from_raw(&competing) }
                .expect("competing maintenance provider is valid");
        assert!(
            maintenance::PreparedRegistration::prepare(Some(competing)).is_ok(),
            "failed registration published its prepared maintenance provider"
        );
    }

    #[test]
    fn invalid_planning_facet_does_not_publish_any_prepared_facet() {
        let identity = identity();
        let provider =
            maintenance_descriptor(c"atomic-planning", c"atomic-planning-am");
        let mut context = 0_u8;
        let context = ptr::from_mut(&mut context).cast();
        let utility = UtilityHookDescriptor {
            struct_size: size_of::<UtilityHookDescriptor>() as u32,
            tag: pg_sys::NodeTag::T_CommentStmt as u32,
            context,
            on_pre: Some(utility_pre),
            on_post: Some(utility_post),
        };
        let object_access = ObjectAccessHookDescriptor {
            struct_size: size_of::<ObjectAccessHookDescriptor>() as u32,
            event_mask: OBJECT_ACCESS_DROP,
            class_id: pg_sys::InvalidOid,
            context,
            on_access: Some(object_access_callback),
        };
        let object_access_str = ObjectAccessStrHookDescriptor {
            struct_size: size_of::<ObjectAccessStrHookDescriptor>() as u32,
            event_mask: OBJECT_ACCESS_DROP,
            class_id: pg_sys::InvalidOid,
            context,
            on_access: Some(object_access_str_callback),
        };
        let invalid_planner = RelationScanPlannerDescriptor {
            struct_size: 0,
            context: ptr::null_mut(),
            plan_relation: Some(plan_relation),
        };
        let utility_count = process_utility::registered_hook_count();
        let object_access_counts = object_access::registered_hook_counts();
        let registration = ProviderRegistration {
            struct_size: size_of::<ProviderRegistration>() as u32,
            provider: &identity,
            maintenance_provider: &provider,
            utility_hooks: &utility,
            utility_hook_count: 1,
            utility_consumers: ptr::null(),
            utility_consumer_count: 0,
            object_access_hooks: &object_access,
            object_access_hook_count: 1,
            object_access_str_hooks: &object_access_str,
            object_access_str_hook_count: 1,
            relation_scan_planner: &invalid_planner,
            modify_planner: ptr::null(),
            query_source: ptr::null(),
        };
        // SAFETY: every local descriptor remains live for synchronous prepare.
        let registration = unsafe {
            ProviderRegistrationRef::from_raw(&registration)
                .expect("registration header and pointers are valid")
        };
        assert_eq!(
            PreparedProviderRegistration::prepare(registration).err(),
            Some(REGISTER_INVALID_DESCRIPTOR)
        );
        assert_eq!(
            process_utility::registered_hook_count(),
            utility_count,
            "failed registration published its prepared utility hook"
        );
        assert_eq!(
            object_access::registered_hook_counts(),
            object_access_counts,
            "failed registration published its prepared object-access hooks"
        );
        let competing =
            maintenance_descriptor(c"atomic-planning-retry", c"atomic-planning-am");
        // SAFETY: the local exact-build descriptor remains live for prepare.
        let competing =
            unsafe { maintenance::ValidatedProvider::from_raw(&competing) }
                .expect("competing maintenance provider is valid");
        assert!(
            maintenance::PreparedRegistration::prepare(Some(competing)).is_ok(),
            "failed registration published its prepared maintenance provider"
        );
    }

    #[test]
    fn registration_rejects_nonzero_count_with_null_pointer() {
        let identity = identity();
        let registration = ProviderRegistration {
            struct_size: size_of::<ProviderRegistration>() as u32,
            provider: &identity,
            maintenance_provider: ptr::null(),
            utility_hooks: ptr::null(),
            utility_hook_count: 1,
            utility_consumers: ptr::null(),
            utility_consumer_count: 0,
            object_access_hooks: ptr::null(),
            object_access_hook_count: 0,
            object_access_str_hooks: ptr::null(),
            object_access_str_hook_count: 0,
            relation_scan_planner: ptr::null(),
            modify_planner: ptr::null(),
            query_source: ptr::null(),
        };

        // SAFETY: the local identity and registration remain live for this
        // synchronous validation call.
        assert!(
            unsafe { ProviderRegistrationRef::from_raw(&registration) }.is_none()
        );
    }

    #[test]
    fn registration_requires_exact_size() {
        let identity = identity();
        let mut registration = ProviderRegistration {
            struct_size: size_of::<ProviderRegistration>() as u32 + 1,
            provider: &identity,
            maintenance_provider: ptr::null(),
            utility_hooks: ptr::null(),
            utility_hook_count: 0,
            utility_consumers: ptr::null(),
            utility_consumer_count: 0,
            object_access_hooks: ptr::null(),
            object_access_hook_count: 0,
            object_access_str_hooks: ptr::null(),
            object_access_str_hook_count: 0,
            relation_scan_planner: ptr::null(),
            modify_planner: ptr::null(),
            query_source: ptr::null(),
        };

        // SAFETY: the local identity and registration remain live for both
        // synchronous validation calls below.
        assert!(
            unsafe { ProviderRegistrationRef::from_raw(&registration) }.is_none()
        );
        registration.struct_size = size_of::<ProviderRegistration>() as u32;
        assert!(
            unsafe { ProviderRegistrationRef::from_raw(&registration) }.is_some()
        );
    }
}
