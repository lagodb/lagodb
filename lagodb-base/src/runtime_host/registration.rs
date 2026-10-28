//! Atomic provider-registration transaction.
//!
//! Every participating registry validates and allocates during prepare. No
//! registry is published until the complete provider batch is ready to
//! commit, preserving the runtime's all-or-nothing registration contract.

use lagodb_core::runtime_api::{
    ProviderRegistration, REGISTER_INVALID_DESCRIPTOR, REGISTER_OK,
};
use pgrx::{pg_guard, pg_sys};

use crate::object_access::{self, PreparedObjectAccessHooks};
use crate::planning_hooks::{self, PreparedPlanningHooks};
use crate::process_utility::{self, PreparedUtilityConsumers, PreparedUtilityHooks};
use crate::provider_bootstrap::{
    self, PreparedProviderIdentity, ValidatedProviderIdentity,
};
use crate::query_host::PendingTableScanRegistration;
use crate::table_provider_registry::{
    PreparedTableProviderRegistration, ValidatedTableProvider,
};

struct PreparedProviderRegistration {
    table_provider: PreparedTableProviderRegistration,
    provider: PreparedProviderIdentity,
    utility: PreparedUtilityHooks,
    utility_consumers: PreparedUtilityConsumers,
    object_access: PreparedObjectAccessHooks,
    planning: PreparedPlanningHooks,
    table_scan: PendingTableScanRegistration,
}

impl PreparedProviderRegistration {
    fn prepare(registration: &ProviderRegistration<'_>) -> Result<Self, u32> {
        let identity = ValidatedProviderIdentity::new(registration.provider())
            .ok_or(REGISTER_INVALID_DESCRIPTOR)?;
        let table_provider = registration
            .table_provider()
            .map(|descriptor| {
                ValidatedTableProvider::new(descriptor)
                    .ok_or(REGISTER_INVALID_DESCRIPTOR)
            })
            .transpose()?;
        // Every module finishes validation and all heap allocation before this
        // value can be committed. Returning an error therefore leaves every
        // logical runtime registry and PostgreSQL hook pointer unchanged.
        let table_provider =
            PreparedTableProviderRegistration::prepare(table_provider)?;
        let utility = process_utility::prepare_hooks(registration.utility_hooks())
            .ok_or(REGISTER_INVALID_DESCRIPTOR)?;
        let utility_consumers =
            process_utility::prepare_consumers(registration.utility_consumers())
                .ok_or(REGISTER_INVALID_DESCRIPTOR)?;
        let object_access = object_access::prepare_hooks(
            registration.object_access_hooks(),
            registration.object_access_str_hooks(),
        )
        .ok_or(REGISTER_INVALID_DESCRIPTOR)?;
        let planning = planning_hooks::prepare(
            registration.relation_scan_planner(),
            registration.modify_planner(),
        )
        .ok_or(REGISTER_INVALID_DESCRIPTOR)?;
        let table_scan = PendingTableScanRegistration::prepare(
            registration.table_scan(),
            registration.table_scan_worker(),
        )?;
        // Validate bootstrap ownership only after the complete batch has been
        // validated. This preserves the more specific duplicate-provider and
        // invalid-descriptor results while still preventing every registry
        // from being committed outside the bootstrap window.
        let provider = provider_bootstrap::prepare_identity(identity)?;
        Ok(Self {
            table_provider,
            provider,
            utility,
            utility_consumers,
            object_access,
            planning,
            table_scan,
        })
    }

    fn commit(self) {
        // Every registry has finished validation and allocation before any
        // registration becomes visible.
        planning_hooks::commit(self.planning);
        self.table_scan.commit();
        self.table_provider.commit();
        process_utility::commit_hooks(self.utility);
        process_utility::commit_consumers(self.utility_consumers);
        object_access::commit_hooks(self.object_access);
        provider_bootstrap::commit_identity(self.provider);
    }
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn register_provider(
    registration: *const ProviderRegistration<'_>,
) -> u32 {
    // SAFETY: PostgreSQL exposes binary-upgrade state as a backend-global flag.
    if unsafe { pg_sys::IsBinaryUpgrade } {
        return REGISTER_OK;
    }
    // SAFETY: `RuntimeClient::register_provider` passes a live reference for
    // this synchronous call.
    let registration = unsafe { &*registration };
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
    use std::{ptr, slice};

    use lagodb_core::runtime_api::{
        CALLBACK_OK, CallbackErrorReport, MaintenanceReport, MaintenanceRequest,
        MaintenanceStats, OBJECT_ACCESS_DROP, ObjectAccessHookDescriptor,
        ObjectAccessStrHookDescriptor, ProviderIdentity,
        RelationScanPlannerDescriptor, TableProvider, UtilityHookDescriptor,
    };

    use super::*;

    unsafe extern "C-unwind" fn access_method_oid() -> pg_sys::Oid {
        pg_sys::InvalidOid
    }

    unsafe extern "C-unwind" fn execute_maintenance(
        _request: *const MaintenanceRequest,
        _report: *mut MaintenanceReport,
        _error: *mut CallbackErrorReport,
    ) -> u32 {
        CALLBACK_OK
    }

    unsafe extern "C-unwind" fn inspect_maintenance(
        _relation: pg_sys::Relation,
        _stats: *mut MaintenanceStats,
        _error: *mut CallbackErrorReport,
    ) -> u32 {
        CALLBACK_OK
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

    fn table_provider_descriptor(
        name: &'static CStr,
        access_method_name: &'static CStr,
    ) -> TableProvider {
        TableProvider::new(
            name,
            access_method_name,
            None,
            access_method_oid,
            execute_maintenance,
            inspect_maintenance,
        )
    }

    fn identity() -> ProviderIdentity {
        ProviderIdentity::access_method(
            c"runtime-api-test",
            c"lagodb_base",
            c"lagodb_base",
        )
    }

    #[test]
    fn invalid_hook_preparation_does_not_publish_table_provider() {
        let provider =
            table_provider_descriptor(c"atomic-invalid", c"atomic-invalid-am");
        let mut context = 0_u8;
        let invalid_utility = UtilityHookDescriptor {
            tag: pg_sys::NodeTag::T_CommentStmt as u32,
            context: ptr::from_mut(&mut context).cast(),
            on_pre: Some(utility_pre),
            on_post: None,
        };
        let identity = identity();
        let registration = ProviderRegistration::new(&identity)
            .with_table_provider(Some(&provider))
            .with_utility_hooks(slice::from_ref(&invalid_utility));
        assert_eq!(
            PreparedProviderRegistration::prepare(&registration).err(),
            Some(REGISTER_INVALID_DESCRIPTOR)
        );

        let competing =
            table_provider_descriptor(c"atomic-invalid", c"atomic-retry-am");
        let competing = ValidatedTableProvider::new(&competing)
            .expect("competing table provider is valid");
        assert!(
            PreparedTableProviderRegistration::prepare(Some(competing)).is_ok(),
            "failed registration published its prepared table provider"
        );
    }

    #[test]
    fn invalid_planning_facet_does_not_publish_any_prepared_facet() {
        let identity = identity();
        let provider =
            table_provider_descriptor(c"atomic-planning", c"atomic-planning-am");
        let mut context = 0_u8;
        let context = ptr::from_mut(&mut context).cast();
        let utility = UtilityHookDescriptor {
            tag: pg_sys::NodeTag::T_CommentStmt as u32,
            context,
            on_pre: Some(utility_pre),
            on_post: Some(utility_post),
        };
        let object_access = ObjectAccessHookDescriptor {
            event_mask: OBJECT_ACCESS_DROP,
            class_id: pg_sys::InvalidOid,
            context,
            on_access: Some(object_access_callback),
        };
        let object_access_str = ObjectAccessStrHookDescriptor {
            event_mask: OBJECT_ACCESS_DROP,
            class_id: pg_sys::InvalidOid,
            context,
            on_access: Some(object_access_str_callback),
        };
        let invalid_planner = RelationScanPlannerDescriptor {
            context: ptr::null_mut(),
            plan_relation: None,
        };
        let utility_count = process_utility::registered_hook_count();
        let object_access_counts = object_access::registered_hook_counts();
        let registration = ProviderRegistration::new(&identity)
            .with_table_provider(Some(&provider))
            .with_utility_hooks(slice::from_ref(&utility))
            .with_object_access_hooks(
                slice::from_ref(&object_access),
                slice::from_ref(&object_access_str),
            )
            .with_planners(Some(&invalid_planner), None);
        assert_eq!(
            PreparedProviderRegistration::prepare(&registration).err(),
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
        let competing = table_provider_descriptor(
            c"atomic-planning-retry",
            c"atomic-planning-am",
        );
        let competing = ValidatedTableProvider::new(&competing)
            .expect("competing table provider is valid");
        assert!(
            PreparedTableProviderRegistration::prepare(Some(competing)).is_ok(),
            "failed registration published its prepared table provider"
        );
    }
}
