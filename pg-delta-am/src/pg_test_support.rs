//! Backend-only probes for the cross-DSO runtime regression suite.

use pg_lakebase_core::hooks::{
    OBJECT_ACCESS_DROP, ObjectAccessEvent, ObjectAccessFilter, ObjectAccessHook,
    ObjectAccessHookError, PostUtilityContext, UtilityHook, UtilityHookError,
    UtilityNode,
};
use pg_lakebase_core::runtime_api::{
    MaintenanceProvider, MaintenanceReport, MaintenanceRequest, MaintenanceStats,
    ProviderIdentity, ProviderRegistration, RuntimeClient, RuntimeRegistrationError,
};
use pgrx::prelude::*;

thread_local! {
    static OBJECT_DROP_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static UTILITY_PRE_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static UTILITY_POST_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

struct DeltaObjectAccessTestHook;

impl ObjectAccessHook for DeltaObjectAccessTestHook {
    fn filter(&self) -> ObjectAccessFilter {
        ObjectAccessFilter::new(OBJECT_ACCESS_DROP)
            .for_class(pg_sys::RelationRelationId)
    }

    fn on_access(
        &self,
        event: &mut ObjectAccessEvent<'_>,
    ) -> Result<(), ObjectAccessHookError> {
        if matches!(event, ObjectAccessEvent::Drop { sub_id, .. } if *sub_id == 0) {
            OBJECT_DROP_COUNT.set(OBJECT_DROP_COUNT.get() + 1);
        }
        Ok(())
    }
}

struct DeltaUtilityTestHook;

impl UtilityHook for DeltaUtilityTestHook {
    fn on_pre(&self, _stmt: &mut UtilityNode) -> Result<(), UtilityHookError> {
        UTILITY_PRE_COUNT.set(UTILITY_PRE_COUNT.get() + 1);
        Ok(())
    }

    fn on_post(&self, _context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        UTILITY_POST_COUNT.set(UTILITY_POST_COUNT.get() + 1);
        Ok(())
    }
}

pub(super) fn init_hooks() {
    pg_lakebase_core::hooks::register_object_access_hook(Box::new(
        DeltaObjectAccessTestHook,
    ));
    pg_lakebase_core::hooks::register_utility_hook(
        pg_sys::NodeTag::T_CommentStmt,
        Box::new(DeltaUtilityTestHook),
    );
}

unsafe extern "C-unwind" fn duplicate_am_oid() -> pg_sys::Oid {
    pg_sys::InvalidOid
}

unsafe extern "C-unwind" fn duplicate_execute(
    _request: *const MaintenanceRequest,
    _report: *mut MaintenanceReport,
) {
}

unsafe extern "C-unwind" fn duplicate_inspect(
    _relation: pg_sys::Relation,
    _stats: *mut MaintenanceStats,
) {
}

#[pg_schema]
mod delta {
    use super::*;

    /// Ask the runtime to register a distinct provider for Iceberg.
    #[pg_extern]
    fn duplicate_iceberg_registration_rejected() -> bool {
        let runtime =
            RuntimeClient::connect().expect("runtime API must be published");
        let descriptor = MaintenanceProvider {
            struct_size: u32::try_from(std::mem::size_of::<MaintenanceProvider>())
                .expect("maintenance provider descriptor size exceeds u32"),
            name: c"delta-duplicate".as_ptr(),
            access_method_name: c"iceberg".as_ptr(),
            capability_flags: 0,
            access_method_oid: duplicate_am_oid,
            execute: duplicate_execute,
            inspect: duplicate_inspect,
        };
        let identity = ProviderIdentity::access_method(
            c"delta-duplicate",
            c"pg_delta_am",
            c"pg_delta_am",
        );
        let registration = ProviderRegistration {
            struct_size: u32::try_from(std::mem::size_of::<ProviderRegistration>())
                .expect("provider registration size exceeds u32"),
            provider: &identity,
            maintenance_provider: &descriptor,
            utility_hooks: std::ptr::null(),
            utility_hook_count: 0,
            utility_consumers: std::ptr::null(),
            utility_consumer_count: 0,
            object_access_hooks: std::ptr::null(),
            object_access_hook_count: 0,
            object_access_str_hooks: std::ptr::null(),
            object_access_str_hook_count: 0,
        };
        // SAFETY: this test registration uses current ABI values backed by
        // local descriptors that remain live for the synchronous call. The
        // duplicate registration is rejected and publishes no context.
        let result = unsafe { runtime.register_provider(&registration) };
        result == Err(RuntimeRegistrationError::DuplicateAccessMethod)
    }

    #[pg_extern]
    fn object_access_drop_count() -> i64 {
        i64::try_from(OBJECT_DROP_COUNT.get()).expect("test count fits i64")
    }

    #[pg_extern]
    fn utility_hook_counts()
    -> TableIterator<'static, (name!(pre_count, i64), name!(post_count, i64))> {
        TableIterator::new(std::iter::once((
            i64::try_from(UTILITY_PRE_COUNT.get()).expect("test count fits i64"),
            i64::try_from(UTILITY_POST_COUNT.get()).expect("test count fits i64"),
        )))
    }
}
