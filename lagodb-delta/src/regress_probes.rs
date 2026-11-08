//! Backend-only probes for the cross-DSO runtime regression suite.

use std::cell::Cell;

use lagodb_core::hooks::{
    OBJECT_ACCESS_DROP, ObjectAccessEvent, ObjectAccessFilter, ObjectAccessHook,
    ObjectAccessHookError, PostUtilityContext, UtilityHook, UtilityHookError,
    UtilityNode,
};
use lagodb_core::runtime_api::{
    CALLBACK_OK, CallbackErrorReport, MaintenanceReport, MaintenanceRequest,
    MaintenanceStats, ProviderIdentity, ProviderRegistration, RuntimeClient,
    RuntimeRegistrationError, TableProvider,
};
use pgrx::prelude::*;

thread_local! {
    static OBJECT_DROP_COUNT: Cell<u64> = const { Cell::new(0) };
    static UTILITY_PRE_COUNT: Cell<u64> = const { Cell::new(0) };
    static UTILITY_POST_COUNT: Cell<u64> = const { Cell::new(0) };
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
    lagodb_core::hooks::register_object_access_hook(Box::new(
        DeltaObjectAccessTestHook,
    ));
    lagodb_core::hooks::register_utility_hook(
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
    _error: *mut CallbackErrorReport,
) -> u32 {
    CALLBACK_OK
}

unsafe extern "C-unwind" fn duplicate_inspect(
    _relation: pg_sys::Relation,
    _stats: *mut MaintenanceStats,
    _error: *mut CallbackErrorReport,
) -> u32 {
    CALLBACK_OK
}

#[pg_schema]
mod delta {
    use super::*;

    /// Ask the runtime to register a distinct provider for Iceberg.
    #[pg_extern]
    fn duplicate_iceberg_registration_rejected() -> bool {
        let runtime =
            RuntimeClient::connect().expect("runtime API must be published");
        let descriptor = TableProvider::new(
            c"delta-duplicate",
            c"iceberg",
            None,
            duplicate_am_oid,
            duplicate_execute,
            duplicate_inspect,
        );
        let identity = ProviderIdentity::access_method(
            c"delta-duplicate",
            c"lagodb_delta",
            c"lagodb_delta",
        );
        let registration = ProviderRegistration::new(&identity)
            .with_table_provider(Some(&descriptor));
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
