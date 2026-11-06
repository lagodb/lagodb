//! Runtime registration for one table access-method provider.
//!
//! The descriptor published here owns the AM identity and the operations
//! shared by COPY and maintenance. Command-specific routers look the
//! descriptor up through [`RuntimeClient::provider_for_am`]; nothing inside
//! the crate depends on this registration module.
//!
//! [`RuntimeClient::provider_for_am`]: crate::runtime_api::RuntimeClient::provider_for_am

mod hierarchy;

use std::ffi::CStr;

use pgrx::pg_sys;

use self::hierarchy::ProviderHierarchyGuard;
use crate::handles::RelationHandle;
use crate::hooks::{HookError, HookRegistrationError, freeze_hooks_with_provider};
use crate::runtime_api::{
    CallbackErrorReport, MaintenanceReport, MaintenanceRequest, MaintenanceStats,
    ProviderIdentity, RuntimeRegistrationError, TableCopyCallbacks, TableProvider,
};
use crate::table_maintenance::{
    TableMaintenanceError, TableMaintenanceReport, TableMaintenanceRequest,
    TableMaintenanceStats,
};

/// All runtime-visible facets owned by one table access-method provider.
///
/// Registration also guards the provider's hierarchies: a provider relation only
/// shares one with relations of the same access method.
pub trait LagodbTableProvider: 'static {
    const NAME: &'static CStr;
    const EXTENSION_NAME: &'static CStr;
    const LIBRARY_NAME: &'static CStr;
    const ACCESS_METHOD_NAME: &'static CStr;

    fn access_method_oid() -> Option<pg_sys::Oid>;

    /// Explicit COPY sessions; None retains native TableAM insertion.
    fn copy_callbacks() -> Option<&'static TableCopyCallbacks> {
        None
    }

    /// Reject a new pg_inherits edge between two of this provider's relations.
    ///
    /// PostgreSQL holds both relations' locks and rolls the DDL back on error.
    /// During CREATE, `child` has no provider state yet: the post-utility hook
    /// has not run.
    fn validate_inheritance_edge(
        _child: pg_sys::Oid,
        _parent: pg_sys::Oid,
    ) -> Result<(), HookError> {
        Ok(())
    }

    /// Execute maintenance, rejecting unsupported options before storage changes.
    fn execute_maintenance(
        request: TableMaintenanceRequest<'_>,
    ) -> Result<TableMaintenanceReport, TableMaintenanceError>;

    fn inspect_maintenance(
        relation: &RelationHandle<'_>,
    ) -> Result<TableMaintenanceStats, TableMaintenanceError>;
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn provider_access_method_oid<P>() -> pg_sys::Oid
where
    P: LagodbTableProvider,
{
    P::access_method_oid().unwrap_or(pg_sys::InvalidOid)
}

unsafe extern "C-unwind" fn provider_execute_maintenance<P>(
    request: *const MaintenanceRequest,
    report: *mut MaintenanceReport,
    error: *mut CallbackErrorReport,
) -> u32
where
    P: LagodbTableProvider,
{
    let operation = || {
        // SAFETY: the exact-build runtime supplies live request/output pointers
        // and holds the Relation open throughout this synchronous callback.
        let request = unsafe { &*request };
        let relation = unsafe { RelationHandle::from_raw(request.relation) };
        let request = TableMaintenanceRequest::from_wire(&relation, request);
        let result = P::execute_maintenance(request)
            .map_err(|error| error.with_provider(P::NAME).into_report_error())?;
        // SAFETY: the runtime owns this writable output for the callback.
        unsafe { report.write(result.into()) };
        Ok(())
    };
    // SAFETY: the runtime supplies a live error record and consumes its
    // PostgreSQL-owned payload before the current memory context is reset.
    unsafe { (&mut *error).capture(operation) }
}

unsafe extern "C-unwind" fn provider_inspect_maintenance<P>(
    relation: pg_sys::Relation,
    stats: *mut MaintenanceStats,
    error: *mut CallbackErrorReport,
) -> u32
where
    P: LagodbTableProvider,
{
    let operation = || {
        // SAFETY: the runtime holds this Relation open and supplies writable
        // output/error records for the synchronous exact-build callback.
        let relation = unsafe { RelationHandle::from_raw(relation) };
        let inspected = P::inspect_maintenance(&relation)
            .map_err(|error| error.with_provider(P::NAME).into_report_error())?;
        let inspected = inspected.to_wire().ok_or_else(|| {
            TableMaintenanceError::framework(
                "provider format name exceeds the maintenance ABI bound",
            )
        })?;
        // SAFETY: the runtime owns this writable output for the callback.
        unsafe { stats.write(inspected) };
        Ok(())
    };
    // SAFETY: the runtime consumes the error payload synchronously while its
    // PostgreSQL memory context remains live.
    unsafe { (&mut *error).capture(operation) }
}

/// Register one table provider and atomically publish all of its staged hooks.
pub fn register_provider<P>()
where
    P: LagodbTableProvider,
{
    ProviderHierarchyGuard::<P>::register();
    let descriptor = TableProvider::new(
        P::NAME,
        P::ACCESS_METHOD_NAME,
        P::copy_callbacks(),
        provider_access_method_oid::<P>,
        provider_execute_maintenance::<P>,
        provider_inspect_maintenance::<P>,
    );
    let identity =
        ProviderIdentity::access_method(P::NAME, P::EXTENSION_NAME, P::LIBRARY_NAME);
    match freeze_hooks_with_provider(&identity, Some(&descriptor)) {
        Ok(()) => {}
        Err(HookRegistrationError::Registration(
            RuntimeRegistrationError::DuplicateProviderName,
        )) => panic!(
            "runtime already has a different table provider named {:?}",
            P::NAME
        ),
        Err(HookRegistrationError::Registration(
            RuntimeRegistrationError::DuplicateAccessMethod,
        )) => panic!(
            "runtime already has a table provider for access method {:?}",
            P::ACCESS_METHOD_NAME
        ),
        Err(error) => panic!("cannot register table provider: {error}"),
    }
}
