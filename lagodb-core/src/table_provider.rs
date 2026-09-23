//! Runtime registration for one table access-method provider.
//!
//! The descriptor published here owns the AM identity, partitioned table
//! semantics, and operations shared by COPY, partitioned table DDL, and maintenance.
//! Command-specific routers remain in their respective modules and consume
//! this one descriptor.

use std::ffi::CStr;
use std::mem::size_of;

use pgrx::pg_sys;

use crate::api::{AmResult, unsupported_callback};
use crate::diag::{PgReportError, ReportableError};
use crate::handles::RelationHandle;
use crate::hooks::{HookRegistrationError, freeze_hooks_with_provider};
use crate::runtime_api::{
    CallbackErrorReport, MaintenanceReport, MaintenanceRequest, MaintenanceStats,
    ProviderIdentity, RuntimeApiError, RuntimeClient, RuntimeRegistrationError,
    TableProvider,
};
use crate::table_maintenance::{
    TableMaintenanceCommandTime, TableMaintenanceError, TableMaintenanceReport,
    TableMaintenanceRequest, TableMaintenanceStats,
};

/// All runtime-visible facets owned by one table access-method provider.
pub trait LagodbTableProvider: 'static {
    const NAME: &'static CStr;
    const EXTENSION_NAME: &'static CStr;
    const LIBRARY_NAME: &'static CStr;
    const ACCESS_METHOD_NAME: &'static CStr;

    /// Whether the table-AM callbacks provide a valid PostgreSQL ANALYZE sample.
    const SUPPORTS_ANALYZE: bool = false;

    /// Whether this AM owns a PostgreSQL partitioned table as one logical table.
    ///
    /// This declaration applies only to partitioned relations matching this
    /// provider's AM; registration alone does not claim utility statements.
    /// Providers must keep these tables out of PostgreSQL partition/inheritance
    /// relationships. SQL TRUNCATE admission inspects explicitly named targets,
    /// while the executor owns target expansion and locked reclassification.
    const OWNS_PARTITIONED_TABLE: bool = false;

    fn access_method_oid() -> Option<pg_sys::Oid>;

    /// Execute TRUNCATE for a provider-owned partitioned table.
    ///
    /// The runtime calls this only after PostgreSQL has completed permission,
    /// dependency, activity, and BEFORE-trigger processing.
    fn truncate_partitioned_table(rel: &RelationHandle<'_>) -> AmResult<()> {
        let _ = rel;
        unsupported_callback("partitioned table truncate")
    }

    /// Execute maintenance, rejecting unsupported options before storage changes.
    fn execute_maintenance(
        request: TableMaintenanceRequest<'_>,
    ) -> Result<TableMaintenanceReport, TableMaintenanceError>;

    fn inspect_maintenance(
        relation: &RelationHandle<'_>,
    ) -> Result<TableMaintenanceStats, TableMaintenanceError>;
}

/// Provider-neutral lookup and relation routing for table AM descriptors.
pub struct TableProviderRouter;

impl TableProviderRouter {
    #[doc(hidden)]
    pub fn has_providers() -> bool {
        RuntimeClient::connect().is_ok_and(RuntimeClient::has_providers)
    }

    pub(crate) fn provider_for_am(
        access_method_oid: pg_sys::Oid,
    ) -> Result<Option<&'static TableProvider>, RuntimeApiError> {
        Ok(RuntimeClient::connect()?.provider_for_am(access_method_oid))
    }

    pub fn is_registered_am(
        access_method_oid: pg_sys::Oid,
    ) -> Result<bool, RuntimeApiError> {
        match Self::provider_for_am(access_method_oid) {
            Ok(provider) => Ok(provider.is_some()),
            Err(RuntimeApiError::Unavailable) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub fn owns_partitioned_table(
        access_method_oid: pg_sys::Oid,
    ) -> Result<bool, RuntimeApiError> {
        match Self::provider_for_am(access_method_oid) {
            Ok(Some(provider)) => Ok(provider.owns_partitioned_table),
            Ok(None) | Err(RuntimeApiError::Unavailable) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub fn supports_analyze(
        access_method_oid: pg_sys::Oid,
    ) -> Result<bool, RuntimeApiError> {
        match Self::provider_for_am(access_method_oid) {
            Ok(Some(provider)) => Ok(provider.supports_analyze),
            Ok(None) | Err(RuntimeApiError::Unavailable) => Ok(false),
            Err(error) => Err(error),
        }
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn provider_access_method_oid<P>() -> pg_sys::Oid
where
    P: LagodbTableProvider,
{
    P::access_method_oid().unwrap_or(pg_sys::InvalidOid)
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn provider_truncate_partitioned_table<P>(
    relation: pg_sys::Relation,
) where
    P: LagodbTableProvider,
{
    // SAFETY: the runtime invokes this callback only with the live, locked
    // partitioned table Relation supplied by its TRUNCATE bridge.
    let relation = unsafe { RelationHandle::from_raw(relation) };
    P::truncate_partitioned_table(&relation).report_unwrap();
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
        let result = P::execute_maintenance(TableMaintenanceRequest {
            relation: &relation,
            mode: request.mode,
            options: request.options(),
            budget: request.budget(),
            command_time: TableMaintenanceCommandTime::from_unix_epoch_ms(
                request.command_time_ms,
            ),
        })
        .map_err(|error| PgReportError::from(error.with_provider(P::NAME)))?;
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
            .map_err(|error| PgReportError::from(error.with_provider(P::NAME)))?;
        let inspected =
            MaintenanceStats::try_from_stats(inspected).ok_or_else(|| {
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
    let descriptor = TableProvider {
        struct_size: u32::try_from(size_of::<TableProvider>())
            .expect("table-provider descriptor size exceeds u32"),
        name: P::NAME.as_ptr(),
        access_method_name: P::ACCESS_METHOD_NAME.as_ptr(),
        owns_partitioned_table: P::OWNS_PARTITIONED_TABLE,
        supports_analyze: P::SUPPORTS_ANALYZE,
        access_method_oid: provider_access_method_oid::<P>,
        truncate_partitioned_table: provider_truncate_partitioned_table::<P>,
        execute_maintenance: provider_execute_maintenance::<P>,
        inspect_maintenance: provider_inspect_maintenance::<P>,
    };
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
