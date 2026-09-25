use pgrx::{PgSqlErrorCode, pg_sys};

use crate::diag::PgReportError;
use crate::handles::RelationHandle;

use super::{
    TableMaintenanceBudget, TableMaintenanceCommandTime, TableMaintenanceError,
    TableMaintenanceMode, TableMaintenanceOptions, TableMaintenanceReport,
    TableMaintenanceStats,
};
use crate::runtime_api::{
    CallbackErrorReport, MaintenanceReport, MaintenanceRequest, MaintenanceStats,
    TableProvider, provider_name,
};
use crate::table_provider::TableProviderRouter;

pub struct TableMaintenanceRequest<'a> {
    pub relation: &'a RelationHandle<'a>,
    pub mode: TableMaintenanceMode,
    pub options: TableMaintenanceOptions,
    pub budget: TableMaintenanceBudget,
    pub command_time: TableMaintenanceCommandTime,
}

pub struct TableMaintenanceRouter;

impl TableMaintenanceRouter {
    #[doc(hidden)]
    pub fn has_providers() -> bool {
        TableProviderRouter::has_providers()
    }

    fn provider_for_am(
        access_method_oid: pg_sys::Oid,
    ) -> Result<&'static TableProvider, TableMaintenanceError> {
        TableProviderRouter::provider_for_am(access_method_oid)
            .map_err(|error| TableMaintenanceError::framework(error.to_string()))?
            .ok_or_else(|| {
                TableMaintenanceError::framework(format!(
                    "no table-maintenance provider is registered for access method OID {access_method_oid}"
                ))
            })
    }

    pub fn is_registered_am(
        access_method_oid: pg_sys::Oid,
    ) -> Result<bool, TableMaintenanceError> {
        TableProviderRouter::is_registered_am(access_method_oid)
            .map_err(|error| TableMaintenanceError::framework(error.to_string()))
    }

    pub fn execute(
        request: TableMaintenanceRequest<'_>,
    ) -> Result<TableMaintenanceReport, TableMaintenanceError> {
        let provider = Self::provider_for_am(request.relation.access_method_oid())?;
        if request.options.analyze && !provider.supports_analyze {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "ANALYZE is not supported by this table maintenance provider",
            )
            .into());
        }
        let wire_request = MaintenanceRequest::new(
            request.relation.as_raw(),
            request.mode,
            request.options,
            request.budget,
            request.command_time,
        );
        let mut report = MaintenanceReport::default();
        let mut error = CallbackErrorReport::default();
        // SAFETY: registration validated this exact-build callback. The locked
        // Relation and both outputs remain live for the synchronous call.
        let status = unsafe {
            (provider.execute_maintenance)(&wire_request, &mut report, &mut error)
        };
        // SAFETY: consume PostgreSQL-owned diagnostics before returning to a
        // caller that can change transactions or reset the memory context.
        unsafe { error.into_result(status, "execute_maintenance") }?;
        Ok(report.into())
    }

    pub fn supports_analyze(
        access_method_oid: pg_sys::Oid,
    ) -> Result<bool, TableMaintenanceError> {
        TableProviderRouter::supports_analyze(access_method_oid)
            .map_err(|error| TableMaintenanceError::framework(error.to_string()))
    }

    pub fn inspect(
        relation: &RelationHandle<'_>,
    ) -> Result<TableMaintenanceStats, TableMaintenanceError> {
        let provider = Self::provider_for_am(relation.access_method_oid())?;
        // SAFETY: runtime only returns descriptors accepted from the trusted
        // core SDK registration path and owns their copied names.
        let name = unsafe { provider_name(provider) }
            .ok_or_else(|| TableMaintenanceError::framework("provider has no name"))?
            .to_string_lossy()
            .into_owned();
        let mut stats = MaintenanceStats::default();
        let mut error = CallbackErrorReport::default();
        // SAFETY: the exact-build callback borrows this live Relation and
        // initializes caller-owned output/error records synchronously.
        let status = unsafe {
            (provider.inspect_maintenance)(relation.as_raw(), &mut stats, &mut error)
        };
        // SAFETY: the callback's PostgreSQL-owned payload is still live here.
        unsafe { error.into_result(status, "inspect_maintenance") }?;
        Ok(stats.into_stats(name))
    }
}
