use std::ffi::CStr;

use pgrx::pg_sys;

use crate::diag::PgReportError;
use crate::handles::RelationHandle;

use super::abi::{
    MAINTENANCE_PROVIDER_VERSION, MaintenanceProviderV2, MaintenanceReportV1,
    MaintenanceRequestV1, MaintenanceStatsV1, REGISTER_DUPLICATE_ACCESS_METHOD,
    REGISTER_DUPLICATE_NAME, REGISTER_INVALID_DESCRIPTOR, REGISTER_OK,
    provider_name, runtime_api,
};
use super::{
    TableMaintenanceBudget, TableMaintenanceCommandTime, TableMaintenanceError,
    TableMaintenanceMode, TableMaintenanceOptions, TableMaintenanceReport,
    TableMaintenanceStats,
};

pub struct TableMaintenanceRequest<'a> {
    pub relation: &'a RelationHandle<'a>,
    pub mode: TableMaintenanceMode,
    pub options: TableMaintenanceOptions,
    pub budget: TableMaintenanceBudget,
    pub command_time: TableMaintenanceCommandTime,
}

pub trait LakebaseTableMaintenanceProvider: 'static {
    const NAME: &'static CStr;
    const ACCESS_METHOD_NAME: &'static CStr;

    fn access_method_oid() -> Option<pg_sys::Oid>;

    fn execute(
        request: TableMaintenanceRequest<'_>,
    ) -> Result<TableMaintenanceReport, TableMaintenanceError>;

    fn inspect(
        relation: &RelationHandle<'_>,
    ) -> Result<TableMaintenanceStats, TableMaintenanceError>;
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn provider_access_method_oid<P>() -> pg_sys::Oid
where
    P: LakebaseTableMaintenanceProvider,
{
    P::access_method_oid().unwrap_or(pg_sys::InvalidOid)
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn provider_execute<P>(
    request: *const MaintenanceRequestV1,
    report: *mut MaintenanceReportV1,
) where
    P: LakebaseTableMaintenanceProvider,
{
    let request = unsafe { request.as_ref() }.unwrap_or_else(|| {
        PgReportError::from_domain_error(TableMaintenanceError::framework(
            "runtime passed a null maintenance request",
        ))
        .report()
    });
    let mode = request.mode().unwrap_or_else(|| {
        PgReportError::from_domain_error(TableMaintenanceError::framework(
            "runtime passed an unknown maintenance mode",
        ))
        .report()
    });
    if request.relation.is_null() || report.is_null() {
        PgReportError::from_domain_error(TableMaintenanceError::framework(
            "runtime passed a null maintenance ABI pointer",
        ))
        .report();
    }
    let relation = unsafe { RelationHandle::from_raw(request.relation) };
    let result = P::execute(TableMaintenanceRequest {
        relation: &relation,
        mode,
        options: request.options(),
        budget: request.budget(),
        command_time: TableMaintenanceCommandTime::from_unix_epoch_ms(
            request.command_time_ms,
        ),
    })
    .map_err(|error| error.with_provider(P::NAME))
    .unwrap_or_else(|error| PgReportError::from_domain_error(error).report());
    unsafe { report.write(result.into()) };
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn provider_inspect<P>(
    relation: pg_sys::Relation,
    stats: *mut MaintenanceStatsV1,
) where
    P: LakebaseTableMaintenanceProvider,
{
    if relation.is_null() || stats.is_null() {
        PgReportError::from_domain_error(TableMaintenanceError::framework(
            "runtime passed a null inspection ABI pointer",
        ))
        .report();
    }
    let relation = unsafe { RelationHandle::from_raw(relation) };
    let inspected = P::inspect(&relation)
        .map_err(|error| error.with_provider(P::NAME))
        .unwrap_or_else(|error| PgReportError::from_domain_error(error).report());
    let inspected = MaintenanceStatsV1::try_from_stats(inspected).unwrap_or_else(|| {
        PgReportError::from_domain_error(TableMaintenanceError::framework(
            "provider format name exceeds the maintenance ABI bound",
        ))
        .report()
    });
    unsafe { stats.write(inspected) };
}

pub fn register_provider<P>()
where
    P: LakebaseTableMaintenanceProvider,
{
    let api = runtime_api().unwrap_or_else(|| {
        panic!("pg_lakebase runtime API is unavailable; preload pg_lakebase_runtime before provider extensions")
    });
    let descriptor = MaintenanceProviderV2 {
        abi_version: MAINTENANCE_PROVIDER_VERSION,
        struct_size: u32::try_from(std::mem::size_of::<MaintenanceProviderV2>())
            .expect("maintenance provider descriptor size exceeds u32"),
        name: P::NAME.as_ptr(),
        access_method_name: P::ACCESS_METHOD_NAME.as_ptr(),
        access_method_oid: provider_access_method_oid::<P>,
        execute: provider_execute::<P>,
        inspect: provider_inspect::<P>,
    };
    let status = unsafe { (api.register_provider)(&descriptor) };
    match status {
        REGISTER_OK => {}
        REGISTER_INVALID_DESCRIPTOR => panic!("runtime rejected an invalid maintenance provider descriptor"),
        REGISTER_DUPLICATE_NAME => panic!("runtime already has a different maintenance provider named {:?}", P::NAME),
        REGISTER_DUPLICATE_ACCESS_METHOD => panic!(
            "runtime already has a maintenance provider for access method {:?}",
            P::ACCESS_METHOD_NAME
        ),
        other => panic!("runtime returned unknown maintenance registration status {other}"),
    }
}

pub struct TableMaintenanceRouter;

impl TableMaintenanceRouter {
    pub(crate) fn has_providers() -> bool {
        runtime_api().is_some_and(|api| unsafe { (api.has_providers)() != 0 })
    }

    fn provider_for_am(
        access_method_oid: pg_sys::Oid,
    ) -> Result<&'static MaintenanceProviderV2, TableMaintenanceError> {
        let api = runtime_api().ok_or_else(|| {
            TableMaintenanceError::framework("pg_lakebase runtime API is unavailable")
        })?;
        let provider = unsafe { (api.provider_for_am)(access_method_oid) };
        unsafe { provider.as_ref() }.ok_or_else(|| {
            TableMaintenanceError::framework(format!(
                "no table-maintenance provider is registered for access method OID {access_method_oid}"
            ))
        })
    }

    pub fn is_registered_am(
        access_method_oid: pg_sys::Oid,
    ) -> Result<bool, TableMaintenanceError> {
        let Some(api) = runtime_api() else {
            return Ok(false);
        };
        Ok(!unsafe { (api.provider_for_am)(access_method_oid) }.is_null())
    }

    pub fn execute(
        request: TableMaintenanceRequest<'_>,
    ) -> Result<TableMaintenanceReport, TableMaintenanceError> {
        let provider = Self::provider_for_am(request.relation.access_method_oid())?;
        let wire_request = MaintenanceRequestV1::new(
            request.relation.as_raw(),
            request.mode,
            request.options,
            request.budget,
            request.command_time,
        );
        let mut report = MaintenanceReportV1::default();
        unsafe { (provider.execute)(&wire_request, &mut report) };
        Ok(report.into())
    }

    pub fn inspect(
        relation: &RelationHandle<'_>,
    ) -> Result<TableMaintenanceStats, TableMaintenanceError> {
        let provider = Self::provider_for_am(relation.access_method_oid())?;
        let name = provider_name(provider)
            .ok_or_else(|| TableMaintenanceError::framework("provider has no name"))?
            .to_string_lossy()
            .into_owned();
        let mut stats = MaintenanceStatsV1::default();
        unsafe { (provider.inspect)(relation.as_raw(), &mut stats) };
        Ok(stats.into_stats(name))
    }
}
