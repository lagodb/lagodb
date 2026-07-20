use std::ffi::CStr;

use pgrx::pg_sys;

use crate::diag::PgReportError;
use crate::handles::RelationHandle;

use super::{
    TableMaintenanceBudget, TableMaintenanceCommandTime, TableMaintenanceError,
    TableMaintenanceMode, TableMaintenanceOptions, TableMaintenanceReport,
    TableMaintenanceStats,
};
use crate::runtime_api::{
    MAINTENANCE_PROVIDER_VERSION, MaintenanceProviderV1, MaintenanceReportV1,
    MaintenanceRequestV1, MaintenanceStatsV1, PROVIDER_CAPABILITY_ANALYZE,
    RuntimeApiError, RuntimeClient, RuntimeRegistrationError, provider_name,
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
    /// Whether PostgreSQL can obtain a statistically valid ANALYZE sample
    /// through this provider's table-AM callbacks.
    const SUPPORTS_ANALYZE: bool = false;

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
    let inspected =
        MaintenanceStatsV1::try_from_stats(inspected).unwrap_or_else(|| {
            PgReportError::from_domain_error(TableMaintenanceError::framework(
                "provider format name exceeds the maintenance ABI bound",
            ))
            .report()
        });
    unsafe { stats.write(inspected) };
}

/// Register this DSO's maintenance provider and atomically publish its hooks.
///
/// Call this once, after every utility and object-access hook owned by the AM
/// has been added to the core building registries.
pub fn register_provider<P>()
where
    P: LakebaseTableMaintenanceProvider,
{
    let descriptor = MaintenanceProviderV1 {
        abi_version: MAINTENANCE_PROVIDER_VERSION,
        struct_size: u32::try_from(std::mem::size_of::<MaintenanceProviderV1>())
            .expect("maintenance provider descriptor size exceeds u32"),
        name: P::NAME.as_ptr(),
        access_method_name: P::ACCESS_METHOD_NAME.as_ptr(),
        capability_flags: if P::SUPPORTS_ANALYZE {
            PROVIDER_CAPABILITY_ANALYZE
        } else {
            0
        },
        access_method_oid: provider_access_method_oid::<P>,
        execute: provider_execute::<P>,
        inspect: provider_inspect::<P>,
    };
    match crate::hooks::freeze_hooks_with_provider(Some(&descriptor)) {
        Ok(()) => {}
        Err(crate::hooks::HookRegistrationError::Registration(
            RuntimeRegistrationError::DuplicateProviderName,
        )) => panic!(
            "runtime already has a different maintenance provider named {:?}",
            P::NAME
        ),
        Err(crate::hooks::HookRegistrationError::Registration(
            RuntimeRegistrationError::DuplicateAccessMethod,
        )) => panic!(
            "runtime already has a maintenance provider for access method {:?}",
            P::ACCESS_METHOD_NAME
        ),
        Err(error) => panic!("cannot register maintenance provider: {error}"),
    }
}

pub struct TableMaintenanceRouter;

impl TableMaintenanceRouter {
    #[doc(hidden)]
    pub fn has_providers() -> bool {
        RuntimeClient::connect().is_ok_and(RuntimeClient::has_providers)
    }

    fn provider_for_am(
        access_method_oid: pg_sys::Oid,
    ) -> Result<&'static MaintenanceProviderV1, TableMaintenanceError> {
        let runtime = RuntimeClient::connect()
            .map_err(|error| TableMaintenanceError::framework(error.to_string()))?;
        runtime.provider_for_am(access_method_oid).ok_or_else(|| {
            TableMaintenanceError::framework(format!(
                "no table-maintenance provider is registered for access method OID {access_method_oid}"
            ))
        })
    }

    pub fn is_registered_am(
        access_method_oid: pg_sys::Oid,
    ) -> Result<bool, TableMaintenanceError> {
        match RuntimeClient::connect() {
            Ok(runtime) => Ok(runtime.provider_for_am(access_method_oid).is_some()),
            Err(RuntimeApiError::Unavailable) => Ok(false),
            Err(error) => Err(TableMaintenanceError::framework(error.to_string())),
        }
    }

    pub fn supports_analyze(
        access_method_oid: pg_sys::Oid,
    ) -> Result<bool, TableMaintenanceError> {
        let provider = Self::provider_for_am(access_method_oid)?;
        Ok(provider.capability_flags & PROVIDER_CAPABILITY_ANALYZE != 0)
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
        // SAFETY: runtime only returns descriptors accepted from the trusted
        // core SDK registration path and owns their copied names.
        let name = unsafe { provider_name(provider) }
            .ok_or_else(|| TableMaintenanceError::framework("provider has no name"))?
            .to_string_lossy()
            .into_owned();
        let mut stats = MaintenanceStatsV1::default();
        unsafe { (provider.inspect)(relation.as_raw(), &mut stats) };
        Ok(stats.into_stats(name))
    }
}
