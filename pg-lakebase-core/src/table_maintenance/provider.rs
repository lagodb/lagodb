use std::ffi::CStr;
use std::marker::PhantomData;
use std::sync::{OnceLock, RwLock};

use pgrx::pg_sys;

use crate::handles::RelationHandle;

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

    fn access_method_oid() -> Option<pg_sys::Oid>;

    fn execute(
        request: TableMaintenanceRequest<'_>,
    ) -> Result<TableMaintenanceReport, TableMaintenanceError>;

    fn inspect(
        relation: &RelationHandle<'_>,
    ) -> Result<TableMaintenanceStats, TableMaintenanceError>;
}

trait ErasedProvider: Sync {
    fn name(&self) -> &'static CStr;
    fn access_method_oid(&self) -> Option<pg_sys::Oid>;
    fn execute(
        &self,
        request: TableMaintenanceRequest<'_>,
    ) -> Result<TableMaintenanceReport, TableMaintenanceError>;
    fn inspect(
        &self,
        relation: &RelationHandle<'_>,
    ) -> Result<TableMaintenanceStats, TableMaintenanceError>;
}

struct ProviderEntry<P: LakebaseTableMaintenanceProvider> {
    _marker: PhantomData<fn() -> P>,
}

impl<P: LakebaseTableMaintenanceProvider> ProviderEntry<P> {
    const fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

unsafe impl<P: LakebaseTableMaintenanceProvider> Sync for ProviderEntry<P> {}

impl<P: LakebaseTableMaintenanceProvider> ErasedProvider for ProviderEntry<P> {
    fn name(&self) -> &'static CStr {
        P::NAME
    }

    fn access_method_oid(&self) -> Option<pg_sys::Oid> {
        P::access_method_oid()
    }

    fn execute(
        &self,
        request: TableMaintenanceRequest<'_>,
    ) -> Result<TableMaintenanceReport, TableMaintenanceError> {
        P::execute(request).map_err(|error| error.with_provider(P::NAME))
    }

    fn inspect(
        &self,
        relation: &RelationHandle<'_>,
    ) -> Result<TableMaintenanceStats, TableMaintenanceError> {
        P::inspect(relation).map_err(|error| error.with_provider(P::NAME))
    }
}

static REGISTRY: OnceLock<RwLock<Vec<&'static dyn ErasedProvider>>> = OnceLock::new();

fn registry() -> &'static RwLock<Vec<&'static dyn ErasedProvider>> {
    REGISTRY.get_or_init(|| RwLock::new(Vec::new()))
}

pub fn register_provider<P: LakebaseTableMaintenanceProvider>() {
    let entry: &'static ProviderEntry<P> =
        Box::leak(Box::new(ProviderEntry::<P>::new()));
    let mut providers = registry()
        .write()
        .expect("table-maintenance provider registry lock poisoned");
    if providers.iter().any(|provider| provider.name() == P::NAME) {
        panic!(
            "table-maintenance provider {:?} is already registered",
            P::NAME
        );
    }
    providers.push(entry);
    #[cfg(feature = "pg17")]
    crate::hooks::utility_hook::install_table_maintenance_router();
}

pub struct TableMaintenanceRouter;

impl TableMaintenanceRouter {
    pub(crate) fn has_providers() -> bool {
        !registry()
            .read()
            .expect("table-maintenance provider registry lock poisoned")
            .is_empty()
    }

    fn provider_for_am(
        access_method_oid: pg_sys::Oid,
    ) -> Result<Option<&'static dyn ErasedProvider>, TableMaintenanceError> {
        let providers = registry()
            .read()
            .expect("table-maintenance provider registry lock poisoned");
        let mut matching = providers
            .iter()
            .copied()
            .filter(|provider| provider.access_method_oid() == Some(access_method_oid));
        let Some(provider) = matching.next() else {
            return Ok(None);
        };
        if matching.next().is_some() {
            return Err(TableMaintenanceError::framework(format!(
                "multiple table-maintenance providers claim access method OID {access_method_oid}"
            )));
        }
        Ok(Some(provider))
    }

    pub fn is_registered_am(
        access_method_oid: pg_sys::Oid,
    ) -> Result<bool, TableMaintenanceError> {
        Ok(Self::provider_for_am(access_method_oid)?.is_some())
    }

    pub fn execute(
        request: TableMaintenanceRequest<'_>,
    ) -> Result<TableMaintenanceReport, TableMaintenanceError> {
        let access_method_oid = request.relation.access_method_oid();
        let provider = Self::provider_for_am(access_method_oid)?.ok_or_else(|| {
            TableMaintenanceError::framework(format!(
                "no table-maintenance provider is registered for access method OID {access_method_oid}"
            ))
        })?;
        provider.execute(request)
    }

    pub fn inspect(
        relation: &RelationHandle<'_>,
    ) -> Result<TableMaintenanceStats, TableMaintenanceError> {
        let access_method_oid = relation.access_method_oid();
        let provider = Self::provider_for_am(access_method_oid)?.ok_or_else(|| {
            TableMaintenanceError::framework(format!(
                "no table-maintenance provider is registered for access method OID {access_method_oid}"
            ))
        })?;
        let mut stats = provider.inspect(relation)?;
        stats.provider = provider.name().to_string_lossy().into_owned();
        Ok(stats)
    }
}
