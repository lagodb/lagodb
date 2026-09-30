//! PostgreSQL TableAM registration and Iceberg table-provider assembly.

use std::ffi::CStr;

use lagodb_core::prelude::*;
use lagodb_core::table_maintenance::{
    TableMaintenanceError, TableMaintenanceReport, TableMaintenanceRequest,
    TableMaintenanceStats,
};
use lagodb_core::table_provider::LagodbTableProvider;
use pgrx::prelude::*;

use crate::error::IcebergError;

use super::access::index::IcebergIndexFetch;
use super::access::mutation::{IcebergModifyQueryState, IcebergModifyState};
use super::access::scan::IcebergScan;
use super::catalog::IcebergAccessMethod;
use super::constants::ICEBERG_AM_NAME;
use super::maintenance::{IcebergTableMaintenance, MaintenanceExecution};

/// Get the cached Iceberg `TableAmRoutine` pointer.
#[inline]
pub fn get_iceberg_am_routine_ptr() -> *const pg_sys::TableAmRoutine {
    let routine = IcebergTableAm::cached_am_routine();
    &*routine as *const pg_sys::TableAmRoutine
}

#[pg_table_am(
    version = "0.1.0",
    author = "robertmu",
    website = "https://github.com/lagodb/lagodb"
)]
pub struct IcebergTableAm;

/// Runtime descriptor owner for all Iceberg table-provider facets.
pub(crate) struct IcebergTableProvider;

impl TableAccessMethod for IcebergTableAm {
    type ScanSession = IcebergScan;
    type IndexFetchSession = IcebergIndexFetch;
    type ModifyQueryState = IcebergModifyQueryState;
    type ModifyState = IcebergModifyState;
    type CopySession = IcebergModifyState;

    const OWNS_PARTITIONED_TABLE: bool = true;

    fn access_method_oid() -> Option<pg_sys::Oid> {
        IcebergAccessMethod::oid()
    }
}

impl LagodbTableProvider for IcebergTableProvider {
    const OWNS_PARTITIONED_TABLE: bool = IcebergTableAm::OWNS_PARTITIONED_TABLE;
    const NAME: &'static CStr = c"iceberg";
    const EXTENSION_NAME: &'static CStr = c"lagodb_iceberg";
    const LIBRARY_NAME: &'static CStr = c"lagodb_iceberg";
    const ACCESS_METHOD_NAME: &'static CStr = ICEBERG_AM_NAME;
    const SUPPORTS_ANALYZE: bool = true;

    fn access_method_oid() -> Option<pg_sys::Oid> {
        IcebergAccessMethod::oid()
    }

    fn truncate_partitioned_table(rel: &RelationHandle<'_>) -> AmResult<()> {
        IcebergTableAm::truncate(rel)
    }

    fn execute_maintenance(
        request: TableMaintenanceRequest<'_>,
    ) -> Result<TableMaintenanceReport, TableMaintenanceError> {
        match IcebergTableMaintenance::execute(request, None)
            .map_err(TableMaintenanceError::from)?
        {
            MaintenanceExecution::Executed(report) => Ok(report),
            MaintenanceExecution::StaleCandidate => Err(TableMaintenanceError::from(
                IcebergError::InvariantViolated(
                    "explicit Iceberg maintenance cannot have a stale scheduler candidate",
                ),
            )),
        }
    }

    fn inspect_maintenance(
        relation: &RelationHandle<'_>,
    ) -> Result<TableMaintenanceStats, TableMaintenanceError> {
        IcebergTableMaintenance::inspect(relation)
            .map_err(TableMaintenanceError::from)
    }
}
