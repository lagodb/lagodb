//! PostgreSQL TableAM identity and callback type registration.

use pg_lakebase_core::prelude::*;
use pgrx::prelude::*;

use super::access::index::IcebergIndexFetch;
use super::access::mutation::{IcebergModifyQueryState, IcebergModifyState};
use super::access::scan::IcebergScan;
use super::catalog::IcebergAccessMethod;

/// Get the cached Iceberg `TableAmRoutine` pointer.
#[inline]
pub fn get_iceberg_am_routine_ptr() -> *const pg_sys::TableAmRoutine {
    let routine = IcebergTableAm::cached_am_routine();
    &*routine as *const pg_sys::TableAmRoutine
}

#[pg_table_am(
    version = "0.1.0",
    author = "robertmu",
    website = "https://github.com/robertmu/pg-lakebase"
)]
pub struct IcebergTableAm;

impl TableAccessMethod for IcebergTableAm {
    type ScanSession = IcebergScan;
    type IndexFetchSession = IcebergIndexFetch;
    type ModifyQueryState = IcebergModifyQueryState;
    type ModifyState = IcebergModifyState;
    type CopySession = IcebergModifyState;

    fn access_method_oid() -> Option<pg_sys::Oid> {
        IcebergAccessMethod::oid()
    }
}
