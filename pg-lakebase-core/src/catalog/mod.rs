//! Catalog access helpers and LagoDB catalog object IDs.

mod access;
mod ids;
mod syscache;

pub use access::{
    CatalogOrderedScan, CatalogRelation, CatalogScan, CatalogScanKey,
    CatalogSnapshot, CatalogUpdateResult, CatalogWriter,
};
pub use ids::{
    LAGODB_SCHEMA, TABLE_OPTIONS_PKEY, TABLE_OPTIONS_TABLE, get_lagodb_namespace_oid,
    get_table_options_oid, get_table_options_pkey_oid,
};
pub(crate) use ids::{MaintenanceCatalogIds, get_maintenance_catalog_ids};
pub(crate) use syscache::search_syscache2;
pub use syscache::{
    SysCacheTuple, SysCacheTupleCopy, search_syscache_copy, search_syscache1,
};

use crate::diag::PgError;
use crate::wrapper::PgWrapper;
use pgrx::pg_sys;
use std::ffi::CStr;

pub fn get_namespace_oid(
    nspname: &CStr,
    missing_ok: bool,
) -> Result<pg_sys::Oid, PgError> {
    PgWrapper::get_namespace_oid(nspname, missing_ok)
}

pub fn get_namespace_name(nspid: pg_sys::Oid) -> Result<Option<String>, PgError> {
    PgWrapper::get_namespace_name(nspid)
}

pub fn get_relation_oid(
    relname: &CStr,
    relnamespace: pg_sys::Oid,
) -> Result<pg_sys::Oid, PgError> {
    PgWrapper::get_relname_relid(relname, relnamespace)
}

/// Resolves a `RangeVar` to a relation OID.
///
/// # Safety
///
/// `relation` must point to a valid PostgreSQL `RangeVar` for the duration of
/// the call.
pub unsafe fn range_var_get_relid(
    relation: *const pg_sys::RangeVar,
    lockmode: pg_sys::LOCKMODE,
    missing_ok: bool,
) -> Result<pg_sys::Oid, PgError> {
    unsafe { PgWrapper::range_var_get_relid(relation, lockmode, missing_ok) }
}

pub fn find_all_inheritors(
    parent_rel_id: pg_sys::Oid,
    lockmode: pg_sys::LOCKMODE,
) -> Result<Vec<pg_sys::Oid>, PgError> {
    PgWrapper::find_all_inheritors(parent_rel_id, lockmode)
}

pub fn get_tablespace_oid(
    spcname: &CStr,
    missing_ok: bool,
) -> Result<pg_sys::Oid, PgError> {
    PgWrapper::get_tablespace_oid(spcname, missing_ok)
}
