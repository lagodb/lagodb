use crate::diag::PgError;
use pgrx::pg_sys;
use std::ffi::CStr;

/// The schema name where LagoDB objects are stored.
pub const LAGODB_SCHEMA: &CStr = c"lagodb";

/// The table name for storing custom table options.
pub const TABLE_OPTIONS_TABLE: &CStr = c"table_options";
pub const TABLE_OPTIONS_PKEY: &CStr = c"table_options_pkey";
const MAINTENANCE_QUEUE_TABLE: &CStr = c"maintenance_queue";
const MAINTENANCE_QUEUE_PKEY: &CStr = c"maintenance_queue_pkey";
const MAINTENANCE_QUEUE_READY_INDEX: &CStr = c"maintenance_queue_ready_idx";
const MAINTENANCE_QUEUE_TARGET_INDEX: &CStr = c"maintenance_queue_target_idx";

#[derive(Clone, Copy, Debug)]
pub(crate) struct MaintenanceCatalogIds {
    pub(crate) table: pg_sys::Oid,
    pub(crate) pkey: pg_sys::Oid,
    pub(crate) ready_index: pg_sys::Oid,
    pub(crate) target_index: pg_sys::Oid,
}

pub fn get_lagodb_namespace_oid() -> Result<pg_sys::Oid, PgError> {
    super::get_namespace_oid(LAGODB_SCHEMA, false)
}

pub fn get_table_options_oid() -> Result<pg_sys::Oid, PgError> {
    let schema_oid = get_lagodb_namespace_oid()?;
    super::get_relation_oid(TABLE_OPTIONS_TABLE, schema_oid)
}

pub fn get_table_options_pkey_oid() -> Result<pg_sys::Oid, PgError> {
    let schema_oid = get_lagodb_namespace_oid()?;
    super::get_relation_oid(TABLE_OPTIONS_PKEY, schema_oid)
}

/// Resolve the maintenance catalog only after its extension SQL is installed.
/// Results are deliberately not cached so a long-lived backend can recover
/// after `DROP EXTENSION lagodb_base;
/// CREATE EXTENSION lagodb_base` installs replacement catalog objects
/// with new OIDs.
pub(crate) fn get_maintenance_catalog_ids()
-> Result<Option<MaintenanceCatalogIds>, PgError> {
    let schema = super::get_namespace_oid(LAGODB_SCHEMA, true)?;
    if schema == pg_sys::InvalidOid {
        return Ok(None);
    }

    let table = super::get_relation_oid(MAINTENANCE_QUEUE_TABLE, schema)?;
    let pkey = super::get_relation_oid(MAINTENANCE_QUEUE_PKEY, schema)?;
    let ready_index = super::get_relation_oid(MAINTENANCE_QUEUE_READY_INDEX, schema)?;
    let target_index =
        super::get_relation_oid(MAINTENANCE_QUEUE_TARGET_INDEX, schema)?;
    if [table, pkey, ready_index, target_index]
        .into_iter()
        .any(|oid| oid == pg_sys::InvalidOid)
    {
        return Ok(None);
    }

    let ids = MaintenanceCatalogIds {
        table,
        pkey,
        ready_index,
        target_index,
    };
    Ok(Some(ids))
}
