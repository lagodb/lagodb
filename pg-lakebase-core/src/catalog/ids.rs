use crate::diag::PgError;
use pgrx::pg_sys;
use std::ffi::CStr;
use std::sync::OnceLock;

/// The schema name where lakebase objects are stored.
pub const LAKEBASE_SCHEMA: &CStr = c"lakebase";

/// The table name for storing custom table options.
pub const TABLE_OPTIONS_TABLE: &CStr = c"table_options";
pub const TABLE_OPTIONS_PKEY: &CStr = c"table_options_pkey";
const MAINTENANCE_QUEUE_TABLE: &CStr = c"maintenance_queue";
const MAINTENANCE_QUEUE_PKEY: &CStr = c"maintenance_queue_pkey";
const MAINTENANCE_QUEUE_READY_INDEX: &CStr = c"maintenance_queue_ready_idx";
const MAINTENANCE_QUEUE_TARGET_INDEX: &CStr = c"maintenance_queue_target_idx";

static LAKEBASE_NAMESPACE_OID: OnceLock<pg_sys::Oid> = OnceLock::new();
static TABLE_OPTIONS_OID: OnceLock<pg_sys::Oid> = OnceLock::new();
static TABLE_OPTIONS_PKEY_OID: OnceLock<pg_sys::Oid> = OnceLock::new();
static MAINTENANCE_CATALOG_IDS: OnceLock<MaintenanceCatalogIds> = OnceLock::new();

#[derive(Clone, Copy, Debug)]
pub(crate) struct MaintenanceCatalogIds {
    pub(crate) table: pg_sys::Oid,
    pub(crate) pkey: pg_sys::Oid,
    pub(crate) ready_index: pg_sys::Oid,
    pub(crate) target_index: pg_sys::Oid,
}

pub fn get_lakebase_namespace_oid() -> Result<pg_sys::Oid, PgError> {
    if let Some(&oid) = LAKEBASE_NAMESPACE_OID.get() {
        return Ok(oid);
    }

    let oid = super::get_namespace_oid(LAKEBASE_SCHEMA, false)?;
    let _ = LAKEBASE_NAMESPACE_OID.set(oid);

    Ok(oid)
}

pub fn get_table_options_oid() -> Result<pg_sys::Oid, PgError> {
    if let Some(&oid) = TABLE_OPTIONS_OID.get() {
        return Ok(oid);
    }

    let schema_oid = get_lakebase_namespace_oid()?;
    let oid = super::get_relation_oid(TABLE_OPTIONS_TABLE, schema_oid)?;

    let _ = TABLE_OPTIONS_OID.set(oid);

    Ok(oid)
}

pub fn get_table_options_pkey_oid() -> Result<pg_sys::Oid, PgError> {
    if let Some(&oid) = TABLE_OPTIONS_PKEY_OID.get() {
        return Ok(oid);
    }

    let schema_oid = get_lakebase_namespace_oid()?;
    let oid = super::get_relation_oid(TABLE_OPTIONS_PKEY, schema_oid)?;

    let _ = TABLE_OPTIONS_PKEY_OID.set(oid);

    Ok(oid)
}

/// Resolve the maintenance catalog only after its extension SQL is installed.
/// Missing results are deliberately not cached so a preload worker can recover
/// after `CREATE EXTENSION` in the configured database.
pub(crate) fn get_maintenance_catalog_ids()
-> Result<Option<MaintenanceCatalogIds>, PgError> {
    if let Some(ids) = MAINTENANCE_CATALOG_IDS.get() {
        return Ok(Some(*ids));
    }

    let schema = super::get_namespace_oid(LAKEBASE_SCHEMA, true)?;
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
    let _ = MAINTENANCE_CATALOG_IDS.set(ids);
    Ok(Some(ids))
}
