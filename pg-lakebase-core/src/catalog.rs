use crate::pg_wrapper::{PgWrapper, PgWrapperError};
use pgrx::pg_sys;
use std::ffi::CStr;
use std::sync::OnceLock;

/// The schema name where lakebase objects are stored.
pub const LAKEBASE_SCHEMA: &CStr = c"lakebase";

/// The table name for storing custom table options.
pub const TABLE_OPTIONS_TABLE: &CStr = c"table_options";
pub const TABLE_OPTIONS_PKEY: &CStr = c"table_options_pkey";

static LAKEBASE_NAMESPACE_OID: OnceLock<pg_sys::Oid> = OnceLock::new();
static TABLE_OPTIONS_OID: OnceLock<pg_sys::Oid> = OnceLock::new();
static TABLE_OPTIONS_PKEY_OID: OnceLock<pg_sys::Oid> = OnceLock::new();

pub fn get_lakebase_namespace_oid() -> Result<pg_sys::Oid, PgWrapperError> {
    if let Some(&oid) = LAKEBASE_NAMESPACE_OID.get() {
        return Ok(oid);
    }

    let oid = PgWrapper::get_namespace_oid(LAKEBASE_SCHEMA, false)?;
    let _ = LAKEBASE_NAMESPACE_OID.set(oid);

    Ok(oid)
}

pub fn get_table_options_oid() -> Result<pg_sys::Oid, PgWrapperError> {
    if let Some(&oid) = TABLE_OPTIONS_OID.get() {
        return Ok(oid);
    }

    let schema_oid = get_lakebase_namespace_oid()?;
    let oid = PgWrapper::get_relname_relid(TABLE_OPTIONS_TABLE, schema_oid)?;

    let _ = TABLE_OPTIONS_OID.set(oid);

    Ok(oid)
}

pub fn get_table_options_pkey_oid() -> Result<pg_sys::Oid, PgWrapperError> {
    if let Some(&oid) = TABLE_OPTIONS_PKEY_OID.get() {
        return Ok(oid);
    }

    let schema_oid = get_lakebase_namespace_oid()?;
    let oid = PgWrapper::get_relname_relid(TABLE_OPTIONS_PKEY, schema_oid)?;

    let _ = TABLE_OPTIONS_PKEY_OID.set(oid);

    Ok(oid)
}
