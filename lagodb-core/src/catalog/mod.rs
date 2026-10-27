//! Catalog access helpers and LagoDB catalog object IDs.

mod access;
mod database;
mod foreign_config;
mod foreign_server;
mod ids;
mod relation;
mod syscache;
mod type_dependencies;

pub use access::{
    CatalogOrderedScan, CatalogRelation, CatalogScan, CatalogScanKey,
    CatalogSnapshot, CatalogUpdateResult, CatalogWriter,
};
pub use database::LockedDatabase;
pub use foreign_config::{
    ForeignConfigInvalidation, ForeignConfigInvalidationKeys,
    register_foreign_config_invalidation_observer,
};
pub use foreign_server::ForeignServerDdlTarget;
pub use ids::{
    LAGODB_SCHEMA, TABLE_OPTIONS_PKEY, TABLE_OPTIONS_TABLE, get_lagodb_namespace_oid,
    get_table_options_oid, get_table_options_pkey_oid,
};
pub(crate) use ids::{MaintenanceCatalogIds, get_maintenance_catalog_ids};
pub use relation::{RelationCatalogEntry, RenameColumnTarget};
pub use syscache::{
    SysCacheTuple, SysCacheTupleCopy, search_syscache_copy, search_syscache1,
};
pub use type_dependencies::TypeDependencies;

use crate::diag::{PgError, PgReportError};
use crate::wrapper::PgWrapper;
use pgrx::pg_sys;
use std::ffi::CStr;

pub fn get_namespace_oid(
    nspname: &CStr,
    missing_ok: bool,
) -> Result<pg_sys::Oid, PgError> {
    PgWrapper::get_namespace_oid(nspname, missing_ok)
}

pub fn current_database_name_bytes() -> Vec<u8> {
    PgWrapper::current_database_name_bytes()
}

pub fn get_namespace_name_bytes(nspid: pg_sys::Oid) -> Option<Vec<u8>> {
    PgWrapper::get_namespace_name_bytes(nspid)
}

pub fn database_path(
    database_oid: pg_sys::Oid,
    tablespace_oid: pg_sys::Oid,
) -> String {
    PgWrapper::database_path(database_oid, tablespace_oid)
}

pub fn get_relation_oid(relname: &CStr, relnamespace: pg_sys::Oid) -> pg_sys::Oid {
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

/// Resolve and lock an ALTER TABLE target with PostgreSQL's own permission
/// callback. `InvalidOid` means an `IF EXISTS` target is missing. Admission
/// errors keep PostgreSQL's SQLSTATE, message, DETAIL and HINT; an outer hook
/// may add its own context.
///
/// # Safety
///
/// `stmt` must be a live ALTER TABLE statement.
pub unsafe fn alter_table_lookup_relation(
    stmt: *mut pg_sys::AlterTableStmt,
    lockmode: pg_sys::LOCKMODE,
) -> Result<pg_sys::Oid, PgReportError> {
    unsafe { PgWrapper::alter_table_lookup_relation(stmt, lockmode) }
        .map_err(PgReportError::from_pg_error)
}

/// Whether a relation has inheritance children, without locking any of them.
///
/// This is tablecmds.c's own existence check. Unlike `relhassubclass` it does
/// not outlive the last child. The caller must hold a lock on the parent that
/// keeps new children out.
pub fn has_inheritance_children(parent_rel_id: pg_sys::Oid) -> Result<bool, PgError> {
    PgWrapper::has_inheritance_children(parent_rel_id)
}

/// Whether a relation inherits from another, without locking its parents.
///
/// The caller must hold a lock on the relation that keeps new parents out.
pub fn has_inheritance_parents(relation_id: pg_sys::Oid) -> Result<bool, PgError> {
    PgWrapper::has_inheritance_parents(relation_id)
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
