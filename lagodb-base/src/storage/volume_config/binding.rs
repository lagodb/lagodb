//! Admission checks for immutable tablespace bindings.
//!
//! Administrative invariant: while creating a storage volume, the administrator
//! must keep the target tablespace free of concurrent access, including relation
//! and database placement and tablespace DDL, until the command's transaction has
//! ended. Binding admission relies on this guarantee; it does not serialize those
//! operations with tablespace object locks or a pg_tablespace catalog write barrier.
//! The target must still be an empty, dedicated user tablespace.
//!
//! Once created, the volume-to-tablespace binding is immutable. The configuration
//! is machine-managed; editing its tablespace OID is not a supported rebinding
//! operation, and reading or reloading the file does not admit a new binding.

use std::ffi::CString;
use std::fs;
use std::path::Path;

use lagodb_core::catalog::{
    CatalogRelation, CatalogScanKey, CatalogSnapshot, database_path,
    get_relation_oid, get_tablespace_oid,
};
use lagodb_core::diag::PgError;
use lagodb_core::wal::Checkpoint;
use pgrx::pg_sys;

use super::error::StorageVolumeError;

pub(crate) struct TablespaceBindingAdmission {
    oid: pg_sys::Oid,
}

impl TablespaceBindingAdmission {
    pub(crate) fn resolve(name: &str) -> Result<Self, StorageVolumeError> {
        let name = CString::new(name).map_err(PgError::from)?;
        let oid = get_tablespace_oid(&name, false)?;
        if oid == pg_sys::DEFAULTTABLESPACE_OID || oid == pg_sys::GLOBALTABLESPACE_OID
        {
            return Err(StorageVolumeError::BuiltinTablespace);
        }
        Ok(Self { oid })
    }

    pub(crate) const fn oid(&self) -> pg_sys::Oid {
        self.oid
    }

    pub(crate) fn ensure_empty(&self) -> Result<(), StorageVolumeError> {
        let namespace = pg_sys::Oid::from(pg_sys::PG_CATALOG_NAMESPACE);
        // The same reference index used by PostgreSQL's shared-dependency check
        // covers storage-less relations in every database, including templates.
        let dependencies = CatalogRelation::open(
            get_relation_oid(c"pg_shdepend", namespace),
            pg_sys::AccessShareLock as _,
        )?;
        let mut scan = dependencies.begin_scan(
            get_relation_oid(c"pg_shdepend_reference_index", namespace),
            true,
            CatalogSnapshot::Default,
            [
                CatalogScanKey::oid_eq(5, pg_sys::TableSpaceRelationId),
                CatalogScanKey::oid_eq(6, self.oid),
            ],
        )?;
        if scan.get_next()?.is_some() {
            return Err(StorageVolumeError::TablespaceInUse);
        }
        let databases = CatalogRelation::open(
            pg_sys::DatabaseRelationId,
            pg_sys::AccessShareLock as _,
        )?;
        let mut scan = databases.begin_scan(
            pg_sys::InvalidOid,
            false,
            CatalogSnapshot::Default,
            [CatalogScanKey::oid_eq(
                pg_sys::Anum_pg_database_dattablespace as _,
                self.oid,
            )],
        )?;
        if scan.get_next()?.is_some() {
            return Err(StorageVolumeError::TablespaceInUse);
        }
        if !self.is_physically_empty()? {
            // mdunlink keeps a dropped relation's first segment until the next
            // checkpoint. Match DropTableSpace: let PG finish pending unlinks,
            // then recheck without guessing whether an empty file is expendable.
            Checkpoint::immediate_and_wait()?;
            if !self.is_physically_empty()? {
                return Err(StorageVolumeError::TablespaceInUse);
            }
        }
        Ok(())
    }

    fn is_physically_empty(&self) -> Result<bool, StorageVolumeError> {
        // Follow PostgreSQL's pg_tablespace_databases convention: empty database
        // directories are harmless; any entry inside one is storage in use.
        // database_path supplies PG's version-specific tablespace directory.
        let db_path = database_path(pg_sys::InvalidOid, self.oid);
        let root = Path::new(&db_path)
            .parent()
            .expect("database path has a parent");
        for entry in fs::read_dir(root).map_err(|error| {
            StorageVolumeError::config_io("inspect tablespace", root, error)
        })? {
            let entry = entry.map_err(|error| {
                StorageVolumeError::config_io("inspect tablespace", root, error)
            })?;
            let path = entry.path();
            let mut files = fs::read_dir(&path).map_err(|error| {
                StorageVolumeError::config_io(
                    "inspect database directory",
                    &path,
                    error,
                )
            })?;
            if let Some(file) = files.next() {
                file.map_err(|error| {
                    StorageVolumeError::config_io(
                        "inspect database directory",
                        &path,
                        error,
                    )
                })?;
                return Ok(false);
            }
        }
        Ok(true)
    }
}
