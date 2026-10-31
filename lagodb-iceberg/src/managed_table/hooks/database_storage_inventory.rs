//! Cluster-visible inventory used by database lifecycle guards.

use std::{fs, io};

use lagodb_core::catalog::{CatalogRelation, CatalogSnapshot, database_path};
use lagodb_core::hooks::{HookError, UtilityHookError};
use lagodb_core::storage::tablespace::TablespaceStorage;
use lagodb_core::wal::Checkpoint;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::managed_table::catalog::local_storage::LOCAL_TABLE_SUFFIX;

/// Storage owned by managed tables in one PostgreSQL database.
///
/// Local ownership is represented by relation-file Iceberg directories in
/// each local tablespace. Object-backed tablespaces contain only object-backed
/// tables: native relation files identify ordinary tables without inspecting
/// their AM.
pub(super) struct ManagedDatabaseInventory {
    database_oid: pg_sys::Oid,
}

impl ManagedDatabaseInventory {
    pub(super) const fn new(database_oid: pg_sys::Oid) -> Self {
        Self { database_oid }
    }

    pub(super) fn contains_any_managed_storage(
        &self,
    ) -> Result<bool, UtilityHookError> {
        let placements = TablespacePlacements::load()?;
        if self.contains_object_storage_in(&placements.object_backed)? {
            return Ok(true);
        }
        for tablespace_oid in placements.local {
            if self.contains_local_storage_in(tablespace_oid)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) fn contains_local_storage_in(
        &self,
        tablespace_oid: pg_sys::Oid,
    ) -> Result<bool, UtilityHookError> {
        let path = database_path(self.database_oid, tablespace_oid);
        let entries = match fs::read_dir(path) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(false);
            }
            Err(error) => return Err(Self::io_error(error)),
        };
        for entry in entries {
            let entry = entry.map_err(Self::io_error)?;
            if entry
                .file_name()
                .to_string_lossy()
                .ends_with(LOCAL_TABLE_SUFFIX)
                && entry.file_type().map_err(Self::io_error)?.is_dir()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn contains_object_storage_in(
        &self,
        object_tablespaces: &[pg_sys::Oid],
    ) -> Result<bool, UtilityHookError> {
        if !self.contains_object_files_in(object_tablespaces)? {
            return Ok(false);
        }
        // Like PG's database FILE_COPY and DROP TABLESPACE paths, finish pending
        // main-fork unlinks before interpreting remaining files as live storage.
        // Otherwise DROP of the last table would block cloning until checkpoint.
        Checkpoint::immediate_and_wait()?;
        self.contains_object_files_in(object_tablespaces)
    }

    fn contains_object_files_in(
        &self,
        object_tablespaces: &[pg_sys::Oid],
    ) -> Result<bool, UtilityHookError> {
        // Match pg_tablespace_databases: a nonempty per-database directory
        // means this database uses the tablespace. No provider-name check.
        for tablespace_oid in object_tablespaces {
            let path = database_path(self.database_oid, *tablespace_oid);
            let mut entries = match fs::read_dir(path) {
                Ok(entries) => entries,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(Self::io_error(error)),
            };
            if entries
                .next()
                .transpose()
                .map_err(Self::io_error)?
                .is_some()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn io_error(error: io::Error) -> UtilityHookError {
        HookError::with_source(PgSqlErrorCode::ERRCODE_IO_ERROR, error)
    }
}

struct TablespacePlacements {
    local: Vec<pg_sys::Oid>,
    object_backed: Vec<pg_sys::Oid>,
}

impl TablespacePlacements {
    fn load() -> Result<Self, UtilityHookError> {
        let relation = CatalogRelation::open(
            pg_sys::TableSpaceRelationId,
            pg_sys::AccessShareLock as pg_sys::LOCKMODE,
        )?;
        let mut scan = relation.begin_scan(
            pg_sys::InvalidOid,
            false,
            CatalogSnapshot::Default,
            [],
        )?;
        let mut placements = Self {
            local: Vec::new(),
            object_backed: Vec::new(),
        };
        while let Some(tuple) = scan.get_next()? {
            // SAFETY: the tuple comes from pg_tablespace and remains valid
            // until the next scan call.
            let tablespace_oid = unsafe {
                (*(pg_sys::GETSTRUCT(tuple.as_raw()) as pg_sys::Form_pg_tablespace))
                    .oid
            };
            if tablespace_oid == pg_sys::GLOBALTABLESPACE_OID {
                continue;
            }
            if TablespaceStorage::is_object_backed(tablespace_oid)? {
                placements.object_backed.push(tablespace_oid);
            } else {
                placements.local.push(tablespace_oid);
            }
        }
        Ok(placements)
    }
}
