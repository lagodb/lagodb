//! Cluster-visible inventory used by database lifecycle guards.

use std::{ffi::c_char, fs, io};

use lagodb_core::catalog::{
    CatalogRelation, CatalogScanKey, CatalogSnapshot, database_path, get_relation_oid,
};
use lagodb_core::hooks::{HookError, UtilityHookError};
use lagodb_core::options::is_distributed_tablespace;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::managed_table::catalog::local_storage::LOCAL_TABLE_SUFFIX;

const SHDEPEND_REFCLASSID: pg_sys::AttrNumber = 5;
const SHDEPEND_REFOBJID: pg_sys::AttrNumber = 6;

/// Fixed-width portion of PostgreSQL's `FormData_pg_shdepend`.
///
/// pgrx does not expose this catalog form, so keep the layout in sync with
/// `catalog/pg_shdepend.h`. None of these fixed catalog fields are nullable.
#[repr(C)]
struct SharedDependencyForm {
    dbid: pg_sys::Oid,
    classid: pg_sys::Oid,
    _objid: pg_sys::Oid,
    _objsubid: i32,
    _refclassid: pg_sys::Oid,
    _refobjid: pg_sys::Oid,
    deptype: c_char,
}

/// Storage owned by managed tables in one PostgreSQL database.
///
/// Local ownership is represented by relation-file Iceberg directories in
/// each local tablespace. Object-backed ownership is represented by native
/// `pg_shdepend` tablespace dependencies, which are cluster-visible and are
/// removed transactionally with their relations.
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
        if object_tablespaces.is_empty() {
            return Ok(false);
        }

        let catalog_namespace = pg_sys::Oid::from(pg_sys::PG_CATALOG_NAMESPACE);
        let relation_oid = get_relation_oid(c"pg_shdepend", catalog_namespace);
        let index_oid =
            get_relation_oid(c"pg_shdepend_reference_index", catalog_namespace);
        let dependencies = CatalogRelation::open(
            relation_oid,
            pg_sys::AccessShareLock as pg_sys::LOCKMODE,
        )?;

        // Volume association is resolved first. The reference index then
        // limits each scan to one object-backed tablespace rather than
        // walking every shared dependency in the source database.
        for tablespace_oid in object_tablespaces {
            let mut scan = dependencies.begin_scan(
                index_oid,
                true,
                CatalogSnapshot::Default,
                [
                    CatalogScanKey::oid_eq(
                        SHDEPEND_REFCLASSID,
                        pg_sys::TableSpaceRelationId,
                    ),
                    CatalogScanKey::oid_eq(SHDEPEND_REFOBJID, *tablespace_oid),
                ],
            )?;
            while let Some(tuple) = scan.get_next()? {
                // SAFETY: `tuple` comes from the live pg_shdepend scan. Its
                // fixed-width, non-null fields remain valid until the scan
                // advances.
                let dependency = unsafe {
                    &*(pg_sys::GETSTRUCT(tuple.as_raw())
                        as *const SharedDependencyForm)
                };
                if dependency.dbid == self.database_oid
                    && dependency.classid == pg_sys::RelationRelationId
                    && dependency.deptype as u32
                        == pg_sys::SharedDependencyType::SHARED_DEPENDENCY_TABLESPACE
                {
                    return Ok(true);
                }
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
            if is_distributed_tablespace(tablespace_oid)? {
                placements.object_backed.push(tablespace_oid);
            } else {
                placements.local.push(tablespace_oid);
            }
        }
        Ok(placements)
    }
}
