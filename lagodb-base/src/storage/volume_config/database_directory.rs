//! Runtime-owned tablespace policy for PostgreSQL database directories.

use lagodb_core::catalog::{CatalogRelation, CatalogScanKey, CatalogSnapshot};
use lagodb_core::diag::{PgError, SqlStateError};
use lagodb_core::options::{TablespaceCacheError, is_distributed_tablespace};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

#[derive(Debug, thiserror::Error)]
pub(crate) enum DatabaseDirectoryError {
    #[error(transparent)]
    Catalog(#[from] PgError),
    #[error(transparent)]
    Tablespace(#[from] TablespaceCacheError),
    #[error("object-backed tablespaces cannot store PostgreSQL database directories")]
    ObjectBacked,
}

impl SqlStateError for DatabaseDirectoryError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::Catalog(error) => error.sql_error_code(),
            Self::Tablespace(error) => error.sql_error_code(),
            Self::ObjectBacked => PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
        }
    }
}

pub(crate) struct DatabaseDirectoryPolicy;

impl DatabaseDirectoryPolicy {
    /// Reject a PostgreSQL database directory in an object-backed tablespace.
    pub(crate) fn check_tablespace(
        tablespace_oid: pg_sys::Oid,
    ) -> Result<(), DatabaseDirectoryError> {
        if is_distributed_tablespace(tablespace_oid)? {
            return Err(DatabaseDirectoryError::ObjectBacked);
        }
        Ok(())
    }

    /// Enforce the directory storage policy after PostgreSQL's native CREATE
    /// DATABASE validation but before `createdb()` starts copying physical files.
    pub(crate) fn on_object_access(
        access: pg_sys::ObjectAccessType::Type,
        class_id: pg_sys::Oid,
        object_id: pg_sys::Oid,
        sub_id: i32,
    ) -> Result<(), DatabaseDirectoryError> {
        if access != pg_sys::ObjectAccessType::OAT_POST_CREATE
            || class_id != pg_sys::DatabaseRelationId
            || sub_id != 0
        {
            return Ok(());
        }

        Self::check_tablespace(Self::created_database_tablespace(object_id)?)
    }

    fn created_database_tablespace(
        database_oid: pg_sys::Oid,
    ) -> Result<pg_sys::Oid, PgError> {
        let databases = CatalogRelation::open(
            pg_sys::DatabaseRelationId,
            pg_sys::AccessShareLock as pg_sys::LOCKMODE,
        )?;
        let mut scan = databases.begin_scan(
            pg_sys::DatabaseOidIndexId.into(),
            true,
            CatalogSnapshot::SelfVisible,
            [CatalogScanKey::oid_eq(
                pg_sys::Anum_pg_database_oid as pg_sys::AttrNumber,
                database_oid,
            )],
        )?;
        let tuple = scan
            .get_next()?
            .expect("OAT_POST_CREATE database must be visible in pg_database");

        // SAFETY: the tuple comes from a live SnapshotSelf scan of pg_database;
        // its fixed fields remain valid until the scan advances or is dropped.
        let database = unsafe {
            &*(pg_sys::GETSTRUCT(tuple.as_raw()) as pg_sys::Form_pg_database)
        };
        Ok(database.dattablespace)
    }
}
