//! Tablespace option extraction and catalog persistence.
//!
//! Storage options are accepted through `CREATE TABLESPACE ... WITH (...)` and
//! persisted into `pg_tablespace.spcoptions`, where PostgreSQL already stores
//! per-tablespace reloptions.

use super::defs;
use super::storage::{
    TablespaceStorage, TablespaceStorageError, store_id_from_tablespace_name,
};
use crate::catalog::{CatalogRelation, search_syscache_copy};
use crate::diag::{PgError, SqlStateError};
use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use pgrx::{FromDatum, IntoDatum};
use std::ffi::CStr;
use thiserror::Error;

// ============================================================================
//  Error Type
// ============================================================================
#[derive(Error, Debug)]
pub enum TablespaceError {
    #[error("invalid tablespace option: {0}")]
    InvalidOption(String),

    #[error("invalid tablespace storage config")]
    InvalidStorage(#[from] TablespaceStorageError),

    #[error("failed to update tablespace")]
    UpdateFailed(#[source] PgError),

    #[error("tablespace OID {0} not found in pg_tablespace")]
    NotFound(pg_sys::Oid),
}

impl From<TablespaceError> for ErrorReport {
    fn from(value: TablespaceError) -> Self {
        ErrorReport::new(value.sql_error_code(), format!("{value}"), "")
    }
}

impl SqlStateError for TablespaceError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
    }
}

// ============================================================================
//  TablespaceOptions
// ============================================================================

/// Wrapper for custom tablespace options extracted from `CREATE TABLESPACE` statements.
#[derive(Debug)]
pub struct TablespaceOptions {
    options: Vec<(String, Option<String>)>,
}

impl TablespaceOptions {
    pub fn extract_from_stmt(
        stmt: &mut pg_sys::CreateTableSpaceStmt,
    ) -> Result<Option<Self>, TablespaceError> {
        // Call into the FFI layer (unsafe)
        // SAFETY: We hold a mutable reference to the statement, so it is safe to modify it via FFI.
        let opts = unsafe {
            defs::extract_and_remove_options(stmt)
                .map_err(TablespaceError::InvalidOption)?
        };

        let Some(options) = (!opts.is_empty()).then(|| Self { options: opts }) else {
            return Ok(None);
        };

        let tablespace_name = unsafe { CStr::from_ptr(stmt.tablespacename) }
            .to_string_lossy()
            .into_owned();
        options.validate_storage_options(&tablespace_name)?;

        Ok(Some(options))
    }

    pub fn read_from_stmt(
        stmt: &pg_sys::CreateTableSpaceStmt,
    ) -> Result<Option<Self>, TablespaceError> {
        // SAFETY: `stmt` is a live PostgreSQL parse tree for this hook callback
        // and the options list is only read by this call.
        let opts = unsafe {
            defs::extract_options(stmt).map_err(TablespaceError::InvalidOption)?
        };

        let Some(options) = (!opts.is_empty()).then(|| Self { options: opts }) else {
            return Ok(None);
        };

        let tablespace_name = unsafe { CStr::from_ptr(stmt.tablespacename) }
            .to_string_lossy()
            .into_owned();
        options.validate_storage_options(&tablespace_name)?;

        Ok(Some(options))
    }

    pub fn persist_to_catalog(
        &self,
        spcoid: pg_sys::Oid,
    ) -> Result<(), TablespaceError> {
        if self.options.is_empty() {
            return Ok(());
        }

        unsafe {
            let rel_guard = CatalogRelation::open(
                pg_sys::TableSpaceRelationId,
                pg_sys::RowExclusiveLock as _,
            )
            .map_err(TablespaceError::UpdateFailed)?;
            let rel = rel_guard.as_raw();

            let oid_datum = spcoid.into_datum().unwrap();
            let tuple = search_syscache_copy(
                pg_sys::SysCacheIdentifier::TABLESPACEOID as i32,
                oid_datum,
                0.into(),
                0.into(),
                0.into(),
            )
            .map_err(TablespaceError::UpdateFailed)?;

            let Some(tuple) = tuple else {
                return Err(TablespaceError::NotFound(spcoid));
            };

            // Extract existing spcoptions
            let existing_datum = tuple
                .get_attr(pg_sys::Anum_pg_tablespace_spcoptions as i16)
                .map_err(TablespaceError::UpdateFailed)?;

            let mut current_options: Vec<String> = existing_datum
                .and_then(|datum| Vec::<String>::from_datum(datum, false))
                .unwrap_or_default();

            current_options.extend(self.catalog_options());

            let new_options_datum = current_options.into_datum();

            // Prepare for heap_modify_tuple
            let tup_desc = (*rel).rd_att;
            let natts = (*tup_desc).natts as usize;
            let mut values = vec![0.into(); natts];
            let mut nulls = vec![false; natts];
            let mut repls = vec![false; natts];

            let spcoptions_idx = (pg_sys::Anum_pg_tablespace_spcoptions - 1) as usize;
            values[spcoptions_idx] = new_options_datum.unwrap_or(0.into());
            nulls[spcoptions_idx] = new_options_datum.is_none();
            repls[spcoptions_idx] = true;

            let new_tuple = pg_sys::heap_modify_tuple(
                tuple.as_raw(),
                tup_desc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
                repls.as_mut_ptr(),
            );
            // SAFETY: `heap_modify_tuple` returns a new heap tuple owned by the
            // caller, and PostgreSQL expects it to be released with
            // `heap_freetuple` after the catalog update.
            let new_tuple_guard = crate::handles::HeapTupleGuard::new(new_tuple);

            rel_guard
                .catalog_update(
                    crate::handles::HeapTupleRef::from_raw(tuple.as_raw()),
                    &new_tuple_guard,
                )
                .map_err(TablespaceError::UpdateFailed)?;
        }

        Ok(())
    }

    fn validate_storage_options(
        &self,
        tablespace_name: &str,
    ) -> Result<(), TablespaceError> {
        store_id_from_tablespace_name(tablespace_name)?;
        let catalog_options = self.catalog_options();
        TablespaceStorage::from_catalog_options(tablespace_name, catalog_options)?;
        Ok(())
    }

    fn catalog_options(&self) -> Vec<String> {
        self.options
            .iter()
            .filter_map(|(key, value)| {
                value.as_ref().map(|value| format!("{}={}", key, value))
            })
            .collect()
    }
}
