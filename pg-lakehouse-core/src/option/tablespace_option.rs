//! TablespaceOptions - Type-safe wrapper for tablespace options.
//!
//! This module provides a high-level interface for extracting and persisting
//! custom options from `CREATE TABLESPACE` statements.

use super::storage_option;
use crate::pg_wrapper::PgWrapper;
use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use pgrx::{FromDatum, IntoDatum};
use thiserror::Error;

// ============================================================================
//  Error Type
// ============================================================================
#[derive(Error, Debug, Clone)]
pub enum TablespaceError {
    #[error("invalid tablespace option: {0}")]
    InvalidOption(String),
    #[error("failed to update tablespace: {0}")]
    UpdateFailed(String),
}

impl From<TablespaceError> for ErrorReport {
    fn from(value: TablespaceError) -> Self {
        let error_message = format!("{value}");
        ErrorReport::new(
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            error_message,
            "",
        )
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
            storage_option::extract_and_remove_custom_options(stmt)
                .map_err(TablespaceError::InvalidOption)?
        };

        Ok((!opts.is_empty()).then(|| Self { options: opts }))
    }

    pub fn persist_to_catalog(
        &self,
        spcoid: pg_sys::Oid,
    ) -> Result<(), TablespaceError> {
        if self.options.is_empty() {
            return Ok(());
        }

        unsafe {
            let rel = PgWrapper::table_open(
                pg_sys::TableSpaceRelationId,
                pg_sys::RowExclusiveLock as _,
            )
            .map_err(|e| TablespaceError::UpdateFailed(e.to_string()))?;

            let oid_datum = spcoid.into_datum().unwrap();
            let tuple = PgWrapper::search_sys_cache_copy(
                pg_sys::SysCacheIdentifier::TABLESPACEOID as i32,
                oid_datum,
                0.into(),
                0.into(),
                0.into(),
            )
            .map_err(|e| TablespaceError::UpdateFailed(e.to_string()))?;

            let tuple = match tuple {
                Some(t) => t,
                None => {
                    return Err(TablespaceError::UpdateFailed(format!(
                        "Tablespace {} not found",
                        spcoid
                    )));
                }
            };

            // Extract existing spcoptions
            let mut is_null = false;
            let existing_datum = PgWrapper::sys_cache_get_attr(
                pg_sys::SysCacheIdentifier::TABLESPACEOID as i32,
                tuple,
                pg_sys::Anum_pg_tablespace_spcoptions as i16,
                &mut is_null,
            )
            .map_err(|e| TablespaceError::UpdateFailed(e.to_string()))?;

            let mut current_options: Vec<String> = if is_null {
                Vec::new()
            } else {
                Vec::<String>::from_datum(existing_datum, false).unwrap_or_default()
            };

            // Append new options
            for (k, v) in &self.options {
                if let Some(val) = v {
                    current_options.push(format!("{}={}", k, val));
                }
            }

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
                tuple,
                tup_desc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
                repls.as_mut_ptr(),
            );

            PgWrapper::catalog_tuple_update(rel, &mut (*tuple).t_self, new_tuple)
                .map_err(|e| TablespaceError::UpdateFailed(e.to_string()))?;

            pg_sys::heap_freetuple(new_tuple);
            pg_sys::heap_freetuple(tuple);
            PgWrapper::table_close(rel, pg_sys::RowExclusiveLock as _)
                .map_err(|e| TablespaceError::UpdateFailed(e.to_string()))?;
        }

        Ok(())
    }
}
