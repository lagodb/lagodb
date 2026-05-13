//! Table option persistence for access-method-specific options.
//!
//! Options are extracted from `CREATE TABLE`, validated against the table AM's
//! schema, and persisted in `lakebase.table_options` for later rd_amcache load.

use crate::catalog;
use crate::diag::SqlStateError;
use crate::options::schema::{self, OptionDef};
use crate::pg_wrapper::{PgWrapper, PgWrapperError};
use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use pgrx::{FromDatum, IntoDatum};
use thiserror::Error;

// ============================================================================
//  Error Type
// ============================================================================

/// Errors that can occur when handling table options.
#[derive(Error, Debug)]
pub enum TableOptionError {
    #[error("invalid table option: {0}")]
    InvalidOption(String),

    #[error("failed to persist table options")]
    PersistFailed(#[source] PgWrapperError),

    #[error("failed to load table options")]
    LoadFailed(#[source] PgWrapperError),

    #[error("null relation pointer")]
    NullRelation,
}

impl From<TableOptionError> for ErrorReport {
    fn from(value: TableOptionError) -> Self {
        ErrorReport::new(value.sql_error_code(), format!("{value}"), "")
    }
}

impl SqlStateError for TableOptionError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
    }
}

// ============================================================================
//  TableOptions
// ============================================================================

/// Wrapper for custom table options extracted from `CREATE TABLE` statements.
#[derive(Debug, Clone)]
pub struct TableOptions {
    options: Vec<(String, Option<String>)>,
}

impl TableOptions {
    pub fn new(options: Vec<(String, Option<String>)>) -> Self {
        Self { options }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, Option<&str>)> {
        self.options
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_deref()))
    }

    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.options
            .iter()
            .find(|(k, _)| k == key)
            .and_then(|(_, v)| v.as_deref())
    }

    pub fn get_int(&self, key: &str) -> Option<i32> {
        self.get_str(key).and_then(|v| v.parse().ok())
    }

    pub fn extract_from_stmt(
        stmt: &mut pg_sys::CreateStmt,
        valid_options: &[OptionDef],
    ) -> Result<Option<Self>, TableOptionError> {
        // SAFETY: We hold a mutable reference to the statement, so it is safe to modify via FFI.
        let opts = unsafe {
            schema::extract_and_remove_options(&mut stmt.options, valid_options)
                .map_err(TableOptionError::InvalidOption)?
        };

        Ok((!opts.is_empty()).then(|| Self { options: opts }))
    }

    pub fn persist_to_catalog(
        &self,
        relid: pg_sys::Oid,
    ) -> Result<(), TableOptionError> {
        // If no options, nothing to persist
        if self.options.is_empty() {
            return Ok(());
        }

        let table_oid = catalog::get_table_options_oid()
            .map_err(TableOptionError::PersistFailed)?;

        unsafe {
            let rel_guard = crate::handles::TableGuard::open(
                table_oid,
                pg_sys::RowExclusiveLock as _,
            )
            .map_err(TableOptionError::PersistFailed)?;
            let rel = rel_guard.as_raw();

            let relid_datum = relid.into_datum().unwrap();

            let options_vec: Vec<String> = self
                .options
                .iter()
                .map(|(k, v)| {
                    let val = v.as_ref().map(|s| s.as_str()).unwrap_or("");
                    format!("{}={}", k, val)
                })
                .collect();

            let options_datum = options_vec.into_datum();

            let mut values =
                [relid_datum, options_datum.unwrap_or(pg_sys::Datum::from(0))];
            let mut nulls = [false, options_datum.is_none()];

            let tup_desc = (*rel).rd_att;
            let tuple = pg_sys::heap_form_tuple(
                tup_desc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
            );
            let _tuple_guard = crate::handles::HeapTupleGuard::new(tuple);

            PgWrapper::catalog_tuple_insert(rel, tuple)
                .map_err(TableOptionError::PersistFailed)?;
        }

        Ok(())
    }

    pub fn load_from_catalog(
        relid: pg_sys::Oid,
    ) -> Result<Option<Self>, TableOptionError> {
        let table_oid = catalog::get_table_options_oid()
            .map_err(TableOptionError::LoadFailed)?;
        let index_oid = catalog::get_table_options_pkey_oid()
            .map_err(TableOptionError::LoadFailed)?;

        unsafe {
            let rel_guard = crate::handles::TableGuard::open(
                table_oid,
                pg_sys::AccessShareLock as _,
            )
            .map_err(TableOptionError::LoadFailed)?;
            let rel = rel_guard.as_raw();

            let mut key: pg_sys::ScanKeyData = std::mem::zeroed();

            PgWrapper::scan_key_init(
                &mut key,
                1, // relid column
                pg_sys::BTEqualStrategyNumber as _,
                pg_sys::Oid::from(pg_sys::F_OIDEQ),
                relid.into_datum().unwrap(),
            );

            let scan = PgWrapper::systable_beginscan(
                rel,
                index_oid,
                true,
                std::ptr::null_mut(),
                1,
                &key as *const _ as *mut _,
            )
            .map_err(TableOptionError::LoadFailed)?;
            let scan_guard = crate::handles::SysScanGuard::from_raw(scan);

            let tuple = PgWrapper::systable_getnext(scan_guard.as_raw())
                .map_err(TableOptionError::LoadFailed)?;
            let mut result = None;

            if let Some(tuple) = tuple {
                let tup_desc = (*rel).rd_att;
                let mut is_null = false;
                // options is column 2
                let datum = pg_sys::heap_getattr(tuple, 2, tup_desc, &mut is_null);

                if !is_null {
                    let options_vec = Vec::<String>::from_datum(datum, is_null);
                    if let Some(options) = options_vec {
                        let parsed: Vec<(String, Option<String>)> = options
                            .into_iter()
                            .map(|s| {
                                let mut parts = s.splitn(2, '=');
                                let key = parts.next().unwrap_or("").to_string();
                                let val = parts.next().map(|v| v.to_string());
                                (key, val)
                            })
                            .collect();
                        result = Some(Self { options: parsed });
                    }
                }
            }

            Ok(result)
        }
    }
}
