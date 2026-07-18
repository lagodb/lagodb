//! Table option persistence for access-method-specific options.
//!
//! Options are extracted from `CREATE TABLE`, validated against the table AM's
//! schema, and persisted in `lakebase.table_options` for later rd_amcache load.

use crate::catalog::{self, CatalogRelation, CatalogScanKey, CatalogSnapshot};
use crate::diag::{PgError, SqlStateError, domain_error_report};
use crate::options::schema::{self, OptionDef, OptionMutability, OptionSchemaError};
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
    #[error("invalid table option")]
    InvalidSchema(#[from] OptionSchemaError),

    #[error("invalid table option: {0}")]
    InvalidOption(String),

    #[error("table option '{0}' can only be specified by CREATE TABLE")]
    CreateOnlyOption(String),

    #[error("failed to persist table options")]
    PersistFailed(#[source] PgError),

    #[error("failed to load table options")]
    LoadFailed(#[source] PgError),

    #[error("failed to delete table options")]
    DeleteFailed(#[source] PgError),

    #[error("null relation pointer")]
    NullRelation,
}

impl From<TableOptionError> for ErrorReport {
    fn from(value: TableOptionError) -> Self {
        domain_error_report(value)
    }
}

impl SqlStateError for TableOptionError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::InvalidSchema(_) | Self::InvalidOption(_) => {
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
            }
            Self::CreateOnlyOption(_) => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }
            Self::PersistFailed(error)
            | Self::LoadFailed(error)
            | Self::DeleteFailed(error) => error.sql_error_code(),
            Self::NullRelation => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        }
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

/// Ordered custom-option changes extracted from `ALTER TABLE ... SET/RESET`.
#[derive(Debug)]
pub struct TableOptionAlterations {
    changes: Vec<TableOptionChange>,
}

#[derive(Debug)]
enum TableOptionChange {
    Set(TableOptions),
    Reset(Vec<String>),
}

impl TableOptions {
    pub fn new(options: Vec<(String, Option<String>)>) -> Self {
        Self { options }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, Option<&str>)> {
        self.options.iter().map(|(k, v)| (k.as_str(), v.as_deref()))
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
            schema::extract_and_remove_options(&mut stmt.options, valid_options)?
        };

        Ok((!opts.is_empty()).then_some(Self { options: opts }))
    }

    pub fn read_from_stmt(
        stmt: &pg_sys::CreateStmt,
        valid_options: &[OptionDef],
    ) -> Result<Option<Self>, TableOptionError> {
        // SAFETY: `stmt.options` belongs to a live PostgreSQL parse tree for
        // this hook callback and is only read by this call.
        let opts = unsafe { schema::extract_options(stmt.options, valid_options)? };

        Ok((!opts.is_empty()).then_some(Self { options: opts }))
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
            let rel_guard =
                CatalogRelation::open(table_oid, pg_sys::RowExclusiveLock as _)
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
            // SAFETY: `heap_form_tuple` returns a heap tuple owned by the
            // caller, and PostgreSQL expects it to be released with
            // `heap_freetuple` when we are done.
            let tuple_guard = crate::handles::HeapTupleGuard::new(tuple);

            rel_guard
                .catalog_insert(&tuple_guard)
                .map_err(TableOptionError::PersistFailed)?;
        }

        Ok(())
    }

    /// Replace the complete persisted option set for one relation.
    ///
    /// The caller must hold a lock that excludes concurrent option changes.
    pub fn replace_in_catalog(
        &self,
        relid: pg_sys::Oid,
    ) -> Result<(), TableOptionError> {
        if self.options.is_empty() {
            Self::delete_from_catalog(relid)?;
            unsafe { pg_sys::CacheInvalidateRelcacheByRelid(relid) };
            return Ok(());
        }

        let table_oid = catalog::get_table_options_oid()
            .map_err(TableOptionError::PersistFailed)?;
        let index_oid = catalog::get_table_options_pkey_oid()
            .map_err(TableOptionError::PersistFailed)?;
        let rel_guard =
            CatalogRelation::open(table_oid, pg_sys::RowExclusiveLock as _)
                .map_err(TableOptionError::PersistFailed)?;
        let rel = rel_guard.as_raw();
        let mut scan_guard = rel_guard
            .begin_scan(
                index_oid,
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::oid_eq(1, relid)],
            )
            .map_err(TableOptionError::PersistFailed)?;

        let options: Vec<String> = self
            .options
            .iter()
            .map(|(key, value)| format!("{}={}", key, value.as_deref().unwrap_or("")))
            .collect();

        if let Some(old_tuple) = scan_guard
            .get_next()
            .map_err(TableOptionError::PersistFailed)?
        {
            let options_datum = options.into_datum();
            let tup_desc = unsafe { (*rel).rd_att };
            let natts = unsafe { (*tup_desc).natts as usize };
            let mut values = vec![pg_sys::Datum::from(0); natts];
            let mut nulls = vec![false; natts];
            let mut replacements = vec![false; natts];
            values[1] = options_datum.unwrap_or(pg_sys::Datum::from(0));
            nulls[1] = options_datum.is_none();
            replacements[1] = true;
            let new_tuple = unsafe {
                pg_sys::heap_modify_tuple(
                    old_tuple.as_raw(),
                    tup_desc,
                    values.as_mut_ptr(),
                    nulls.as_mut_ptr(),
                    replacements.as_mut_ptr(),
                )
            };
            let new_tuple = unsafe { crate::handles::HeapTupleGuard::new(new_tuple) };
            rel_guard
                .catalog_update(old_tuple, &new_tuple)
                .map_err(TableOptionError::PersistFailed)?;
        } else {
            drop(scan_guard);
            self.persist_to_catalog(relid)?;
        }

        unsafe { pg_sys::CacheInvalidateRelcacheByRelid(relid) };
        Ok(())
    }

    pub fn load_from_catalog(
        relid: pg_sys::Oid,
    ) -> Result<Option<Self>, TableOptionError> {
        let table_oid =
            catalog::get_table_options_oid().map_err(TableOptionError::LoadFailed)?;
        let index_oid = catalog::get_table_options_pkey_oid()
            .map_err(TableOptionError::LoadFailed)?;

        unsafe {
            let rel_guard =
                CatalogRelation::open(table_oid, pg_sys::AccessShareLock as _)
                    .map_err(TableOptionError::LoadFailed)?;
            let rel = rel_guard.as_raw();

            let mut scan_guard = rel_guard
                .begin_scan(
                    index_oid,
                    true,
                    CatalogSnapshot::Default,
                    [CatalogScanKey::oid_eq(1, relid)],
                )
                .map_err(TableOptionError::LoadFailed)?;

            let tuple = scan_guard
                .get_next()
                .map_err(TableOptionError::LoadFailed)?;
            let mut result = None;

            if let Some(tuple) = tuple {
                let tup_desc = (*rel).rd_att;
                let mut is_null = false;
                // options is column 2
                let datum =
                    pg_sys::heap_getattr(tuple.as_raw(), 2, tup_desc, &mut is_null);

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

    /// Delete persisted options for a relation if they exist.
    pub fn delete_from_catalog(relid: pg_sys::Oid) -> Result<(), TableOptionError> {
        let table_oid = catalog::get_table_options_oid()
            .map_err(TableOptionError::DeleteFailed)?;
        let index_oid = catalog::get_table_options_pkey_oid()
            .map_err(TableOptionError::DeleteFailed)?;

        let rel_guard =
            CatalogRelation::open(table_oid, pg_sys::RowExclusiveLock as _)
                .map_err(TableOptionError::DeleteFailed)?;

        let mut scan_guard = rel_guard
            .begin_scan(
                index_oid,
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::oid_eq(1, relid)],
            )
            .map_err(TableOptionError::DeleteFailed)?;

        if let Some(tuple) = scan_guard
            .get_next()
            .map_err(TableOptionError::DeleteFailed)?
        {
            rel_guard
                .catalog_delete(tuple)
                .map_err(TableOptionError::DeleteFailed)?;
        }

        Ok(())
    }
}

impl TableOptionAlterations {
    /// Reject recognized CREATE-only options before the command tree is
    /// rewritten.  Keeping this rule on the shared schema ensures SET and
    /// RESET cannot drift apart as AM option sets grow.
    unsafe fn reject_create_only(
        options: *mut pg_sys::List,
        valid_options: &[OptionDef],
    ) -> Result<(), TableOptionError> {
        if options.is_null() {
            return Ok(());
        }
        let count = unsafe { pg_sys::list_length(options) };
        for index in 0..count {
            let element =
                unsafe { pg_sys::list_nth(options, index).cast::<pg_sys::DefElem>() };
            if element.is_null() {
                continue;
            }
            let name = unsafe { std::ffi::CStr::from_ptr((*element).defname) };
            if let Some(definition) = valid_options
                .iter()
                .find(|definition| name.to_bytes() == definition.name.as_bytes())
                && definition.mutability == OptionMutability::CreateOnly
            {
                return Err(TableOptionError::CreateOnlyOption(
                    definition.name.to_owned(),
                ));
            }
        }
        Ok(())
    }

    /// Whether an ALTER command list contains an option owned by this schema.
    ///
    /// # Safety
    ///
    /// `cmds` must be null or a live PostgreSQL list of `AlterTableCmd` nodes.
    pub unsafe fn commands_contain_options(
        cmds: *mut pg_sys::List,
        valid_options: &[OptionDef],
    ) -> bool {
        if cmds.is_null() {
            return false;
        }

        let len = unsafe { pg_sys::list_length(cmds) };
        for idx in 0..len {
            let command = unsafe {
                pg_sys::list_nth(cmds, idx) as *const pg_sys::AlterTableCmd
            };
            if command.is_null()
                || !matches!(
                    unsafe { (*command).subtype },
                    pg_sys::AlterTableType::AT_SetRelOptions
                        | pg_sys::AlterTableType::AT_ResetRelOptions
                )
            {
                continue;
            }

            let options = unsafe { (*command).def as *mut pg_sys::List };
            if options.is_null() {
                continue;
            }
            let option_count = unsafe { pg_sys::list_length(options) };
            for option_idx in 0..option_count {
                let element = unsafe {
                    pg_sys::list_nth(options, option_idx) as *const pg_sys::DefElem
                };
                if element.is_null() {
                    continue;
                }
                let name = unsafe { std::ffi::CStr::from_ptr((*element).defname) };
                if valid_options
                    .iter()
                    .any(|option| name.to_bytes() == option.name.as_bytes())
                {
                    return true;
                }
            }
        }
        false
    }

    /// Extract custom changes and remove them from PostgreSQL's command tree.
    ///
    /// # Safety
    ///
    /// `cmds` must be null or a uniquely borrowed live PostgreSQL list of
    /// `AlterTableCmd` nodes.
    pub unsafe fn extract_from_commands(
        cmds: *mut pg_sys::List,
        valid_options: &[OptionDef],
    ) -> Result<Self, TableOptionError> {
        let mut changes = Vec::new();
        if cmds.is_null() {
            return Ok(Self { changes });
        }

        let len = unsafe { pg_sys::list_length(cmds) };
        for idx in 0..len {
            let command =
                unsafe { pg_sys::list_nth(cmds, idx) as *mut pg_sys::AlterTableCmd };
            if command.is_null() {
                continue;
            }

            match unsafe { (*command).subtype } {
                pg_sys::AlterTableType::AT_SetRelOptions => {
                    unsafe {
                        Self::reject_create_only(
                            (*command).def.cast::<pg_sys::List>(),
                            valid_options,
                        )?;
                    }
                    let options_ptr = unsafe {
                        std::ptr::addr_of_mut!((*command).def)
                            .cast::<*mut pg_sys::List>()
                    };
                    let options = unsafe {
                        schema::extract_and_remove_options(
                            options_ptr,
                            valid_options,
                        )?
                    };
                    if !options.is_empty() {
                        changes
                            .push(TableOptionChange::Set(TableOptions::new(options)));
                    }
                }
                pg_sys::AlterTableType::AT_ResetRelOptions => {
                    unsafe {
                        Self::reject_create_only(
                            (*command).def.cast::<pg_sys::List>(),
                            valid_options,
                        )?;
                    }
                    let options_ptr = unsafe {
                        std::ptr::addr_of_mut!((*command).def)
                            .cast::<*mut pg_sys::List>()
                    };
                    let names = unsafe {
                        schema::extract_and_remove_option_names(
                            options_ptr,
                            valid_options,
                        )?
                    };
                    if !names.is_empty() {
                        changes.push(TableOptionChange::Reset(names));
                    }
                }
                _ => {}
            }
        }

        Ok(Self { changes })
    }

    /// Apply ordered SET/RESET operations to persisted option overrides.
    ///
    /// RESET removes the persisted override; it does not persist a copied
    /// default value. The AM must pass the result through the same option
    /// resolver used by CREATE TABLE, so an absent value regains that schema's
    /// current CREATE default.
    pub fn apply_to_overrides(self, current: Option<TableOptions>) -> TableOptions {
        let mut options = current.map_or_else(Vec::new, |current| current.options);
        for change in self.changes {
            match change {
                TableOptionChange::Set(set) => {
                    for (key, value) in set.options {
                        if let Some((_, current_value)) =
                            options.iter_mut().find(|(name, _)| *name == key)
                        {
                            *current_value = value;
                        } else {
                            options.push((key, value));
                        }
                    }
                }
                TableOptionChange::Reset(names) => {
                    options.retain(|(name, _)| !names.contains(name));
                }
            }
        }
        TableOptions::new(options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn option_value<'a>(options: &'a TableOptions, name: &str) -> Option<&'a str> {
        options
            .iter()
            .find_map(|(key, value)| (key == name).then_some(value))
            .flatten()
    }

    #[test]
    fn reset_removes_persisted_override_instead_of_copying_a_default() {
        let current = TableOptions::new(vec![
            ("compression".to_owned(), Some("snappy".to_owned())),
            ("write-format".to_owned(), Some("parquet".to_owned())),
        ]);
        let alterations = TableOptionAlterations {
            changes: vec![TableOptionChange::Reset(vec!["compression".to_owned()])],
        };

        let updated = alterations.apply_to_overrides(Some(current));

        assert_eq!(option_value(&updated, "compression"), None);
        assert_eq!(option_value(&updated, "write-format"), Some("parquet"));
    }

    #[test]
    fn ordered_set_then_reset_restores_the_unspecified_state() {
        let alterations = TableOptionAlterations {
            changes: vec![
                TableOptionChange::Set(TableOptions::new(vec![(
                    "compression".to_owned(),
                    Some("snappy".to_owned()),
                )])),
                TableOptionChange::Reset(vec!["compression".to_owned()]),
            ],
        };

        let updated = alterations.apply_to_overrides(None);

        assert_eq!(option_value(&updated, "compression"), None);
    }
}
