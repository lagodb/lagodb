//! COPY candidate admission and execution-lock target binding.

use std::ffi::CStr;

use pgrx::{PgSqlErrorCode, pg_sys};

use crate::catalog::RelationCatalogEntry;
use crate::diag::PgReportError;
use crate::handles::RelationHandle;
use crate::table_provider::TableProviderRouter;

use super::{CopyError, CopyFromPreparation, CopyStatement};

/// Candidate identity for relation COPY, without a target relation lock.
///
/// This is admission only: missing names and cross-database references are
/// left to the selected executor. Explicit schema ACL checks are deferred too.
/// PostgreSQL's existing namespace lookups retain search_path and pg_temp
/// semantics, including their namespace-hook side effects. A candidate cannot
/// authorize execution; preparation must bind the locked target.
pub struct CopyTargetProbe<'statement> {
    relation: Option<&'statement pg_sys::RangeVar>,
}

impl<'statement> CopyTargetProbe<'statement> {
    pub fn new(statement: &'statement CopyStatement<'_>) -> Self {
        Self {
            relation: statement.relation(),
        }
    }

    /// Standard FROM fallback for explicit AM writes.
    pub fn requires_write_bridge(&self) -> Result<bool, CopyError> {
        let Some(target) = self.find() else {
            return Ok(false);
        };
        Self::table_requires_write_bridge(
            target.relkind() as u8,
            target.access_method_oid(),
        )
    }

    fn table_requires_write_bridge(
        relkind: u8,
        access_method: pg_sys::Oid,
    ) -> Result<bool, CopyError> {
        if relkind != pg_sys::RELKIND_RELATION {
            return Ok(false);
        }
        Ok(TableProviderRouter::provider_for_am(access_method)?
            .is_some_and(|provider| provider.copy().is_some()))
    }

    /// Find current catalog identity without opening the target relation.
    ///
    /// Catalog failures and namespace-hook errors propagate to the utility
    /// boundary; this method does not catch and discard PostgreSQL ERRORs.
    pub fn find(&self) -> Option<RelationCatalogEntry> {
        let relation = self.relation?;
        // SAFETY: CopyStatement borrows a live COPY parse tree; optional name
        // pointers are either NULL or its NUL-terminated PostgreSQL strings.
        unsafe {
            if !relation.catalogname.is_null() {
                let database_name = pg_sys::get_database_name(pg_sys::MyDatabaseId);
                // The backend's current database exists for this invocation.
                let current_database = CStr::from_ptr(database_name);
                let same_database =
                    CStr::from_ptr(relation.catalogname) == current_database;
                pg_sys::pfree(database_name.cast());
                if !same_database {
                    return None;
                }
            }
            let oid = if relation.schemaname.is_null() {
                pg_sys::RelnameGetRelid(relation.relname)
            } else {
                let namespace = pg_sys::LookupNamespaceNoError(relation.schemaname);
                if namespace == pg_sys::InvalidOid {
                    return None;
                }
                pg_sys::get_relname_relid(relation.relname, namespace)
            };
            if oid == pg_sys::InvalidOid {
                return None;
            }
            RelationCatalogEntry::find_cached(oid)
        }
    }
}

impl CopyFromPreparation<'_, '_> {
    /// Recheck the shared FROM claim against PostgreSQL's execution-locked target.
    /// A positive probe is not authority to execute an unrelated replacement.
    pub fn require_write_bridge(&self) -> Result<(), CopyError> {
        let relation = unsafe { RelationHandle::from_raw(self.relation()) };
        if CopyTargetProbe::table_requires_write_bridge(
            relation.relkind() as u8,
            relation.access_method_oid(),
        )? {
            Ok(())
        } else {
            Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                "COPY target no longer matches the claimed table route",
            )
            .into())
        }
    }
}
