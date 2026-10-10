//! COPY candidate admission and execution-lock target binding.

use std::ffi::CStr;

use pgrx::{PgSqlErrorCode, pg_sys};

use crate::catalog::RelationCatalogEntry;
use crate::diag::PgReportError;
use crate::handles::RelationHandle;
use crate::table_provider::TableProviderRouter;

use super::{CopyError, CopyFromPreparation, CopyStatement, CopyToPreparation};

/// Candidate identity for relation COPY, without a target relation lock.
///
/// This is admission only: missing names and cross-database references are
/// left to the selected executor. Explicit schema ACL checks are deferred too.
/// PostgreSQL's existing namespace lookups retain search_path and pg_temp
/// semantics, including their namespace-hook side effects. A candidate cannot
/// authorize partitioned table execution; preparation must bind the locked target.
pub struct CopyTargetProbe<'statement> {
    relation: Option<&'statement pg_sys::RangeVar>,
}

impl<'statement> CopyTargetProbe<'statement> {
    pub fn new(statement: &'statement CopyStatement<'_>) -> Self {
        Self {
            relation: statement.relation(),
        }
    }

    /// Standard FROM fallback for explicit AM writes. Partitioned targets
    /// qualify only when their provider owns the root as a logical table.
    pub fn requires_write_bridge(&self) -> Result<bool, CopyError> {
        let Some(target) = self.find() else {
            return Ok(false);
        };
        CopyTargetRoute::requires_write_bridge(
            target.relkind() as u8,
            target.access_method_oid(),
        )
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

/// COPY TO preparation bound to the claiming AM's partitioned table.
///
/// RLS query-mode exports retain the same target binding and execution lock.
pub struct PartitionedTableCopyToPreparation<'statement, 'parse> {
    inner: CopyToPreparation<'statement, 'parse>,
}

impl<'statement, 'parse> PartitionedTableCopyToPreparation<'statement, 'parse> {
    pub(super) fn into_preparation(self) -> CopyToPreparation<'statement, 'parse> {
        self.inner
    }
}

impl CopyFromPreparation<'_, '_> {
    /// Recheck the shared FROM claim against PostgreSQL's execution-locked target.
    /// A positive probe is not authority to execute an unrelated replacement.
    pub fn require_write_bridge(&self) -> Result<(), CopyError> {
        let relation = unsafe { RelationHandle::from_raw(self.relation()) };
        if CopyTargetRoute::requires_write_bridge(
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

impl<'statement, 'parse> CopyToPreparation<'statement, 'parse> {
    /// Bind a named COPY TO target, including a relation rewritten for RLS.
    ///
    /// PG retains AccessShareLock when preparation closes the RLS relation
    /// reference. Its query_relation OID identifies the actual locked target,
    /// independently of the relation/query executor shape. Pure query COPY
    /// has no named target and cannot produce a partitioned table preparation.
    /// `None` rejects the target when the claiming AM no longer exists.
    pub fn into_partitioned_table(
        self,
        access_method: Option<pg_sys::Oid>,
    ) -> Result<PartitionedTableCopyToPreparation<'statement, 'parse>, CopyError>
    {
        let target = RelationCatalogEntry::find_cached(self.query_relation())
            .ok_or_else(CopyTargetRoute::partitioned_table_required_error)?;
        let route = CopyTargetRoute::for_identity(
            target.relkind() as u8,
            target.access_method_oid(),
        )?;
        route.require_partitioned_table(target.access_method_oid(), access_method)?;
        Ok(PartitionedTableCopyToPreparation { inner: self })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CopyTargetRoute {
    PostgreSql,
    ProviderOwnedPartitionedTable,
}

impl CopyTargetRoute {
    fn requires_write_bridge(
        relkind: u8,
        access_method: pg_sys::Oid,
    ) -> Result<bool, CopyError> {
        if relkind != pg_sys::RELKIND_RELATION
            && relkind != pg_sys::RELKIND_PARTITIONED_TABLE
        {
            return Ok(false);
        }
        Ok(
            TableProviderRouter::provider_for_am(access_method)?.is_some_and(
                |provider| {
                    !provider.copy.is_null()
                        && (relkind == pg_sys::RELKIND_RELATION
                            || provider.owns_partitioned_table)
                },
            ),
        )
    }

    /// Resolve target ownership once before entering the executor loop.
    ///
    /// # Safety
    ///
    /// `relation` must be the live relation retained by COPY preparation.
    pub(crate) unsafe fn for_relation(
        relation: pg_sys::Relation,
    ) -> Result<Self, CopyError> {
        let relation = unsafe { RelationHandle::from_raw(relation) };
        Self::for_identity(relation.relkind() as u8, relation.access_method_oid())
    }

    fn for_identity(
        relkind: u8,
        access_method: pg_sys::Oid,
    ) -> Result<Self, CopyError> {
        if relkind != pg_sys::RELKIND_PARTITIONED_TABLE {
            return Ok(Self::PostgreSql);
        }

        let owns_partitioned_table =
            TableProviderRouter::owns_partitioned_table(access_method)?;
        Ok(if owns_partitioned_table {
            Self::ProviderOwnedPartitionedTable
        } else {
            Self::PostgreSql
        })
    }

    pub(crate) const fn provider_owned_partitioned_table(self) -> bool {
        matches!(self, Self::ProviderOwnedPartitionedTable)
    }

    fn require_partitioned_table(
        self,
        actual_access_method: pg_sys::Oid,
        expected_access_method: Option<pg_sys::Oid>,
    ) -> Result<(), CopyError> {
        if self.provider_owned_partitioned_table()
            && Some(actual_access_method) == expected_access_method
        {
            Ok(())
        } else {
            Err(Self::partitioned_table_required_error())
        }
    }

    fn partitioned_table_required_error() -> CopyError {
        PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
            "COPY target no longer matches the claimed partitioned table",
        )
        .into()
    }
}
