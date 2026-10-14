//! SQL boundary for the managed-table JDBC catalog projection.
//!
//! PostgreSQL reads the metadata rows with the caller's statement snapshot.
//! This adapter only resolves storage placement; it never binds an Iceberg
//! read view, inspects transaction overlays, or opens table files.

use lagodb_core::diag::{PgReportError, ReportableError};
use lagodb_core::handles::RelationTablespace;
use lagodb_core::storage::tablespace::TablespaceStorage;
use pgrx::prelude::*;

#[pg_schema]
mod iceberg {
    use super::*;

    /// Accept `pg_class.reltablespace`, not a relation OID, so catalog discovery
    /// needs no relation handles or per-table locks. Binding lookup reuses the
    /// core cache and its existing tablespace invalidation lifecycle.
    #[pg_extern(stable, strict, parallel_restricted)]
    fn catalog_is_object_tablespace(reltablespace: pg_sys::Oid) -> bool {
        let tablespace = RelationTablespace::from_catalog_oid(reltablespace);
        TablespaceStorage::is_object_backed(tablespace.resolved_oid())
            .map_err(PgReportError::from_domain_error)
            .report_unwrap()
    }
}

extension_sql_file!(
    "../../../sql/jdbc_catalog.sql",
    name = "managed_jdbc_catalog",
    requires = [iceberg::catalog_is_object_tablespace],
);
