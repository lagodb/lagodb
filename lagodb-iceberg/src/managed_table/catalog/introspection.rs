//! SQL introspection of managed Iceberg tables.
//!
//! `iceberg.managed_tables` classifies tables from the catalog projection
//! alone. `iceberg.partition_spec` describes the rules: unlike the JDBC
//! catalog adapter in `sql_api`, it reads the transaction-visible Iceberg
//! metadata, which stays the authoritative partition definition. Neither
//! exposes metadata locations, snapshots or file paths.

use iceberg_lite::spec::TableMetadata;
use lagodb_core::diag::{PgReportError, ReportableError};
use lagodb_core::handles::{RelationGuard, RelationHandle};
use pgrx::PgRelation;
use pgrx::prelude::*;

use super::IcebergRelationExt;
use crate::managed_table::storage::StorageContext;
use crate::managed_table::transaction::ManagedTransaction;
use crate::managed_table::{ManagedResult, ManagedTableError};

/// Column order of `iceberg.partition_spec`.
type PartitionSpecRow = (i32, i32, i32, String, i32, Option<String>, String);

/// One field of a table's current default partition spec.
struct PartitionSpecField {
    spec_id: i32,
    /// 1-based position in the spec, which orders partition values.
    ordinal_position: i32,
    partition_field_id: i32,
    partition_field_name: String,
    source_field_id: i32,
    /// `None` once a void field's source column has been dropped.
    source_column: Option<String>,
    transform: String,
}

impl PartitionSpecField {
    /// Fields of the transaction-visible default spec, in spec order. An
    /// unpartitioned spec may still list void fields.
    fn current_spec(relation: &RelationHandle<'_>) -> ManagedResult<Vec<Self>> {
        if !relation.is_iceberg() {
            return Err(ManagedTableError::NotManagedTable {
                relation: relation
                    .relation_name()
                    .as_c_str()
                    .to_string_lossy()
                    .into_owned(),
            });
        }
        let storage = StorageContext::for_read(relation)?;
        let loaded = ManagedTransaction::current()
            .current_table_metadata(relation.oid(), storage.file_io())?;
        Ok(Self::from_metadata(&loaded.metadata))
    }

    /// Source columns resolve against the current schema, which carries this
    /// transaction's staged renames.
    fn from_metadata(metadata: &TableMetadata) -> Vec<Self> {
        let spec = metadata.default_partition_spec();
        let schema = metadata.current_schema();
        spec.fields()
            .iter()
            .zip(1..)
            .map(|(field, ordinal_position)| Self {
                spec_id: spec.spec_id(),
                ordinal_position,
                partition_field_id: field.field_id,
                partition_field_name: field.name.clone(),
                source_field_id: field.source_id,
                source_column: schema
                    .field_by_id(field.source_id)
                    .map(|source| source.name.clone()),
                transform: field.transform.to_string(),
            })
            .collect()
    }

    fn into_row(self) -> PartitionSpecRow {
        (
            self.spec_id,
            self.ordinal_position,
            self.partition_field_id,
            self.partition_field_name,
            self.source_field_id,
            self.source_column,
            self.transform,
        )
    }
}

#[pg_schema]
mod iceberg {
    use super::*;

    /// Decoding the `PgRelation` argument takes AccessShareLock through a
    /// strict `relation_open`, and dropping it at return releases the lock, as
    /// `pg_get_replica_identity_index` does. A relation dropped before that
    /// lock is an error, as in `table_to_xmlschema`, never an empty spec. The
    /// NoLock `table_open` adds only its relation-kind check.
    #[pg_extern(stable, strict, parallel_restricted)]
    #[allow(clippy::type_complexity)] // pgrx requires named SQL columns in this tuple.
    fn partition_spec(
        relation: PgRelation,
    ) -> TableIterator<
        'static,
        (
            name!(spec_id, i32),
            name!(ordinal_position, i32),
            name!(partition_field_id, i32),
            name!(partition_field_name, String),
            name!(source_field_id, i32),
            name!(source_column, Option<String>),
            name!(transform, String),
        ),
    > {
        let fields = RelationGuard::open_table(relation.oid(), pg_sys::NoLock as _)
            .map_err(ManagedTableError::from)
            .and_then(|guard| PartitionSpecField::current_spec(&guard.as_handle()))
            .map_err(PgReportError::from_domain_error)
            .report_unwrap();
        TableIterator::new(fields.into_iter().map(PartitionSpecField::into_row))
    }
}

extension_sql_file!(
    "../../../sql/introspection.sql",
    name = "managed_table_introspection",
    requires = [iceberg::partition_spec],
);
