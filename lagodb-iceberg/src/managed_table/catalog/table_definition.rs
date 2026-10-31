//! PostgreSQL catalog definition to Iceberg table-definition lowering.

use iceberg_lite::spec::{Schema, UnboundPartitionSpec};
use lagodb_core::handles::{PartitionKey, RelationHandle};
use pgrx::pg_sys;

use crate::error::IcebergResult;

use super::partition_lowering::PgPartitionSpecAdapter;
use super::schema_builder::tuple_desc_to_schema;

/// Fully validated metadata needed to create one managed Iceberg table.
pub(crate) struct ManagedTableDefinition {
    schema: Schema,
    partition_spec: UnboundPartitionSpec,
}

impl ManagedTableDefinition {
    pub(crate) fn build(
        rel: &RelationHandle<'_>,
        partition_spec: Option<&pg_sys::PartitionSpec>,
    ) -> IcebergResult<Self> {
        let schema = tuple_desc_to_schema(rel)?;
        let partition_spec = match partition_spec {
            Some(spec) => PartitionKey::analyze(rel, spec, |key| {
                PgPartitionSpecAdapter::new(rel, &schema, key).build()
            })?,
            None => UnboundPartitionSpec::default(),
        };
        Ok(Self {
            schema,
            partition_spec,
        })
    }

    pub(crate) fn into_parts(self) -> (Schema, UnboundPartitionSpec) {
        (self.schema, self.partition_spec)
    }
}
