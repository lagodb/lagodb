//! Managed metadata loading and transaction enrollment for schema evolution.

mod update;

pub(in crate::managed_table) use update::SchemaEvolutionUpdate;

use std::sync::Arc;

use iceberg_lite::io::FileIO;
use iceberg_lite::table::Table;
use lagodb_core::handles::RelationHandle;

use super::bridge::IcebergTableId;
use super::metadata_tracker::{LoadedTableMetadata, TxMetadata};
use crate::error::IcebergResult;
use crate::managed_table::storage::StorageContext;

/// Loads managed catalog state and stages a schema-evolution plan for
/// one PostgreSQL relation.
pub(in crate::managed_table) struct ManagedSchemaEvolution<'borrow, 'relation> {
    relation: &'borrow RelationHandle<'relation>,
}

impl<'borrow, 'relation> ManagedSchemaEvolution<'borrow, 'relation> {
    pub(in crate::managed_table) fn new(
        relation: &'borrow RelationHandle<'relation>,
    ) -> Self {
        Self { relation }
    }

    pub(in crate::managed_table) fn preflight(
        &self,
        update: &SchemaEvolutionUpdate,
    ) -> IcebergResult<()> {
        if !update.requires_existing_schema_preflight() {
            return Ok(());
        }
        let context = StorageContext::for_write(self.relation)?;
        let file_io = context.into_file_io();
        let loaded = TxMetadata::current()
            .current_table_metadata(self.relation.oid(), &file_io)?;
        update.preflight(&self.table(file_io, loaded)?)
    }

    pub(in crate::managed_table) fn stage(
        &self,
        update: &SchemaEvolutionUpdate,
    ) -> IcebergResult<()> {
        if update.is_empty() {
            return Ok(());
        }
        let context = StorageContext::for_write(self.relation)?;
        let file_io = context.into_file_io();
        let tracker = TxMetadata::current();
        let loaded = tracker.begin_table_modify(self.relation.oid(), &file_io)?;
        let table = self.table(file_io.clone(), loaded)?;
        let prepared = update.prepare_for_relation(self.relation, &table)?;
        tracker.stage_schema_update(self.relation.oid(), prepared, &file_io)
    }

    fn table(
        &self,
        file_io: FileIO,
        loaded: LoadedTableMetadata,
    ) -> IcebergResult<Table> {
        Ok(Table::builder()
            .metadata_location(loaded.location)
            .metadata(Arc::new(loaded.metadata))
            .identifier(
                IcebergTableId::for_relation(self.relation.oid()).into_table_ident(),
            )
            .file_io(file_io)
            .build()?)
    }
}
