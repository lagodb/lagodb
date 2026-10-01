//! Transaction-visible managed-table metadata and scan snapshot construction.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg_lite::spec::Schema as IcebergSchema;
use iceberg_lite::table::Table;
use lagodb_core::handles::RelationGuard;
use pgrx::pg_sys;

use crate::error::IcebergResult;
use crate::managed_table::catalog::bridge::IcebergTableId;
use crate::managed_table::catalog::metadata_tracker::{
    LoadedTableMetadata, TxMetadata,
};
use crate::managed_table::storage::StorageContext;
use crate::scan::IcebergReadSnapshot;
use crate::write::PgTransactionIsolation;

/// Managed-table metadata and transaction delta visible to one read operation.
///
/// The Iceberg [`Table`] is built only when this view is consumed for scan
/// execution. Planning-only callers can inspect the schema and properties
/// without constructing a table or its object cache.
pub(crate) struct ManagedTableReadView {
    relation_oid: pg_sys::Oid,
    context: StorageContext,
    loaded: LoadedTableMetadata,
}

impl ManagedTableReadView {
    pub(crate) fn load(relation_oid: pg_sys::Oid) -> IcebergResult<Self> {
        PgTransactionIsolation::current()?;
        // Every caller is inside planning or execution with PostgreSQL's
        // relation lock already held. Acquire only the relcache reference
        // needed to resolve the managed relation's storage policy.
        let relation = RelationGuard::open_table(
            relation_oid,
            pg_sys::NoLock as pg_sys::LOCKMODE,
        )?;
        let context = StorageContext::for_read(&relation.as_handle())?;
        let loaded = TxMetadata::current()
            .current_table_metadata(relation_oid, context.file_io())?;
        Ok(Self {
            relation_oid,
            context,
            loaded,
        })
    }

    pub(crate) fn schema(&self) -> &Arc<IcebergSchema> {
        self.loaded.metadata.current_schema()
    }

    pub(crate) fn properties(&self) -> &HashMap<String, String> {
        self.loaded.metadata.properties()
    }

    pub(crate) fn storage_bytes(&self) -> IcebergResult<u64> {
        Ok(self.loaded.relation_stats(self.context.file_io())?.1)
    }

    pub(crate) fn into_read_snapshot(self) -> IcebergResult<IcebergReadSnapshot> {
        let Self {
            relation_oid,
            context,
            loaded,
        } = self;
        let LoadedTableMetadata {
            location,
            metadata,
            delta,
            ..
        } = loaded;
        let table = Table::builder()
            .file_io(context.into_file_io())
            .metadata_location(location)
            .metadata(metadata)
            .identifier(IcebergTableId::for_relation(relation_oid).into_table_ident())
            .build()?;
        Ok(IcebergReadSnapshot::new(table, delta))
    }
}
