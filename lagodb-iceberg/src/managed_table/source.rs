//! Managed-table snapshot loading shared by TableAM, CustomScan, and Query Offload.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg_lite::overlay::SnapshotDelta;
use iceberg_lite::spec::Schema as IcebergSchema;
use iceberg_lite::table::Table;
use pgrx::pg_sys;

use crate::error::IcebergResult;
use crate::managed_table::catalog::bridge::IcebergTableId;
use crate::managed_table::catalog::metadata_tracker::{
    LoadedTableMetadata, TxMetadata,
};
use crate::managed_table::storage::StorageContext;
use crate::scan::IcebergReadSnapshot;
use crate::write::PgTransactionIsolation;

/// Managed Iceberg table and transaction delta captured for one statement.
pub(crate) struct ManagedTableSnapshot {
    table: Table,
    schema: Arc<IcebergSchema>,
    delta: Option<Arc<SnapshotDelta>>,
}

/// Managed statement snapshot paired with ANALYZE-only relation statistics.
pub(crate) struct ManagedAnalyzeSnapshot {
    snapshot: ManagedTableSnapshot,
    storage_bytes: u64,
}

impl ManagedTableSnapshot {
    pub(crate) fn load_query(
        relation_oid: pg_sys::Oid,
        tablespace_oid: pg_sys::Oid,
    ) -> IcebergResult<Self> {
        let (context, loaded) = Self::load_metadata(relation_oid, tablespace_oid)?;
        Self::from_metadata(relation_oid, &context, loaded)
    }

    fn load_metadata(
        relation_oid: pg_sys::Oid,
        tablespace_oid: pg_sys::Oid,
    ) -> IcebergResult<(StorageContext, LoadedTableMetadata)> {
        PgTransactionIsolation::current()?;
        let context = StorageContext::for_tablespace(tablespace_oid)?;
        let loaded = TxMetadata::current()
            .current_table_metadata(relation_oid, context.file_io())?;
        Ok((context, loaded))
    }

    fn from_metadata(
        relation_oid: pg_sys::Oid,
        context: &StorageContext,
        loaded: LoadedTableMetadata,
    ) -> IcebergResult<Self> {
        let schema = loaded.metadata.current_schema().clone();
        let table = Table::builder()
            .file_io(context.file_io().clone())
            .metadata_location(loaded.location)
            .metadata(loaded.metadata)
            .identifier(IcebergTableId::for_relation(relation_oid).into_table_ident())
            .build()?;
        Ok(Self {
            table,
            schema,
            delta: loaded.delta,
        })
    }

    pub(crate) fn schema(&self) -> &Arc<IcebergSchema> {
        &self.schema
    }

    pub(crate) fn properties(&self) -> &HashMap<String, String> {
        self.table.metadata().properties()
    }

    pub(crate) fn into_read_snapshot(self) -> IcebergReadSnapshot {
        IcebergReadSnapshot::new(self.table, self.delta)
    }
}

impl ManagedAnalyzeSnapshot {
    pub(crate) fn load(
        relation_oid: pg_sys::Oid,
        tablespace_oid: pg_sys::Oid,
    ) -> IcebergResult<Self> {
        let (context, loaded) =
            ManagedTableSnapshot::load_metadata(relation_oid, tablespace_oid)?;
        let storage_bytes = loaded.relation_stats(context.file_io())?.1;
        let snapshot =
            ManagedTableSnapshot::from_metadata(relation_oid, &context, loaded)?;
        Ok(Self {
            snapshot,
            storage_bytes,
        })
    }

    pub(crate) fn into_parts(self) -> (IcebergReadSnapshot, u64) {
        (self.snapshot.into_read_snapshot(), self.storage_bytes)
    }
}
