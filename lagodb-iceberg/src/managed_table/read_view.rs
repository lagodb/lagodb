//! Transaction-visible managed-table metadata and scan snapshot construction.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg_lite::spec::Schema as IcebergSchema;
use iceberg_lite::table::Table;
use lagodb_core::handles::{RelationGuard, SnapshotHandle};
use pgrx::pg_sys;

use crate::error::IcebergResult;
use crate::managed_table::catalog::bridge::IcebergTableId;
use crate::managed_table::catalog::local_storage::LocalTableRoot;
use crate::managed_table::catalog::metadata_tracker::{
    LoadedTableMetadata, LoadedTableRead, TxMetadata,
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
    read: LoadedTableRead,
}

impl ManagedTableReadView {
    /// Current definition for planner and maintenance callers.
    pub(crate) fn load(relation_oid: pg_sys::Oid) -> IcebergResult<Self> {
        Self::load_visible(relation_oid, None)
    }

    /// Bind an executor read to the metadata row visible to its PG snapshot.
    /// The delta contains only batches already published at binding time;
    /// Start and rescan do not attach later batches. Nested reads during an
    /// unfinished write do not provide heap-equivalent visibility; see the
    /// catalog README's nested-read scope.
    pub(crate) fn for_snapshot(
        relation_oid: pg_sys::Oid,
        snapshot: &SnapshotHandle<'_>,
    ) -> IcebergResult<Self> {
        Self::load_visible(relation_oid, Some(snapshot))
    }

    fn load_visible(
        relation_oid: pg_sys::Oid,
        snapshot: Option<&SnapshotHandle<'_>>,
    ) -> IcebergResult<Self> {
        PgTransactionIsolation::current()?;
        // Every caller is inside planning or execution with PostgreSQL's
        // relation lock already held. Acquire only the relcache reference
        // needed to resolve the managed relation's storage policy.
        let relation = RelationGuard::open_table(
            relation_oid,
            pg_sys::NoLock as pg_sys::LOCKMODE,
        )?;
        let context = StorageContext::for_read(&relation.as_handle())?;
        let tracker = TxMetadata::current();
        let read = match snapshot {
            Some(snapshot) => {
                let local_root = context
                    .local_storage()
                    .map(|_| LocalTableRoot::for_relation(&relation.as_handle()))
                    .transpose()?;
                tracker.table_metadata_at(
                    relation_oid,
                    context.file_io(),
                    snapshot,
                    local_root.as_ref(),
                )?
            }
            None => {
                let loaded = tracker
                    .current_table_metadata(relation_oid, context.file_io())?;
                LoadedTableRead::current(loaded)
            }
        };
        Ok(Self {
            relation_oid,
            context,
            read,
        })
    }

    pub(crate) fn schema(&self) -> &Arc<IcebergSchema> {
        &self.read.read_schema
    }

    pub(crate) fn properties(&self) -> &HashMap<String, String> {
        self.read.loaded.metadata.properties()
    }

    pub(crate) fn storage_bytes(&self) -> IcebergResult<u64> {
        Ok(self.read.loaded.relation_stats(self.context.file_io())?.1)
    }

    pub(crate) fn into_read_snapshot(self) -> IcebergResult<IcebergReadSnapshot> {
        let Self {
            relation_oid,
            context,
            read,
        } = self;
        let LoadedTableRead {
            loaded,
            read_schema,
            snapshot_id,
        } = read;
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
        Ok(IcebergReadSnapshot::with_baseline(
            table,
            delta,
            read_schema,
            snapshot_id,
        ))
    }
}
