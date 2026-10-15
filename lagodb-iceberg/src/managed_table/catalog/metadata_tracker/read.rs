//! Transaction-visible definitions and complete metadata read views.

use std::rc::Rc;
use std::sync::Arc;

use iceberg_lite::io::FileIO;
use iceberg_lite::spec::TableMetadata;
use lagodb_core::catalog::CatalogSnapshot;
use lagodb_core::handles::SnapshotHandle;
use pgrx::pg_sys;

use super::{
    LoadedTableMetadata, LoadedTableRead, ManagedTableActionLog, TxMetadata,
};
use crate::error::{IcebergError, IcebergResult};
use crate::managed_table::catalog::local_storage::LocalTableRoot;
use crate::managed_table::catalog::metadata_table::IcebergMetadata;

impl TxMetadata {
    /// Current-definition entry point for planning and maintenance.
    ///
    /// Reads the latest committed metadata location every time, then attaches
    /// any transaction-local schema update and file delta for this relation.
    /// Executor reads use `table_metadata_at` instead, retaining their PG
    /// snapshot when choosing the committed metadata pointer.
    pub fn current_table_metadata(
        &self,
        relid: pg_sys::Oid,
        file_io: &FileIO,
    ) -> IcebergResult<LoadedTableMetadata> {
        TableMetadataView::load(self, relid, file_io)?.into_read_metadata()
    }

    /// Data visibility follows the executor snapshot within the current local
    /// storage generation. The read schema follows the current PG definition;
    /// neither definitions nor local TRUNCATE are frozen by the data snapshot.
    pub(crate) fn table_metadata_at(
        &self,
        relid: pg_sys::Oid,
        file_io: &FileIO,
        snapshot: &SnapshotHandle<'_>,
        local_root: Option<&LocalTableRoot>,
    ) -> IcebergResult<LoadedTableRead> {
        let current = IcebergMetadata::get(relid)?;
        let current_location = current
            .metadata_location
            .ok_or(IcebergError::MetadataLocationNull)?;
        // Local CREATE/TRUNCATE bootstrap an empty generation. Repeated
        // TRUNCATE can clear and reuse its directory, so directory equality
        // alone cannot make an earlier JSON valid. The existing rebuild state
        // follows savepoint rollback/RELEASE; all new rows live in the delta.
        let rebuilt = local_root.is_some()
            && self
                .inner
                .borrow()
                .tables
                .get(&relid)
                .is_some_and(|state| state.was_rebuilt());
        // A current catalog definition can exist before the executor's data
        // snapshot can see its metadata row (for example, concurrent CREATE).
        // Absence under that snapshot means an empty committed baseline, not
        // a missing table. Current metadata above remains mandatory.
        let data = if rebuilt {
            None
        } else {
            IcebergMetadata::find_with_snapshot(
                relid,
                CatalogSnapshot::Borrowed(snapshot),
            )?
        };
        let (location, maintenance_due_at, has_data_baseline) = match data {
            Some(data) => {
                let location = data
                    .metadata_location
                    .ok_or(IcebergError::MetadataLocationNull)?;
                if local_root.is_none_or(|root| root.contains_path(&location)) {
                    (location, data.maintenance_due_at, true)
                } else {
                    // PG TRUNCATE exposes the new storage even to old data
                    // snapshots. Its retired directory can already be deleted:
                    // reject the old baseline before opening any JSON there.
                    // Current metadata supplies context, not its newer rows.
                    (current_location.clone(), current.maintenance_due_at, false)
                }
            }
            None => (current_location.clone(), current.maintenance_due_at, false),
        };
        let metadata = TableMetadata::read_from(file_io, &location)?;
        let snapshot_id = if has_data_baseline {
            metadata.current_snapshot_id()
        } else {
            None
        };
        // Read one JSON when the data and definition locations coincide. Schema
        // actions always validate/replay against the current definition.
        let current_metadata = if current_location == location {
            None
        } else {
            Some(TableMetadata::read_from(file_io, &current_location)?)
        };
        let definition = current_metadata.as_ref().unwrap_or(&metadata);
        let actions = TableMetadataView::actions(self, relid, file_io);
        let (read_schema, delta) = match actions {
            Some(actions) => (
                actions.read_schema(definition)?,
                actions.visible_delta(snapshot.command_id())?,
            ),
            None => (Arc::clone(definition.current_schema()), None),
        };
        Ok(LoadedTableRead {
            loaded: LoadedTableMetadata {
                location,
                maintenance_due_at,
                metadata,
                delta,
            },
            read_schema,
            snapshot_id,
        })
    }

    /// Write-side entry point for data mutation.
    ///
    /// Registers the relation with this transaction's tracker (idempotent),
    /// then returns the current definition and transaction-local actions for
    /// writer setup. Target row reads use `table_metadata_at`; their captured
    /// starting snapshot is passed separately to conflict validation.
    ///
    /// This is the supported way for a data writer to obtain its base
    /// snapshot: it bundles `register_table` with the metadata read so a
    /// caller cannot accidentally observe metadata without enrolling the
    /// table in the tracker.
    pub fn begin_table_modify(
        &self,
        relid: pg_sys::Oid,
        file_io: &FileIO,
    ) -> IcebergResult<LoadedTableMetadata> {
        self.register_table(relid);
        self.current_table_metadata(relid, file_io)
    }

    /// DDL entry point for rebuilding a table from its current definition.
    ///
    /// Registers the relation and replays staged schema/property updates onto
    /// the latest catalog metadata, without constructing a file delta that
    /// the replacement generation will discard.
    pub(crate) fn begin_table_definition_change(
        &self,
        relid: pg_sys::Oid,
        file_io: &FileIO,
    ) -> IcebergResult<TableMetadata> {
        self.register_table(relid);
        let view = TableMetadataView::load(self, relid, file_io)?;
        Ok(view.metadata)
    }
}

/// Catalog metadata with the schema/property overlay and the captured action
/// log. File changes are resolved only when consumed as a complete read view.
struct TableMetadataView {
    location: String,
    maintenance_due_at: Option<pg_sys::TimestampTz>,
    metadata: TableMetadata,
    actions: Option<Rc<ManagedTableActionLog>>,
}

impl TableMetadataView {
    fn actions(
        tracker: &TxMetadata,
        relid: pg_sys::Oid,
        file_io: &FileIO,
    ) -> Option<Rc<ManagedTableActionLog>> {
        let mut inner = tracker.inner.borrow_mut();
        inner.tables.get_mut(&relid).map(|state| {
            if state.file_io.is_none() {
                state.file_io = Some(file_io.clone());
            }
            Rc::clone(&state.transaction.actions)
        })
    }

    fn load(
        tracker: &TxMetadata,
        relid: pg_sys::Oid,
        file_io: &FileIO,
    ) -> IcebergResult<Self> {
        let actions = Self::actions(tracker, relid, file_io);

        let catalog_metadata = IcebergMetadata::get(relid)?;
        let location = catalog_metadata
            .metadata_location
            .ok_or(IcebergError::MetadataLocationNull)?;
        let mut metadata = TableMetadata::read_from(file_io, &location)?;
        if let Some(actions) = &actions {
            metadata = actions.overlay_metadata(metadata)?;
        }
        Ok(Self {
            location,
            maintenance_due_at: catalog_metadata.maintenance_due_at,
            metadata,
            actions,
        })
    }

    fn into_read_metadata(self) -> IcebergResult<LoadedTableMetadata> {
        let delta = match self.actions {
            Some(actions) => actions.combined_delta()?,
            None => None,
        };
        Ok(LoadedTableMetadata {
            location: self.location,
            maintenance_due_at: self.maintenance_due_at,
            metadata: self.metadata,
            delta,
        })
    }
}
