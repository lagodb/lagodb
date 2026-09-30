//! Transaction-visible definitions and complete metadata read views.

use std::rc::Rc;

use iceberg_lite::io::FileIO;
use iceberg_lite::spec::TableMetadata;
use pgrx::pg_sys;

use super::{LoadedTableMetadata, ManagedTableActionLog, TxMetadata};
use crate::error::{IcebergError, IcebergResult};
use crate::managed_table::catalog::metadata_table::IcebergMetadata;

impl TxMetadata {
    /// Read-side entry point for scans and planner statistics.
    ///
    /// Reads the latest committed metadata location every time, then attaches
    /// any transaction-local schema update and file delta for this relation.
    /// That gives Read Committed behavior without writing statement-time
    /// metadata files.
    pub fn current_table_metadata(
        &self,
        relid: pg_sys::Oid,
        file_io: &FileIO,
    ) -> IcebergResult<LoadedTableMetadata> {
        TableMetadataView::load(self, relid, file_io)?.into_read_metadata()
    }

    /// Write-side entry point for data mutation.
    ///
    /// Registers the relation with this transaction's tracker (idempotent),
    /// then returns the latest committed metadata plus any prior
    /// transaction-local schema update and file delta for statement-local
    /// reads.
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
        Ok(TableMetadataView::load(self, relid, file_io)?.metadata)
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
    fn load(
        tracker: &TxMetadata,
        relid: pg_sys::Oid,
        file_io: &FileIO,
    ) -> IcebergResult<Self> {
        let actions = {
            let mut inner = tracker.inner.borrow_mut();
            match inner.tables.get_mut(&relid) {
                Some(state) => {
                    if state.file_io.is_none() {
                        state.file_io = Some(file_io.clone());
                    }
                    Some(Rc::clone(&state.transaction.actions))
                }
                None => None,
            }
        };

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
