//! Executor-bound remote definitions and command-visible local file actions.

use std::rc::Rc;
use std::sync::Arc;

use iceberg_lite::overlay::SnapshotDelta;
use iceberg_lite::spec::TableMetadata;
use iceberg_lite::table::Table;
use pgrx::pg_sys;

use super::{CURRENT, ForeignTransaction};
use crate::foreign_table::IcebergFdwError;
use crate::foreign_table::catalog::{
    BoundCatalogTable, CatalogAccessIdentity, CatalogRuntimeConfig, ForeignCatalog,
    RemoteTableKey,
};

/// A resolved table and the published data batches visible to one read.
pub(crate) struct ForeignTableReadView {
    key: RemoteTableKey,
    base: Table,
    delta: Option<Arc<SnapshotDelta>>,
}

impl ForeignTableReadView {
    fn base(key: RemoteTableKey, base: Table) -> Self {
        Self::new(key, base, None)
    }

    fn new(
        key: RemoteTableKey,
        base: Table,
        delta: Option<Arc<SnapshotDelta>>,
    ) -> Self {
        Self { key, base, delta }
    }

    pub(crate) fn metadata(&self) -> &TableMetadata {
        self.base.metadata()
    }

    pub(super) fn enrollment_parts(&self) -> (&RemoteTableKey, &Table) {
        (&self.key, &self.base)
    }

    pub(crate) fn into_parts(
        self,
    ) -> (RemoteTableKey, Table, Option<Arc<SnapshotDelta>>) {
        (self.key, self.base, self.delta)
    }
}

impl ForeignTransaction {
    /// Validate existing transaction state before any catalog I/O and reuse its
    /// SQLite connection. REST reuse belongs to the executor, not this write
    /// transaction. A read never enrolls a writer.
    pub(crate) fn catalog_from_transaction(
        catalog_identity: &CatalogAccessIdentity,
        runtime_config: &CatalogRuntimeConfig,
    ) -> Result<Option<ForeignCatalog>, IcebergFdwError> {
        CURRENT.with(|slot| {
            let tracker = slot.borrow().as_ref().map(Rc::clone);
            let Some(tracker) = tracker else {
                return Ok(None);
            };
            let inner = tracker.inner.borrow();
            let Some(catalog) = inner.catalogs.get(catalog_identity) else {
                return Ok(None);
            };
            if catalog.invalidated || &catalog.runtime_config != runtime_config {
                return Err(IcebergFdwError::CatalogConfigurationChanged);
            }
            Ok(catalog.catalog.reuse_transaction_connection())
        })
    }

    /// Whether the table has state that a parallel worker cannot reconstruct.
    pub(crate) fn has_local_actions(key: &RemoteTableKey) -> bool {
        CURRENT.with(|slot| {
            slot.borrow().as_ref().is_some_and(|tracker| {
                tracker
                    .inner
                    .borrow()
                    .catalogs
                    .get(key.catalog_identity())
                    .and_then(|catalog| catalog.tables.get(key))
                    .is_some_and(|state| state.has_catalog_changes())
            })
        })
    }

    /// Bind the resolved table and the published actions visible to the read CID.
    /// Maintenance passes `None` to inspect all staged actions. Active writer
    /// buffers are not readable, and a bound scan never acquires later batches.
    pub(crate) fn scan_view(
        catalog_table: BoundCatalogTable,
        read_command_id: Option<pg_sys::CommandId>,
    ) -> Result<ForeignTableReadView, IcebergFdwError> {
        let (key, session, base) = catalog_table.into_parts();
        CURRENT.with(|slot| {
            let tracker = slot.borrow().as_ref().map(Rc::clone);
            match tracker {
                Some(tracker) => {
                    Self::validate_catalog(&tracker.inner.borrow(), &session)?;
                    tracker.scan_view_parts(key, base, read_command_id)
                }
                None => Ok(ForeignTableReadView::base(key, base)),
            }
        })
    }

    pub(super) fn scan_view_parts(
        &self,
        key: RemoteTableKey,
        base: Table,
        read_command_id: Option<pg_sys::CommandId>,
    ) -> Result<ForeignTableReadView, IcebergFdwError> {
        let inner = self.inner.borrow();
        let state = inner
            .catalogs
            .get(key.catalog_identity())
            .and_then(|catalog| catalog.tables.get(&key));
        let Some(state) = state else {
            return Ok(ForeignTableReadView::base(key, base));
        };
        if !state.is_same_generation(&base) {
            return Err(IcebergFdwError::TableGenerationConflict {
                table: key.publication_name(),
            });
        }
        let actions = Rc::clone(&state.transaction().actions);
        drop(inner);
        let delta = actions.visible_delta(read_command_id)?;
        Ok(ForeignTableReadView::new(key, base, delta))
    }
}
