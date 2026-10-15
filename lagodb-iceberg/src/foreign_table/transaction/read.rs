//! Executor-bound remote definitions and command-visible local file actions.

use std::rc::Rc;
use std::sync::Arc;

use iceberg_lite::overlay::SnapshotDelta;
use iceberg_lite::table::Table;
use pgrx::pg_sys;

use super::{CURRENT, ForeignTransaction};
use crate::foreign_table::catalog::ForeignCatalog;
use crate::foreign_table::options::{CatalogRuntimeConfig, ServerBindingKey};
use crate::foreign_table::{IcebergFdwError, IcebergForeignTable, RemoteTableKey};

/// A table plus the file overlay visible to a statement in this backend.
pub(crate) struct ForeignTableView {
    pub(crate) key: RemoteTableKey,
    pub(crate) table: Table,
    pub(crate) delta: Option<Arc<SnapshotDelta>>,
}

impl ForeignTransaction {
    /// Validate an existing write binding before any catalog I/O and reuse its
    /// connection when the backend supports it. A read never enrolls a writer.
    pub(crate) fn catalog_for_binding(
        server_key: &ServerBindingKey,
        runtime_config: &CatalogRuntimeConfig,
    ) -> Result<Option<ForeignCatalog>, IcebergFdwError> {
        CURRENT.with(|slot| {
            let tracker = slot.borrow().as_ref().map(Rc::clone);
            let Some(tracker) = tracker else {
                return Ok(None);
            };
            let inner = tracker.inner.borrow();
            let Some(binding) = inner.catalogs.get(server_key) else {
                return Ok(None);
            };
            if &binding.runtime_config != runtime_config {
                return Err(IcebergFdwError::CatalogBindingChanged);
            }
            Ok(binding.catalog.reuse_connection())
        })
    }

    /// Inspect the current action log without enrolling a table or transaction.
    pub(crate) fn has_local_actions(key: &RemoteTableKey) -> bool {
        CURRENT.with(|slot| {
            slot.borrow().as_ref().is_some_and(|tracker| {
                tracker
                    .inner
                    .borrow()
                    .catalogs
                    .get(key.catalog_binding())
                    .and_then(|binding| binding.tables.get(key))
                    .is_some_and(|state| !state.transaction.actions.is_empty())
            })
        })
    }

    /// Bind the resolved table and the published actions visible to the read CID.
    /// Maintenance passes `None` to inspect all staged actions. Active writer
    /// buffers are not readable, and a bound scan never acquires later batches.
    pub(crate) fn scan_view(
        resolved: IcebergForeignTable,
        read_command_id: Option<pg_sys::CommandId>,
    ) -> Result<ForeignTableView, IcebergFdwError> {
        CURRENT.with(|slot| {
            let tracker = slot.borrow().as_ref().map(Rc::clone);
            match tracker {
                Some(tracker) => tracker.overlay(resolved, read_command_id),
                None => {
                    let (key, _binding, table) = resolved.into_parts();
                    Ok(ForeignTableView {
                        key,
                        table,
                        delta: None,
                    })
                }
            }
        })
    }

    fn overlay(
        &self,
        resolved: IcebergForeignTable,
        read_command_id: Option<pg_sys::CommandId>,
    ) -> Result<ForeignTableView, IcebergFdwError> {
        let (key, binding, table) = resolved.into_parts();
        Self::validate_binding(&self.inner.borrow(), &binding)?;
        self.overlay_parts(key, table, read_command_id)
    }

    pub(super) fn overlay_parts(
        &self,
        key: RemoteTableKey,
        table: Table,
        read_command_id: Option<pg_sys::CommandId>,
    ) -> Result<ForeignTableView, IcebergFdwError> {
        let actions = self
            .inner
            .borrow()
            .catalogs
            .get(key.catalog_binding())
            .and_then(|binding| binding.tables.get(&key))
            .map(|state| Rc::clone(&state.transaction.actions));
        let Some(actions) = actions else {
            return Ok(ForeignTableView {
                key,
                table,
                delta: None,
            });
        };
        let metadata = actions.overlay_metadata(table.metadata().clone())?;
        let delta = actions.visible_delta(read_command_id)?;
        let mut builder = Table::builder()
            .identifier(table.identifier().clone())
            .file_io(table.file_io().clone())
            .metadata(metadata);
        if let Some(location) = table.metadata_location() {
            builder = builder.metadata_location(location.to_owned());
        }
        Ok(ForeignTableView {
            key,
            table: builder.build()?,
            delta,
        })
    }
}
