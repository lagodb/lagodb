use std::cell::{Cell, RefCell};
use std::rc::Rc;

use pg_lakebase_core::transaction::{self, TransactionResource};
use pg_lakebase_storage::ObjectLocation;
use pgrx::pg_sys;

use crate::error::{IcebergError, IcebergResult};

use super::resource::{ObjectFileState, StorageResource};

const TOP_LEVEL_NEST_LEVEL: i32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MetadataAttemptId(u32);

#[derive(Debug)]
struct TrackedResource {
    nest_level: i32,
    resource: StorageResource,
}

#[derive(Debug)]
struct MetadataAttemptState {
    id: MetadataAttemptId,
    resources: Vec<StorageResource>,
}

/// Metadata materialization resources have a top-level-only lifecycle. They do
/// not carry savepoint nesting state and are never visited by subtransaction
/// callbacks.
#[derive(Debug)]
struct MetadataResourceRegistry {
    active_attempt: Option<MetadataAttemptState>,
    /// Resources selected by a successful catalog publication and retained until
    /// transaction end.
    promoted: Vec<StorageResource>,
    /// Rejected resources whose immediate cleanup failed.
    cleanup_required: Vec<StorageResource>,
    next_attempt_id: u32,
}

impl MetadataResourceRegistry {
    fn new() -> Self {
        Self {
            active_attempt: None,
            promoted: Vec::new(),
            cleanup_required: Vec::new(),
            next_attempt_id: 1,
        }
    }

    fn begin_attempt(&mut self) -> IcebergResult<MetadataAttemptId> {
        // SAFETY: metadata attempts are started from guarded PostgreSQL extension
        // paths while a transaction is active. This only reads backend-local
        // transaction nesting state and does not retain PostgreSQL-owned memory.
        let nest_level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };
        if nest_level != TOP_LEVEL_NEST_LEVEL {
            return Err(IcebergError::InvariantViolated(
                "metadata resource attempts require top-level transaction context",
            ));
        }
        if self.active_attempt.is_some() {
            return Err(IcebergError::InvariantViolated(
                "metadata resource attempts cannot be nested",
            ));
        }

        let id = MetadataAttemptId(self.next_attempt_id);
        self.next_attempt_id = self.next_attempt_id.checked_add(1).ok_or(
            IcebergError::InvariantViolated("metadata resource attempt id overflow"),
        )?;
        self.active_attempt = Some(MetadataAttemptState {
            id,
            resources: Vec::new(),
        });
        Ok(id)
    }

    fn promote_attempt(&mut self, id: MetadataAttemptId) -> IcebergResult<()> {
        let resources = self.take_attempt(id)?;
        self.promoted.extend(resources);
        Ok(())
    }

    fn take_attempt(
        &mut self,
        id: MetadataAttemptId,
    ) -> IcebergResult<Vec<StorageResource>> {
        let Some(attempt) = self.active_attempt.take() else {
            return Err(IcebergError::InvariantViolated(
                "metadata resource attempt is not active",
            ));
        };
        if attempt.id != id {
            self.active_attempt = Some(attempt);
            return Err(IcebergError::InvariantViolated(
                "metadata resource attempt is not active",
            ));
        }
        Ok(attempt.resources)
    }

    fn drain_top_level(&mut self) -> (Vec<StorageResource>, Vec<StorageResource>) {
        let promoted = std::mem::take(&mut self.promoted);
        let mut cleanup_required = std::mem::take(&mut self.cleanup_required);
        if let Some(active) = self.active_attempt.take() {
            cleanup_required.extend(active.resources.into_iter().rev());
        }
        (promoted, cleanup_required)
    }
}

struct TopLevelResources {
    transaction: Vec<TrackedResource>,
    promoted_metadata: Vec<StorageResource>,
    cleanup_metadata: Vec<StorageResource>,
}

#[derive(Debug)]
struct StorageResourceRegistry {
    entries: Vec<TrackedResource>,
    metadata: MetadataResourceRegistry,
}

impl StorageResourceRegistry {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            metadata: MetadataResourceRegistry::new(),
        }
    }

    fn track(&mut self, resource: StorageResource) {
        if let Some(attempt) = self.metadata.active_attempt.as_mut() {
            attempt.resources.push(resource);
            return;
        }
        // SAFETY: storage resources are registered from guarded PostgreSQL
        // extension paths while a transaction is active. This reads the current
        // backend's nesting level and stores only the copied integer value.
        let nest_level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };
        self.entries.push(TrackedResource {
            nest_level,
            resource,
        });
    }

    fn begin_metadata_attempt(&mut self) -> IcebergResult<MetadataAttemptId> {
        self.metadata.begin_attempt()
    }

    fn promote_metadata_attempt(
        &mut self,
        id: MetadataAttemptId,
    ) -> IcebergResult<()> {
        self.metadata.promote_attempt(id)
    }

    fn take_metadata_attempt_resources(
        &mut self,
        id: MetadataAttemptId,
    ) -> IcebergResult<Vec<StorageResource>> {
        self.metadata.take_attempt(id)
    }

    fn current_write_resources(&self) -> impl Iterator<Item = &StorageResource> {
        self.metadata
            .active_attempt
            .iter()
            .flat_map(|attempt| attempt.resources.iter())
            .chain(self.entries.iter().map(|entry| &entry.resource))
    }

    fn current_write_resources_mut(
        &mut self,
    ) -> impl Iterator<Item = &mut StorageResource> {
        self.metadata
            .active_attempt
            .iter_mut()
            .flat_map(|attempt| attempt.resources.iter_mut())
            .chain(self.entries.iter_mut().map(|entry| &mut entry.resource))
    }

    fn ensure_object_file_staged(
        &self,
        location: &ObjectLocation,
    ) -> std::result::Result<(), String> {
        for resource in self.current_write_resources() {
            if let StorageResource::ObjectFile {
                location: tracked_location,
                state,
                ..
            } = resource
                && tracked_location == location
            {
                return match state {
                    ObjectFileState::Staged => Ok(()),
                    ObjectFileState::Uploaded => Err(format!(
                        "object '{}' is already in Uploaded state; \
                         duplicate finalize_write?",
                        location,
                    )),
                };
            }
        }
        Err(format!(
            "no staged entry found for '{}'; \
             finalize_write called without a prior writer() registration",
            location,
        ))
    }

    fn mark_object_file_uploaded(
        &mut self,
        location: &ObjectLocation,
    ) -> std::result::Result<(), String> {
        for resource in self.current_write_resources_mut() {
            if let StorageResource::ObjectFile {
                location: tracked_location,
                state,
                ..
            } = resource
                && tracked_location == location
            {
                if *state == ObjectFileState::Staged {
                    *state = ObjectFileState::Uploaded;
                    return Ok(());
                } else {
                    return Err(format!(
                        "object '{}' is already Uploaded; state machine error",
                        location,
                    ));
                }
            }
        }
        Err(format!(
            "no entry found for '{}' during mark_uploaded",
            location,
        ))
    }

    fn drain_top_level(&mut self) -> TopLevelResources {
        let (promoted_metadata, cleanup_metadata) = self.metadata.drain_top_level();
        TopLevelResources {
            transaction: std::mem::take(&mut self.entries),
            promoted_metadata,
            cleanup_metadata,
        }
    }

    // Savepoint callbacks only affect transaction entries. Metadata resources
    // are top-level-only and intentionally absent from both operations.
    fn promote_subtransaction_resources(&mut self, nest_level: i32) {
        for entry in &mut self.entries {
            if entry.nest_level >= nest_level {
                entry.nest_level = nest_level - 1;
            }
        }
    }

    fn take_aborted_subtransaction_resources(
        &mut self,
        nest_level: i32,
    ) -> Vec<TrackedResource> {
        let mut aborted = Vec::new();
        let mut kept = Vec::new();
        for entry in self.entries.drain(..) {
            if entry.nest_level >= nest_level {
                aborted.push(entry);
            } else {
                kept.push(entry);
            }
        }
        self.entries = kept;
        aborted
    }
}

thread_local! {
    static CURRENT: RefCell<Option<Rc<StorageTransactionResource>>> =
        const { RefCell::new(None) };
}

#[derive(Debug)]
pub(super) struct StorageTransactionResource {
    inner: RefCell<StorageResourceRegistry>,
    nest_level: Cell<i32>,
}

impl StorageTransactionResource {
    pub(super) fn current() -> Rc<Self> {
        CURRENT.with(|slot| {
            let mut slot = slot.borrow_mut();
            if let Some(resource) = slot.as_ref() {
                return Rc::clone(resource);
            }

            let resource = Rc::new(Self {
                inner: RefCell::new(StorageResourceRegistry::new()),
                nest_level: Cell::new(TOP_LEVEL_NEST_LEVEL),
            });
            transaction::register_resource(
                Rc::clone(&resource) as Rc<dyn TransactionResource>
            );
            *slot = Some(Rc::clone(&resource));
            resource
        })
    }

    pub(super) fn track(&self, resource: StorageResource) {
        self.inner.borrow_mut().track(resource);
    }

    pub(super) fn begin_metadata_attempt(&self) -> IcebergResult<MetadataAttemptId> {
        self.inner
            .try_borrow_mut()
            .map_err(|_| {
                IcebergError::InvariantViolated(
                    "storage transaction resource registry is already borrowed",
                )
            })?
            .begin_metadata_attempt()
    }

    pub(super) fn promote_metadata_attempt(
        &self,
        id: MetadataAttemptId,
    ) -> IcebergResult<()> {
        self.inner
            .try_borrow_mut()
            .map_err(|_| {
                IcebergError::InvariantViolated(
                    "storage transaction resource registry is already borrowed",
                )
            })?
            .promote_metadata_attempt(id)
    }

    pub(super) fn discard_metadata_attempt(
        &self,
        id: MetadataAttemptId,
    ) -> IcebergResult<()> {
        let resources = {
            let mut registry = self.inner.try_borrow_mut().map_err(|_| {
                IcebergError::InvariantViolated(
                    "storage transaction resource registry is already borrowed",
                )
            })?;
            registry.take_metadata_attempt_resources(id)?
        };
        self.cleanup_metadata_attempt(resources);
        Ok(())
    }

    fn cleanup_metadata_attempt(&self, resources: Vec<StorageResource>) {
        let cleanup_required: Vec<_> = resources
            .into_iter()
            .rev()
            .filter_map(StorageResource::on_abort)
            .collect();
        if !cleanup_required.is_empty() {
            self.inner
                .borrow_mut()
                .metadata
                .cleanup_required
                .extend(cleanup_required);
        }
    }

    pub(super) fn ensure_object_file_staged(
        location: &ObjectLocation,
    ) -> std::result::Result<(), String> {
        CURRENT.with(|slot| {
            let slot = slot.borrow();
            let resource = slot.as_ref().ok_or_else(|| {
                format!(
                    "storage transaction resource registry not initialized; \
                     finalize_write called without a prior writer() for '{}'",
                    location,
                )
            })?;
            resource.inner.borrow().ensure_object_file_staged(location)
        })
    }

    pub(super) fn mark_object_file_uploaded(
        location: &ObjectLocation,
    ) -> std::result::Result<(), String> {
        CURRENT.with(|slot| {
            let slot = slot.borrow();
            let resource = slot.as_ref().ok_or_else(|| {
                format!(
                    "storage transaction resource registry not initialized during mark_uploaded for '{}'",
                    location,
                )
            })?;
            resource
                .inner
                .borrow_mut()
                .mark_object_file_uploaded(location)
        })
    }
}

impl TransactionResource for StorageTransactionResource {
    fn nest_level(&self) -> i32 {
        self.nest_level.get()
    }

    fn set_nest_level(&self, level: i32) {
        self.nest_level.set(level);
    }

    fn on_commit(&self) {
        let resources = self.inner.borrow_mut().drain_top_level();
        for resource in resources
            .transaction
            .into_iter()
            .map(|entry| entry.resource)
            .chain(resources.promoted_metadata)
        {
            resource.on_commit();
        }
        for resource in resources.cleanup_metadata {
            let _ = resource.on_abort();
        }
        CURRENT.with(|slot| *slot.borrow_mut() = None);
    }

    fn on_abort(&self) {
        let resources = self.inner.borrow_mut().drain_top_level();
        for resource in resources
            .transaction
            .into_iter()
            .map(|entry| entry.resource)
            .chain(resources.promoted_metadata)
            .chain(resources.cleanup_metadata)
        {
            let _ = resource.on_abort();
        }
        CURRENT.with(|slot| *slot.borrow_mut() = None);
    }

    fn on_commit_sub(&self, current_nest_level: i32) {
        self.inner
            .borrow_mut()
            .promote_subtransaction_resources(current_nest_level);
    }

    fn on_abort_sub(&self, current_nest_level: i32) {
        let entries = self
            .inner
            .borrow_mut()
            .take_aborted_subtransaction_resources(current_nest_level);
        for entry in entries {
            let _ = entry.resource.on_abort();
        }
    }
}
