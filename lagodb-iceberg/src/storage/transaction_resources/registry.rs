use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::mem;
use std::rc::Rc;

use lagodb_core::diag::PgReportError;
use lagodb_core::resource::CommittedCleanup;
use lagodb_core::transaction::{self, TransactionResource, TransactionResult};
use lagodb_storage::ObjectLocation;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::error::{IcebergError, IcebergResult};

use super::resource::{CleanupContext, ObjectFileState, StorageResource};

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

    fn has_pending(&self) -> bool {
        self.active_attempt.is_some()
            || !self.promoted.is_empty()
            || !self.cleanup_required.is_empty()
    }

    fn drain_top_level(&mut self) -> (Vec<StorageResource>, Vec<StorageResource>) {
        let promoted = mem::take(&mut self.promoted);
        let mut cleanup_required = mem::take(&mut self.cleanup_required);
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

impl TopLevelResources {
    fn is_empty(&self) -> bool {
        self.transaction.is_empty()
            && self.promoted_metadata.is_empty()
            && self.cleanup_metadata.is_empty()
    }

    fn execute_commit_cleanup(self) {
        for resource in self
            .transaction
            .into_iter()
            .map(|entry| entry.resource)
            .chain(self.promoted_metadata)
        {
            resource.on_commit();
        }
        for resource in self.cleanup_metadata {
            let _ = resource.on_abort(CleanupContext::TransactionEnd);
        }
    }
}

#[derive(Debug)]
struct StorageResourceRegistry {
    entries: Vec<TrackedResource>,
    metadata: MetadataResourceRegistry,
    /// Stable state handles for active staged/uploaded objects, independent of
    /// `entries` / `metadata` Vec positions across savepoint rollbacks and
    /// metadata-attempt promotions.
    staged_objects: HashMap<ObjectLocation, Rc<Cell<ObjectFileState>>>,
}

impl StorageResourceRegistry {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            metadata: MetadataResourceRegistry::new(),
            staged_objects: HashMap::new(),
        }
    }

    fn track(&mut self, resource: StorageResource) {
        self.index_resource(&resource);
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

    fn index_resource(&mut self, resource: &StorageResource) {
        if let StorageResource::ObjectFile {
            location, state, ..
        } = resource
        {
            self.staged_objects
                .entry(location.clone())
                .or_insert_with(|| Rc::clone(state));
        }
    }

    fn unindex_resource(&mut self, resource: &StorageResource) {
        if let StorageResource::ObjectFile {
            location, state, ..
        } = resource
            && self
                .staged_objects
                .get(location)
                .is_some_and(|indexed| Rc::ptr_eq(indexed, state))
        {
            self.staged_objects.remove(location);
        }
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
        let resources = self.metadata.take_attempt(id)?;
        for resource in &resources {
            self.unindex_resource(resource);
        }
        Ok(resources)
    }

    fn ensure_object_file_staged(
        &self,
        location: &ObjectLocation,
    ) -> IcebergResult<()> {
        let Some(state) = self.staged_objects.get(location) else {
            return Err(IcebergError::InvariantViolated(
                "object finalize_write called without a staged resource entry",
            ));
        };
        match state.get() {
            ObjectFileState::Staged => Ok(()),
            ObjectFileState::Uploaded => Err(IcebergError::InvariantViolated(
                "object finalize_write called for an already uploaded resource",
            )),
        }
    }

    fn mark_object_file_uploaded(
        &mut self,
        location: &ObjectLocation,
    ) -> IcebergResult<()> {
        let Some(state) = self.staged_objects.get(location) else {
            return Err(IcebergError::InvariantViolated(
                "object upload completion called without a staged resource entry",
            ));
        };
        match state.get() {
            ObjectFileState::Staged => {
                state.set(ObjectFileState::Uploaded);
                Ok(())
            }
            ObjectFileState::Uploaded => Err(IcebergError::InvariantViolated(
                "object upload completion called for an already uploaded resource",
            )),
        }
    }

    fn has_pending(&self) -> bool {
        !self.entries.is_empty() || self.metadata.has_pending()
    }

    fn drain_top_level(&mut self) -> TopLevelResources {
        self.staged_objects.clear();
        let (promoted_metadata, cleanup_metadata) = self.metadata.drain_top_level();
        TopLevelResources {
            transaction: mem::take(&mut self.entries),
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
        let aborted: Vec<TrackedResource> = self
            .entries
            .extract_if(.., |entry| entry.nest_level >= nest_level)
            .collect();
        for entry in &aborted {
            self.unindex_resource(&entry.resource);
        }
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
            .filter_map(|resource| resource.on_abort(CleanupContext::Statement))
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
    ) -> IcebergResult<()> {
        CURRENT.with(|slot| {
            let slot = slot.borrow();
            let resource = slot.as_ref().ok_or(IcebergError::InvariantViolated(
                "storage transaction resource registry not initialized before finalize_write",
            ))?;
            resource.inner.borrow().ensure_object_file_staged(location)
        })
    }

    pub(super) fn mark_object_file_uploaded(
        location: &ObjectLocation,
    ) -> IcebergResult<()> {
        CURRENT.with(|slot| {
            let slot = slot.borrow();
            let resource = slot.as_ref().ok_or(IcebergError::InvariantViolated(
                "storage transaction resource registry not initialized during upload completion",
            ))?;
            resource
                .inner
                .borrow_mut()
                .mark_object_file_uploaded(location)
        })
    }
}

impl TransactionResource for StorageTransactionResource {
    fn nest_level(&self) -> i32 {
        TOP_LEVEL_NEST_LEVEL
    }

    fn set_nest_level(&self, _level: i32) {}

    fn on_pre_prepare(&self) -> TransactionResult<()> {
        if !self.inner.borrow().has_pending() {
            return Ok(());
        }
        Err(PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            "cannot prepare a transaction with pending external-object cleanup",
        ))
    }

    fn on_commit(&self) {
        let resources = self.inner.borrow_mut().drain_top_level();
        CURRENT.with(|slot| *slot.borrow_mut() = None);
        // Physical post-commit deletion (including staging-file unlinks) runs
        // after lock release, as the transaction resource contract requires.
        if !resources.is_empty() {
            CommittedCleanup::defer(move || resources.execute_commit_cleanup());
        }
    }

    fn on_abort(&self) {
        let resources = self.inner.borrow_mut().drain_top_level();
        CURRENT.with(|slot| *slot.borrow_mut() = None);
        for resource in resources
            .transaction
            .into_iter()
            .map(|entry| entry.resource)
            .chain(resources.promoted_metadata)
            .chain(resources.cleanup_metadata)
        {
            let _ = resource.on_abort(CleanupContext::TransactionEnd);
        }
    }

    fn on_prepare(&self) {
        // PRE_PREPARE accepted only an empty registry; release the backend reference.
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
            let _ = entry.resource.on_abort(CleanupContext::TransactionEnd);
        }
    }
}
