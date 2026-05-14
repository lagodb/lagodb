use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tracing::warn;

use crate::backend::RegisteredStore;
use crate::cache::Residency;
use crate::config::DEFAULT_MAX_OPEN_HANDLES_PER_CONNECTION;
use crate::error::{StorageError, StorageResult};
use crate::handle::{FileHandle, OpenFileState, OpenFlags};
use crate::object::{ObjectInfo, ObjectLocation};

/// Connection-local registry mapping numeric file handles to [`crate::handle::OpenFileState`].
///
/// Dropping a handle releases everything attached to it — the open-slot permit and the `Arc<Residency>`
/// carried on the handle. When the last `Arc<Residency>` goes away (the canonical Arc held by the
/// entry plus any short-lived state clones held by in-flight READ handlers), the embedded cache
/// activity lease drops, and — for large fills — the embedded [`crate::cache::LargeFillSession`]
/// Arc refcount drops. When that last session Arc drops, the session's own `Drop` enqueues a
/// reap request. No external "finalize" step is required.
pub struct HandleTable {
    next_handle_id: AtomicU64,
    handles: Mutex<HashMap<u64, Arc<HandleEntry>>>,
    open_slots: Arc<Semaphore>,
    max_open_handles: usize,
}

struct HandleEntry {
    state: OpenFileState,
    resources: Mutex<Option<HandleResources>>,
    lifecycle: Mutex<HandleLifecycle>,
    read_released: Notify,
}

/// Non-`OpenFileState` resources that must survive until the handle finishes closing.
///
/// The open-slot permit is returned to the connection's semaphore when this value drops.
/// Cache resources (lease, fill session) live inside the `Arc<Residency>` carried on
/// [`OpenFileState`]; they release automatically once every clone of that `Arc` has dropped.
struct HandleResources {
    _slot: OpenHandleSlot,
}

struct HandleLifecycle {
    closing: bool,
    active_reads: usize,
}

pub(crate) struct ReadHandleGuard {
    entry: Arc<HandleEntry>,
}

/// Connection-local bookkeeping returned after [`HandleTable::close`] has passed the CLOSE barrier.
pub(crate) struct ClosedHandle {
    _resources: HandleResources,
}

pub(crate) struct OpenHandleSlot {
    _permit: OwnedSemaphorePermit,
}

pub(crate) struct ReservedOpen {
    pub(crate) slot: OpenHandleSlot,
    pub(crate) key: ObjectLocation,
    pub(crate) store: Arc<RegisteredStore>,
    pub(crate) info: ObjectInfo,
    pub(crate) flags: OpenFlags,
    pub(crate) residency: Option<Arc<Residency>>,
}

impl Default for HandleTable {
    fn default() -> Self {
        Self::with_max_open_handles(DEFAULT_MAX_OPEN_HANDLES_PER_CONNECTION)
    }
}

impl HandleTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_open_handles(max_open_handles: usize) -> Self {
        let max_open_handles = max_open_handles.max(1);
        Self {
            next_handle_id: AtomicU64::new(1),
            handles: Mutex::new(HashMap::new()),
            open_slots: Arc::new(Semaphore::new(max_open_handles)),
            max_open_handles,
        }
    }

    fn lock_handles(&self) -> MutexGuard<'_, HashMap<u64, Arc<HandleEntry>>> {
        self.handles
            .lock()
            .expect("critical handle table mutex poisoned; connection handle state is no longer trustworthy")
    }

    pub fn len(&self) -> usize {
        self.lock_handles().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn reserve_open(&self) -> StorageResult<OpenHandleSlot> {
        match self.open_slots.clone().try_acquire_owned() {
            Ok(permit) => Ok(OpenHandleSlot { _permit: permit }),
            Err(TryAcquireError::NoPermits) => {
                warn!(
                    max_open_handles = self.max_open_handles,
                    "open handle limit exceeded"
                );
                Err(StorageError::resource_exhausted(format!(
                    "open handles per connection limit ({}) exceeded",
                    self.max_open_handles
                )))
            }
            Err(TryAcquireError::Closed) => Err(StorageError::io(
                "open handle limiter closed",
                std::io::Error::other("open handle limiter closed"),
            )),
        }
    }

    /// Convenience for tests: opens a handle without an attached `Residency`.
    ///
    /// Production `OPEN` flow always uses [`Self::open_reserved`]; residency-less handles exist
    /// only so tests can exercise failure modes such as "READ rejects a large handle without a
    /// bound fill session".
    pub fn open(
        &self,
        key: ObjectLocation,
        store: Arc<RegisteredStore>,
        info: ObjectInfo,
        flags: OpenFlags,
    ) -> StorageResult<OpenFileState> {
        let slot = self.reserve_open()?;
        Ok(self.open_reserved(ReservedOpen {
            slot,
            key,
            store,
            info,
            flags,
            residency: None,
        }))
    }

    pub(crate) fn open_reserved(&self, reserved: ReservedOpen) -> OpenFileState {
        let handle = FileHandle(self.next_handle_id.fetch_add(1, Ordering::Relaxed));
        let ReservedOpen {
            slot,
            key,
            store,
            info,
            flags,
            residency,
        } = reserved;
        let state = OpenFileState {
            handle,
            key,
            store,
            size: info.size,
            etag: info.etag,
            flags,
            residency,
        };
        self.lock_handles().insert(
            handle.0,
            Arc::new(HandleEntry {
                state: state.clone(),
                resources: Mutex::new(Some(HandleResources { _slot: slot })),
                lifecycle: Mutex::new(HandleLifecycle {
                    closing: false,
                    active_reads: 0,
                }),
                read_released: Notify::new(),
            }),
        );
        state
    }

    pub fn get(&self, handle: FileHandle) -> StorageResult<OpenFileState> {
        self.lock_handles()
            .get(&handle.0)
            .map(|entry| entry.state.clone())
            .ok_or_else(|| StorageError::closed_handle(handle.0))
    }

    pub(crate) fn begin_read(
        &self,
        handle: FileHandle,
    ) -> StorageResult<ReadHandleGuard> {
        let entry = self
            .lock_handles()
            .get(&handle.0)
            .cloned()
            .ok_or_else(|| StorageError::closed_handle(handle.0))?;
        entry.begin_read(handle)?;
        Ok(ReadHandleGuard { entry })
    }

    /// Completes READ/CLOSE serialization for `handle`, removes it from this table, and returns
    /// the handle's resources wrapped in a [`ClosedHandle`].
    pub(crate) async fn close(
        &self,
        handle: FileHandle,
    ) -> StorageResult<ClosedHandle> {
        let entry = self
            .lock_handles()
            .get(&handle.0)
            .cloned()
            .ok_or_else(|| StorageError::closed_handle(handle.0))?;
        entry.begin_close(handle)?;

        loop {
            let notified = entry.read_released.notified();
            if entry.active_reads() == 0 {
                break;
            }
            notified.await;
        }

        let mut handles = self.lock_handles();
        if handles
            .get(&handle.0)
            .is_some_and(|current| Arc::ptr_eq(current, &entry))
        {
            handles.remove(&handle.0);
        }
        Ok(ClosedHandle {
            _resources: entry.take_resources(handle)?,
        })
    }

    pub(crate) async fn close_all(&self) -> Vec<StorageResult<ClosedHandle>> {
        let handles = self
            .lock_handles()
            .keys()
            .copied()
            .map(FileHandle)
            .collect::<Vec<_>>();
        let mut closed = Vec::with_capacity(handles.len());
        for handle in handles {
            closed.push(self.close(handle).await);
        }
        closed
    }
}

impl HandleEntry {
    fn lock_lifecycle(&self) -> MutexGuard<'_, HandleLifecycle> {
        self.lifecycle
            .lock()
            .expect("critical handle lifecycle mutex poisoned; connection handle state is no longer trustworthy")
    }

    fn lock_resources(&self) -> MutexGuard<'_, Option<HandleResources>> {
        self.resources
            .lock()
            .expect("critical handle resource mutex poisoned; connection handle state is no longer trustworthy")
    }

    fn begin_read(&self, handle: FileHandle) -> StorageResult<()> {
        let mut lifecycle = self.lock_lifecycle();
        if lifecycle.closing {
            return Err(StorageError::closed_handle(handle.0));
        }
        lifecycle.active_reads += 1;
        Ok(())
    }

    fn begin_close(&self, handle: FileHandle) -> StorageResult<()> {
        let mut lifecycle = self.lock_lifecycle();
        if lifecycle.closing {
            return Err(StorageError::closed_handle(handle.0));
        }
        lifecycle.closing = true;
        Ok(())
    }

    fn active_reads(&self) -> usize {
        self.lock_lifecycle().active_reads
    }

    fn release_read(&self) {
        let should_notify = {
            let mut lifecycle = self.lock_lifecycle();
            lifecycle.active_reads = lifecycle.active_reads.saturating_sub(1);
            lifecycle.closing && lifecycle.active_reads == 0
        };
        if should_notify {
            self.read_released.notify_waiters();
        }
    }

    fn take_resources(&self, handle: FileHandle) -> StorageResult<HandleResources> {
        self.lock_resources()
            .take()
            .ok_or_else(|| StorageError::closed_handle(handle.0))
    }
}

impl ReadHandleGuard {
    pub(crate) fn state(&self) -> &OpenFileState {
        &self.entry.state
    }
}

impl Drop for ReadHandleGuard {
    fn drop(&mut self) {
        self.entry.release_read();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::time::timeout;

    use crate::backend::{MemoryObjectBackend, StoreRegistry};
    use crate::object::StoreId;

    use super::*;

    const TEST_STORE_ID: &str = "test-store";

    #[tokio::test]
    async fn close_waits_for_active_read_guard() {
        let handles = Arc::new(HandleTable::new());
        let state = open_test_handle(&handles);
        let read = handles.begin_read(state.handle).unwrap();

        let close_handles = handles.clone();
        let mut close =
            tokio::spawn(async move { close_handles.close(state.handle).await });

        for _ in 0..10 {
            match handles.begin_read(state.handle) {
                Ok(extra_read) => {
                    drop(extra_read);
                    tokio::task::yield_now().await;
                }
                Err(_) => break,
            }
        }
        assert!(handles.begin_read(state.handle).is_err());
        assert!(
            timeout(Duration::from_millis(20), &mut close)
                .await
                .is_err()
        );

        drop(read);
        let closed = timeout(Duration::from_secs(1), close)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(closed);
    }

    #[tokio::test]
    async fn read_guard_releases_close_barrier_on_drop() {
        let handles = Arc::new(HandleTable::new());
        let state = open_test_handle(&handles);
        let read = handles.begin_read(state.handle).unwrap();

        let close_handles = handles.clone();
        let close =
            tokio::spawn(async move { close_handles.close(state.handle).await });

        drop(read);
        let closed = timeout(Duration::from_secs(1), close)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(closed);
        assert!(handles.begin_read(state.handle).is_err());
    }

    fn open_test_handle(handles: &HandleTable) -> OpenFileState {
        let registry = StoreRegistry::new();
        registry
            .register_shared_backend(
                TEST_STORE_ID,
                Arc::new(MemoryObjectBackend::new()),
            )
            .unwrap();
        let registered = registry
            .resolve(&StoreId::new(TEST_STORE_ID).unwrap())
            .unwrap();
        handles
            .open(
                ObjectLocation::new(TEST_STORE_ID, "bucket", "file").unwrap(),
                registered,
                ObjectInfo {
                    size: 8,
                    etag: None,
                },
                OpenFlags::READ_ONLY,
            )
            .unwrap()
    }
}
