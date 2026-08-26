use std::cell::UnsafeCell;
use std::ffi::{CStr, c_long};
use std::marker::PhantomData;
use std::mem::{offset_of, size_of};
use std::ptr::{NonNull, from_mut, null_mut};

use pgrx::pg_sys;
use pgrx::{PGRXSharedMemory, PgSharedMemoryInitialization};

use crate::worker::state::{CoordinatorSlot, Slot, WorkerKey};

/// Typed access to a PostgreSQL shared dynahash. Worker state transitions hold
/// `SHARED_STATE` while calling this object, so related hashes and scalar
/// supervisor state share one atomic lock boundary.
struct PostgresSharedHash<K, E> {
    name: &'static CStr,
    raw: UnsafeCell<*mut pg_sys::HTAB>,
    _types: PhantomData<(K, E)>,
}

// SAFETY: the pointer is assigned by the postmaster startup hook and inherited
// by its children. All table access is serialized by the runtime LWLock.
unsafe impl<K, E> Sync for PostgresSharedHash<K, E> {}

impl<K, E: Copy + PGRXSharedMemory> PostgresSharedHash<K, E> {
    const fn new(name: &'static CStr) -> Self {
        Self {
            name,
            raw: UnsafeCell::new(null_mut()),
            _types: PhantomData,
        }
    }

    fn find(&self, key: &K) -> Option<E> {
        let entry = self.search(key, pg_sys::HASHACTION::HASH_FIND, None);
        // SAFETY: `search` returns either null or a table entry of type `E`;
        // the caller holds the runtime LWLock for the copy.
        unsafe { entry.as_ref().copied() }
    }

    fn with_mut<R>(&self, key: &K, operation: impl FnOnce(&mut E) -> R) -> Option<R> {
        let entry = self.search(key, pg_sys::HASHACTION::HASH_FIND, None);
        // SAFETY: the runtime exclusive LWLock gives this call unique access
        // to a non-null table entry for the duration of `operation`.
        unsafe { entry.as_mut().map(operation) }
    }

    fn get_or_insert(&self, key: &K, initialize: impl FnOnce() -> E) -> E {
        let mut found = false;
        let entry =
            self.search(key, pg_sys::HASHACTION::HASH_ENTER, Some(&mut found));
        let entry =
            NonNull::new(entry).expect("PostgreSQL HASH_ENTER returned a null entry");
        if !found {
            // SAFETY: HASH_ENTER returned a newly allocated entry whose full
            // `E` storage must be initialized before it is read. The typed
            // initializer repeats the same leading key used for the lookup.
            unsafe { entry.as_ptr().write(initialize()) };
        }
        // SAFETY: an existing entry was already initialized, and the new-entry
        // branch initialized the complete value above.
        unsafe { *entry.as_ref() }
    }

    fn replace(&self, key: K, entry: E) -> bool {
        self.with_mut(&key, |current| *current = entry).is_some()
    }

    fn remove(&self, key: &K) -> bool {
        !self
            .search(key, pg_sys::HASHACTION::HASH_REMOVE, None)
            .is_null()
    }

    fn snapshots(&self) -> Vec<E> {
        let mut entries = Vec::new();
        self.for_each(|entry| entries.push(*entry));
        entries
    }

    fn for_each(&self, mut operation: impl FnMut(&E)) {
        let mut status = pg_sys::HASH_SEQ_STATUS::default();
        // SAFETY: the initialized hash remains stable under the runtime LWLock.
        unsafe { pg_sys::hash_seq_init(&mut status, self.raw()) };
        loop {
            // SAFETY: `status` belongs to this hash scan and is advanced only
            // here while structural table changes are excluded by the lock.
            let entry = unsafe { pg_sys::hash_seq_search(&mut status) }.cast::<E>();
            // SAFETY: non-null sequence results point to initialized `E` entries.
            let Some(entry) = (unsafe { entry.as_ref() }) else {
                break;
            };
            operation(entry);
        }
    }

    fn for_each_mut(&self, mut operation: impl FnMut(&mut E)) {
        let mut status = pg_sys::HASH_SEQ_STATUS::default();
        // SAFETY: the initialized hash remains stable under the runtime LWLock.
        unsafe { pg_sys::hash_seq_init(&mut status, self.raw()) };
        loop {
            // SAFETY: `status` belongs to this hash scan and the exclusive
            // runtime lock prevents aliases to mutable table entries.
            let entry = unsafe { pg_sys::hash_seq_search(&mut status) }.cast::<E>();
            // SAFETY: non-null sequence results point to initialized `E` entries.
            let Some(entry) = (unsafe { entry.as_mut() }) else {
                break;
            };
            operation(entry);
        }
    }

    fn search(
        &self,
        key: &K,
        action: pg_sys::HASHACTION::Type,
        found: Option<&mut bool>,
    ) -> *mut E {
        let raw = self.raw();
        let key = (key as *const K).cast();
        let found = found.map_or(null_mut(), from_mut);
        // SAFETY: `raw` is an initialized PostgreSQL hash, `key` has the
        // table's fixed key type, and the runtime LWLock is held. Calling
        // `hash_search_with_hash_value` avoids macOS libc's unrelated
        // `hash_search` symbol while preserving PostgreSQL dynahash semantics.
        unsafe {
            let hash = pg_sys::get_hash_value(raw, key);
            pg_sys::hash_search_with_hash_value(raw, key, hash, action, found).cast()
        }
    }

    fn raw(&self) -> *mut pg_sys::HTAB {
        // SAFETY: the startup hook is the only writer and publishes the pointer
        // before child backends can access the runtime.
        let raw = unsafe { *self.raw.get() };
        assert!(!raw.is_null(), "shared hash was not initialized");
        raw
    }

    /// # Safety
    ///
    /// Must run from PostgreSQL's shared-memory request hook.
    unsafe fn request_shared_memory(&self) {
        let (_, maximum_entries) = Self::entry_limits();
        // SAFETY: entry count and entry size describe this fixed shared hash.
        let size =
            unsafe { pg_sys::hash_estimate_size(maximum_entries, size_of::<E>()) };
        // SAFETY: the caller guarantees shared-memory request-hook context.
        unsafe { pg_sys::RequestAddinShmemSpace(size) };
    }

    /// # Safety
    ///
    /// Must run from PostgreSQL's shared-memory startup hook.
    unsafe fn initialize(&self) {
        let (initial_entries, maximum_entries) = Self::entry_limits();
        // SAFETY: index 21 is PostgreSQL 17's AddinShmemInitLock, matching the
        // pgrx shared-memory implementation used by this build.
        let lock = unsafe { &raw mut (*pg_sys::MainLWLockArray.add(21)).lock };
        // SAFETY: startup hook context permits acquiring AddinShmemInitLock.
        unsafe { pg_sys::LWLockAcquire(lock, pg_sys::LWLockMode::LW_EXCLUSIVE) };
        let mut control = pg_sys::HASHCTL {
            keysize: size_of::<K>(),
            entrysize: size_of::<E>(),
            ..Default::default()
        };
        let flags = i32::try_from(pg_sys::HASH_ELEM | pg_sys::HASH_BLOBS)
            .expect("PostgreSQL hash flags exceed i32");
        // SAFETY: AddinShmemInitLock is held and HASHCTL describes the complete
        // fixed entry/key layout requested during the request hook.
        let raw = unsafe {
            pg_sys::ShmemInitHash(
                self.name.as_ptr(),
                initial_entries,
                maximum_entries,
                &mut control,
                flags,
            )
        };
        // SAFETY: startup initialization is the sole writer of the inherited
        // backend-local HTAB pointer.
        unsafe { *self.raw.get() = raw };
        // SAFETY: `lock` was acquired by this function above.
        unsafe { pg_sys::LWLockRelease(lock) };
    }

    fn entry_limits() -> (c_long, c_long) {
        // SAFETY: PostgreSQL initializes this postmaster GUC before invoking
        // shared-memory request and startup hooks.
        let initial = c_long::from(unsafe { pg_sys::max_worker_processes });
        (initial, initial * 2)
    }
}

pub(crate) struct CoordinatorTable(PostgresSharedHash<u32, CoordinatorSlot>);

impl CoordinatorTable {
    pub(crate) const fn new() -> Self {
        Self(PostgresSharedHash::new(c"lagodb coordinator hash"))
    }

    pub(crate) fn find(&self, database_oid: u32) -> Option<CoordinatorSlot> {
        self.0.find(&database_oid)
    }

    pub(crate) fn with_mut<R>(
        &self,
        database_oid: u32,
        operation: impl FnOnce(&mut CoordinatorSlot) -> R,
    ) -> Option<R> {
        self.0.with_mut(&database_oid, operation)
    }

    pub(crate) fn get_or_insert(&self, database_oid: u32) -> CoordinatorSlot {
        self.0
            .get_or_insert(&database_oid, || CoordinatorSlot::new(database_oid))
    }

    pub(crate) fn replace(&self, entry: CoordinatorSlot) -> bool {
        self.0.replace(entry.database_oid, entry)
    }

    pub(crate) fn remove(&self, database_oid: u32) -> bool {
        self.0.remove(&database_oid)
    }
    pub(crate) fn snapshots(&self) -> Vec<CoordinatorSlot> {
        self.0.snapshots()
    }
    pub(crate) fn for_each(&self, operation: impl FnMut(&CoordinatorSlot)) {
        self.0.for_each(operation)
    }
    pub(crate) fn for_each_mut(&self, operation: impl FnMut(&mut CoordinatorSlot)) {
        self.0.for_each_mut(operation)
    }
}

impl PgSharedMemoryInitialization for CoordinatorTable {
    type Value = ();

    unsafe fn on_shmem_request(&'static self) {
        // SAFETY: pgrx invokes this method from PostgreSQL's request hook.
        unsafe { self.0.request_shared_memory() }
    }

    unsafe fn on_shmem_startup(&'static self, (): Self::Value) {
        // SAFETY: pgrx invokes this method from PostgreSQL's startup hook.
        unsafe { self.0.initialize() }
    }
}

pub(crate) struct WorkerTable(PostgresSharedHash<WorkerKey, Slot>);

impl WorkerTable {
    pub(crate) const fn new() -> Self {
        Self(PostgresSharedHash::new(c"lagodb worker hash"))
    }

    pub(crate) fn find(&self, key: WorkerKey) -> Option<Slot> {
        self.0.find(&key)
    }
    pub(crate) fn with_mut<R>(
        &self,
        key: WorkerKey,
        operation: impl FnOnce(&mut Slot) -> R,
    ) -> Option<R> {
        self.0.with_mut(&key, operation)
    }
    pub(crate) fn get_or_insert(&self, key: WorkerKey) -> Slot {
        self.0.get_or_insert(&key, || Slot::new(key))
    }
    pub(crate) fn replace(&self, entry: Slot) -> bool {
        self.0.replace(entry.key(), entry)
    }
    pub(crate) fn remove(&self, key: WorkerKey) -> bool {
        self.0.remove(&key)
    }
    pub(crate) fn snapshots(&self) -> Vec<Slot> {
        self.0.snapshots()
    }
    pub(crate) fn for_each(&self, operation: impl FnMut(&Slot)) {
        self.0.for_each(operation)
    }
    pub(crate) fn for_each_mut(&self, operation: impl FnMut(&mut Slot)) {
        self.0.for_each_mut(operation)
    }
}

impl PgSharedMemoryInitialization for WorkerTable {
    type Value = ();

    unsafe fn on_shmem_request(&'static self) {
        // SAFETY: pgrx invokes this method from PostgreSQL's request hook.
        unsafe { self.0.request_shared_memory() }
    }

    unsafe fn on_shmem_startup(&'static self, (): Self::Value) {
        // SAFETY: pgrx invokes this method from PostgreSQL's startup hook.
        unsafe { self.0.initialize() }
    }
}

const _: () = assert!(offset_of!(CoordinatorSlot, database_oid) == 0);
const _: () = assert!(offset_of!(Slot, database_oid) == 0);
const _: () = assert!(offset_of!(Slot, worker_id) == size_of::<u32>());
const _: () = assert!(size_of::<WorkerKey>() == size_of::<u64>());
