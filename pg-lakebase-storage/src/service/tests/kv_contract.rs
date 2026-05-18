//! End-to-end KV-access contract for the OPEN/READ paths.
//!
//! These tests exercise the full `StorageService` + `CacheManager` + `PersistentCacheIndex` stack
//! wrapped in the counting KV harness from
//! [`crate::cache::index::persistent::test_support`]. Each test issues an OPEN (and where
//! applicable a sequence of READs) and asserts the exact number of KV transactions and per-table
//! operations observed.
//!
//! # Why end-to-end and not just index-level
//!
//! Index-level tests cover one trait method at a time. These tests guard the **composition** —
//! `lookup_for_open` + `admit_small_if_absent` or `open_hit` + zero-KV READ — which is the thing
//! that actually matters for end-user performance. The table below spells out the contract; any
//! regression in the cache/service state machine flips a count and fails a specific assertion.
//!
//! # Performance contract being asserted (touch granularity = 60 s unless stated otherwise)
//!
//! | Path                                    | OPEN txns         | READ txns (per call) |
//! |-----------------------------------------|-------------------|----------------------|
//! | Small cold (miss → HEAD+GET → admit)    | 1 read + 1 write  | 0                    |
//! | Small warm, inside window               | 1 read            | 0                    |
//! | Small warm, cross window (touch)        | 1 read + 1 write  | 0                    |
//! | Complete warm, inside window            | 1 read            | 0                    |
//! | Complete warm, cross window (touch)    | 1 read + 1 write  | 0                    |
//! | Large cold fill (first OPEN)            | 1 read            | see large-fill case  |
//! | Large cold fill promote                 | —                 | adds 1 write at promote |
//!
//! Each row gets exactly one `#[tokio::test]`.

use std::sync::Arc;
use std::time::Duration;

use crate::backend::{MemoryObjectBackend, StoreRegistry};
use crate::cache::CacheManager;
use crate::cache::index::persistent::RedbKv;
use crate::cache::index::persistent::test_support::{
    CountingKv, KvCounts, KvCountsSnapshot, counting_redb_index, unique_redb_path,
};
use crate::config::{CacheRuntimeConfig, StorageRuntime, StorageRuntimeConfig};
use crate::handle::OpenFlags;
use crate::object::ObjectLocation;
use crate::service::StorageService;
use crate::service::command::{
    CloseCommand, OpenCommand, ReadCommand, StorageCommand,
};
use crate::service::reply::CommandOutput;
use crate::session::handle_table::HandleTable;

use super::fixtures::{BUCKET, DEFAULT_STORE, LARGE_KEY, SMALL_KEY, test_cache_dir};

const WARM_TOUCH_GRANULARITY: Duration = Duration::from_secs(60);
const IMMEDIATE_TOUCH_GRANULARITY: Duration = Duration::ZERO;

/// Service stack wired on top of a [`CountingKv`]-instrumented redb index.
type InstrumentedService = StorageService<
    crate::cache::index::persistent::PersistentCacheIndex<CountingKv<RedbKv>>,
>;

struct Harness {
    service: InstrumentedService,
    handles: HandleTable,
    counts: Arc<KvCounts>,
}

impl Harness {
    async fn from_backend(
        backend: Arc<MemoryObjectBackend>,
        touch_granularity: Duration,
    ) -> Self {
        let (index, counts) = counting_redb_index(unique_redb_path("contract"));
        let runtime_cfg = StorageRuntimeConfig {
            cache: CacheRuntimeConfig {
                touch_granularity,
                ..CacheRuntimeConfig::default()
            },
        };
        let runtime = StorageRuntime::new(runtime_cfg).unwrap();
        let cache = Arc::new(
            CacheManager::new(test_cache_dir(), index, runtime).with_limits(8, 4),
        );
        cache.spawn_large_fill_reaper();
        let service = StorageService::with_registry_config(
            StoreRegistry::new()
                .with_shared_backend(DEFAULT_STORE, backend)
                .unwrap(),
            cache,
            crate::config::StorageServiceConfig::default(),
        );
        let handles = HandleTable::new();
        Self {
            service,
            handles,
            counts,
        }
    }

    async fn open(&self, key: &str) -> crate::handle::FileHandle {
        let reply = self
            .service
            .execute(
                &self.handles,
                StorageCommand::Open(OpenCommand {
                    store_id: DEFAULT_STORE.to_string(),
                    bucket: BUCKET.to_string(),
                    key: key.to_string(),
                    flags: OpenFlags::READ_ONLY,
                }),
            )
            .await
            .unwrap();
        let CommandOutput::Open(output) = reply.output else {
            panic!("unexpected open output");
        };
        output.handle
    }

    async fn read(&self, handle: crate::handle::FileHandle, offset: u64, len: u32) {
        let reply = self
            .service
            .execute(
                &self.handles,
                StorageCommand::Read(ReadCommand {
                    handle,
                    offset,
                    len,
                }),
            )
            .await
            .unwrap();
        let CommandOutput::Read(output) = reply.output else {
            panic!("unexpected read output");
        };
        let (_bytes, _eof) = output.into_bytes().await.unwrap();
    }

    async fn close(&self, handle: crate::handle::FileHandle) {
        self.service
            .execute(
                &self.handles,
                StorageCommand::Close(CloseCommand { handle }),
            )
            .await
            .unwrap();
    }
}

fn location(key: &str) -> ObjectLocation {
    ObjectLocation::new(DEFAULT_STORE, BUCKET, key).unwrap()
}

fn seed_backend(backend: &Arc<MemoryObjectBackend>, key: &str, data: &[u8]) {
    backend.insert(location(key), data.to_vec());
}

/// Cold small OPEN + warm READ: one miss read-txn, one insert-if-absent write-txn, zero reads
/// touching KV.
#[tokio::test]
async fn small_cold_open_plus_read_has_one_read_txn_and_one_write_txn() {
    let backend = Arc::new(MemoryObjectBackend::new());
    backend.insert(location(SMALL_KEY), b"abc".to_vec());
    let harness = Harness::from_backend(backend, WARM_TOUCH_GRANULARITY).await;

    harness.counts.reset();
    let handle = harness.open(SMALL_KEY).await;
    let after_open = harness.counts.snapshot();
    assert_eq!(
        after_open.read_txns, 1,
        "lookup miss must be exactly one read txn"
    );
    assert_eq!(
        after_open.write_txns, 1,
        "admit must be exactly one write txn"
    );
    assert_eq!(
        after_open.meta_get, 2,
        "one miss read + one insert-if-absent check"
    );
    assert_eq!(after_open.small_get, 0);
    assert_eq!(after_open.small_put, 1);
    assert_eq!(after_open.meta_put, 1);
    assert_eq!(after_open.lru_put, 1);
    assert_eq!(after_open.lru_remove, 0);

    harness.counts.reset();
    harness.read(handle, 0, 3).await;
    let after_read = harness.counts.snapshot();
    assert_eq!(
        after_read,
        KvCountsSnapshot::default(),
        "READ must not issue any KV operation"
    );

    harness.close(handle).await;
}

/// Warm small OPEN (cache already populated) + READ, inside the touch window. One read txn,
/// zero writes, zero READ-time KV.
#[tokio::test]
async fn small_warm_open_inside_window_has_one_read_txn_and_zero_read_txns() {
    let backend = Arc::new(MemoryObjectBackend::new());
    backend.insert(location(SMALL_KEY), b"abc".to_vec());
    let harness = Harness::from_backend(backend, WARM_TOUCH_GRANULARITY).await;

    // Prime the cache with a first OPEN+close.
    let first = harness.open(SMALL_KEY).await;
    harness.close(first).await;

    harness.counts.reset();
    let handle = harness.open(SMALL_KEY).await;
    let after_open = harness.counts.snapshot();
    assert_eq!(after_open.read_txns, 1);
    assert_eq!(after_open.write_txns, 0);
    assert_eq!(after_open.meta_get, 1);
    assert_eq!(after_open.small_get, 1);
    assert_eq!(after_open.meta_put, 0);
    assert_eq!(after_open.lru_put, 0);

    harness.counts.reset();
    harness.read(handle, 0, 3).await;
    let after_read = harness.counts.snapshot();
    assert_eq!(after_read, KvCountsSnapshot::default());

    harness.close(handle).await;
}

/// Warm small OPEN, cross window, fires a touch: one read txn + one write txn. READ after OPEN
/// stays zero KV because the fresh `last_access_ns` we just wrote is inside the next window.
#[tokio::test]
async fn small_warm_open_cross_window_touches_then_read_is_zero_kv() {
    let backend = Arc::new(MemoryObjectBackend::new());
    backend.insert(location(SMALL_KEY), b"abc".to_vec());
    let harness = Harness::from_backend(backend, IMMEDIATE_TOUCH_GRANULARITY).await;

    let first = harness.open(SMALL_KEY).await;
    harness.close(first).await;

    harness.counts.reset();
    let handle = harness.open(SMALL_KEY).await;
    let after_open = harness.counts.snapshot();
    assert_eq!(after_open.read_txns, 1);
    assert_eq!(
        after_open.write_txns, 1,
        "zero-granularity must touch on every OPEN"
    );
    assert_eq!(
        after_open.meta_get, 1,
        "touch must reuse observed meta, not re-read it"
    );
    assert_eq!(after_open.small_get, 1);
    assert_eq!(after_open.meta_put, 1);
    assert_eq!(after_open.lru_remove, 1);
    assert_eq!(after_open.lru_put, 1);

    harness.counts.reset();
    harness.read(handle, 0, 3).await;
    let after_read = harness.counts.snapshot();
    assert_eq!(after_read, KvCountsSnapshot::default());

    harness.close(handle).await;
}

/// Warm CompleteFile OPEN, inside window: one read txn, zero writes, zero READ-time KV.
#[tokio::test]
async fn complete_warm_open_inside_window_is_one_read_txn_and_read_is_zero_kv() {
    let backend = Arc::new(MemoryObjectBackend::new());
    backend.insert(location(LARGE_KEY), b"abcdefghij".to_vec());
    let harness = Harness::from_backend(backend, WARM_TOUCH_GRANULARITY).await;

    // Drive a full large-fill to promote to CompleteFile, then close so the next OPEN is a
    // warm CompleteFile hit.
    let warm_handle = harness.open(LARGE_KEY).await;
    harness.read(warm_handle, 0, 10).await;
    harness.close(warm_handle).await;

    harness.counts.reset();
    let handle = harness.open(LARGE_KEY).await;
    let after_open = harness.counts.snapshot();
    assert_eq!(after_open.read_txns, 1);
    assert_eq!(after_open.write_txns, 0);
    assert_eq!(after_open.meta_get, 1);
    assert_eq!(after_open.small_get, 0);
    assert_eq!(after_open.meta_put, 0);

    harness.counts.reset();
    harness.read(handle, 0, 4).await;
    let after_read = harness.counts.snapshot();
    assert_eq!(after_read, KvCountsSnapshot::default());

    harness.close(handle).await;
}

/// Warm CompleteFile OPEN, cross window: one read + one write txn. Touch reuses observed meta.
#[tokio::test]
async fn complete_warm_open_cross_window_touches_without_second_meta_get() {
    let backend = Arc::new(MemoryObjectBackend::new());
    backend.insert(location(LARGE_KEY), b"abcdefghij".to_vec());
    let harness = Harness::from_backend(backend, IMMEDIATE_TOUCH_GRANULARITY).await;

    let warm_handle = harness.open(LARGE_KEY).await;
    harness.read(warm_handle, 0, 10).await;
    harness.close(warm_handle).await;

    harness.counts.reset();
    let handle = harness.open(LARGE_KEY).await;
    let after_open = harness.counts.snapshot();
    assert_eq!(after_open.read_txns, 1);
    assert_eq!(after_open.write_txns, 1);
    assert_eq!(after_open.meta_get, 1);
    assert_eq!(after_open.meta_put, 1);
    assert_eq!(after_open.lru_remove, 1);
    assert_eq!(after_open.lru_put, 1);

    harness.close(handle).await;
}

/// Cold large OPEN (first OPEN on a never-cached key): lookup is one read txn (miss), admit is
/// one read txn (re-check for concurrent promote). `admit_large` never writes KV — partial fills
/// are memory-only.
#[tokio::test]
async fn large_cold_open_has_exactly_two_read_txns_and_no_writes() {
    let backend = Arc::new(MemoryObjectBackend::new());
    backend.insert(location(LARGE_KEY), b"abcdefghij".to_vec());
    let harness = Harness::from_backend(backend, WARM_TOUCH_GRANULARITY).await;

    harness.counts.reset();
    let handle = harness.open(LARGE_KEY).await;
    let after_open = harness.counts.snapshot();
    assert_eq!(
        after_open.read_txns, 2,
        "lookup miss read + admit_large re-check read"
    );
    assert_eq!(after_open.write_txns, 0);
    assert_eq!(after_open.meta_get, 2);
    assert_eq!(after_open.small_get, 0);
    assert_eq!(after_open.meta_put, 0);
    assert_eq!(after_open.small_put, 0);

    harness.close(handle).await;
}

/// Partial READ on a live large fill never touches KV; only the chunk-write-path backs into
/// `put_new_complete` when the final chunk lands.
#[tokio::test]
async fn large_partial_read_is_zero_kv_and_promote_is_exactly_one_write_txn() {
    let backend = Arc::new(MemoryObjectBackend::new());
    backend.insert(location(LARGE_KEY), b"abcdefghij".to_vec());
    let harness = Harness::from_backend(backend, WARM_TOUCH_GRANULARITY).await;

    let handle = harness.open(LARGE_KEY).await;

    // Read a partial range: 4 bytes at offset 0; this downloads chunk 0 but does not promote
    // (size 10, chunk 4 → chunks {0, 1, 2}).
    harness.counts.reset();
    harness.read(handle, 0, 4).await;
    let partial = harness.counts.snapshot();
    assert_eq!(
        partial,
        KvCountsSnapshot::default(),
        "partial-fill READ must not touch KV"
    );

    // Finish the fill: READ the remaining bytes. Final chunk write triggers promote to
    // CompleteFile, which is exactly one write transaction (put_new_complete): meta_put + lru_put.
    harness.counts.reset();
    harness.read(handle, 4, 6).await;
    let promote = harness.counts.snapshot();
    assert_eq!(promote.read_txns, 0);
    assert_eq!(
        promote.write_txns, 1,
        "promote must be exactly one write transaction"
    );
    assert_eq!(promote.meta_put, 1);
    assert_eq!(promote.lru_put, 1);
    assert_eq!(promote.small_put, 0);

    // A subsequent READ on the same handle continues to serve from the session's complete meta
    // without any KV access.
    harness.counts.reset();
    harness.read(handle, 0, 10).await;
    let post_promote = harness.counts.snapshot();
    assert_eq!(
        post_promote,
        KvCountsSnapshot::default(),
        "READ after session complete is still zero KV on the same handle"
    );

    harness.close(handle).await;
}

// Keep `seed_backend` available for readers — some future contract test may want it.
#[allow(dead_code)]
fn _keep_seed_backend_in_scope(backend: &Arc<MemoryObjectBackend>) {
    seed_backend(backend, SMALL_KEY, b"unused");
}
