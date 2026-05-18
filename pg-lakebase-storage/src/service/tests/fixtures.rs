//! Shared fixtures used by the sibling service-test modules.
//!
//! Conventions:
//! * All items are `pub(crate)` — they are only ever compiled under `#[cfg(test)]`,
//!   and several sibling modules (`small_objects`, `large_objects`, `direct_io`, …)
//!   need to reach them through `super::fixtures::…`.
//! * Test doubles (counting / blocking backends and indexes) live in [`super::test_doubles`]
//!   and are re-exported below so test files do not need to know the sub-module layout.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cache::CachedObjectMeta;
use crate::cache::{CacheIndex, CacheManager, InMemoryCacheIndex};
use crate::config::{StorageRuntime, StorageRuntimeConfig};
use crate::handle::{FileHandle, OpenFlags};
use crate::object::{ObjectInfo, ObjectLocation};
use crate::service::StorageService;
use crate::service::command::{
    CloseCommand, InvalidateObjectCacheCommand, OpenCommand, ReadCommand,
    StorageCommand,
};
use crate::service::reply::CommandOutput;
use crate::session::handle_table::HandleTable;

pub(crate) use super::test_doubles::{
    BlockingHeadBackend, BlockingRangeBackend, CountingCompleteIndex,
};

/// Default store id used by almost every service test.
pub(crate) const DEFAULT_STORE: &str = "default";
/// Default bucket — kept as a constant so tests read as prose rather than string soup.
pub(crate) const BUCKET: &str = "bucket";
/// Canonical key for objects large enough to take the ranged-GET / large-fill path.
pub(crate) const LARGE_KEY: &str = "file";
/// Canonical key for objects small enough to be served from the SmallKV path.
pub(crate) const SMALL_KEY: &str = "tiny";

/// Monotonic suffix used to make [`test_cache_dir`] outputs unique within a process.
static TEST_CACHE_ID: AtomicU64 = AtomicU64::new(0);

/// Returns a fresh per-test temp directory under `/tmp`. Nothing cleans it up; tests
/// rely on the OS temp reaper, which matches the behaviour before the split.
pub(crate) fn test_cache_dir() -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let id = TEST_CACHE_ID.fetch_add(1, Ordering::Relaxed);
    PathBuf::from("/tmp").join(format!(
        "pg-lakebase-storage-service-test-{}-{stamp}-{id}",
        std::process::id()
    ))
}

/// Shorthand for `ObjectLocation::new(DEFAULT_STORE, BUCKET, key)`.
pub(crate) fn default_location(key: &str) -> ObjectLocation {
    ObjectLocation::new(DEFAULT_STORE, BUCKET, key).unwrap()
}

/// Default cache manager used across tests: in-memory index, 4/4 small/large watermarks.
pub(crate) fn memory_cache() -> Arc<CacheManager<InMemoryCacheIndex>> {
    memory_cache_with_limits(4, 4)
}

/// Cache manager with explicit small/large watermarks for tests that exercise thresholds.
pub(crate) fn memory_cache_with_limits(
    small_limit: u64,
    large_limit: u64,
) -> Arc<CacheManager<InMemoryCacheIndex>> {
    let runtime = StorageRuntime::new(StorageRuntimeConfig::default()).unwrap();
    let cache = Arc::new(
        CacheManager::new(test_cache_dir(), InMemoryCacheIndex::new(), runtime)
            .with_limits(small_limit, large_limit),
    );
    cache.spawn_large_fill_reaper();
    cache
}

/// Output of [`open_file`] / [`open_named_file`] — mirrors the wire `OpenResponse`.
pub(crate) struct OpenResult {
    pub(crate) handle: FileHandle,
    pub(crate) direct_io: bool,
}

/// Output of [`read`] — body bytes plus the EOF flag reported by the service.
pub(crate) struct ReadResult {
    pub(crate) data: Vec<u8>,
    pub(crate) eof: bool,
}

/// Opens `(DEFAULT_STORE, BUCKET, key)` in read-only mode and returns its handle.
pub(crate) async fn open_file<I: CacheIndex + 'static>(
    service: &StorageService<I>,
    handles: &HandleTable,
    bucket: &str,
    key: &str,
) -> OpenResult {
    open_named_file(service, handles, DEFAULT_STORE, bucket, key).await
}

/// Same as [`open_file`] but lets the caller pick a non-default store id (registry tests).
pub(crate) async fn open_named_file<I: CacheIndex + 'static>(
    service: &StorageService<I>,
    handles: &HandleTable,
    store_id: &str,
    bucket: &str,
    key: &str,
) -> OpenResult {
    let reply = service
        .execute(
            handles,
            StorageCommand::Open(OpenCommand {
                store_id: store_id.to_string(),
                bucket: bucket.to_string(),
                key: key.to_string(),
                flags: OpenFlags::READ_ONLY,
            }),
        )
        .await
        .unwrap();
    let CommandOutput::Open(output) = reply.output else {
        panic!("unexpected open output");
    };
    OpenResult {
        handle: output.handle,
        direct_io: output.direct_io,
    }
}

/// Issues a `Read` command against the given handle and drains the attachment into bytes.
pub(crate) async fn read<I: CacheIndex + 'static>(
    service: &StorageService<I>,
    handles: &HandleTable,
    handle: FileHandle,
    offset: u64,
    len: u32,
) -> ReadResult {
    let reply = service
        .execute(
            handles,
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
    let (data, eof) = output.into_bytes().await.unwrap();
    ReadResult { data, eof }
}

/// Closes `handle` through the service (releases large-fill leases and cache activity).
pub(crate) async fn close<I: CacheIndex + 'static>(
    service: &StorageService<I>,
    handles: &HandleTable,
    handle: FileHandle,
) {
    service
        .execute(handles, StorageCommand::Close(CloseCommand { handle }))
        .await
        .unwrap();
}

/// Returns the residency variant hint bound to `handle`, or `None` when the handle has no
/// residency (test-only direct-open path).
pub(crate) fn residency_hint(
    handles: &HandleTable,
    handle: FileHandle,
) -> Option<crate::cache::ResidencyStateHint> {
    handles
        .get(handle)
        .unwrap()
        .residency
        .as_ref()
        .map(|residency| residency.state_hint())
}

/// Builds an `InvalidateObjectCache` command for `(DEFAULT_STORE, BUCKET, key)`.
pub(crate) fn invalidate_cmd(key: &str) -> StorageCommand {
    StorageCommand::InvalidateObjectCache(InvalidateObjectCacheCommand {
        store_id: DEFAULT_STORE.to_string(),
        bucket: BUCKET.to_string(),
        key: key.to_string(),
    })
}

/// Seeds `cache` with a `CompleteFile`-shaped row: `meta` defaults to
/// [`CachedObjectMeta::complete`] with the given size.
pub(crate) async fn seed_complete_cache<I: CacheIndex>(
    cache: &CacheManager<I>,
    key: &ObjectLocation,
    data: &[u8],
) {
    let meta = CachedObjectMeta::complete(
        key.clone(),
        ObjectInfo {
            size: data.len() as u64,
            etag: None,
        },
    );
    seed_complete_cache_with_meta(cache, key, data, meta).await;
}

/// Variant of [`seed_complete_cache`] that accepts a pre-built meta (e.g. for `last_access_ns`).
pub(crate) async fn seed_complete_cache_with_meta<I: CacheIndex>(
    cache: &CacheManager<I>,
    key: &ObjectLocation,
    data: &[u8],
    meta: CachedObjectMeta,
) {
    let path = cache.complete_path(key).unwrap();
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.unwrap();
    }
    tokio::fs::write(path, data).await.unwrap();
    cache.index().put_new_complete(meta).await.unwrap();
}

/// Writes arbitrary bytes to a cache-relative path, creating parents as needed.
///
/// Used by large-object tests to plant stale partial payloads before opening the object.
pub(crate) async fn write_cache_file(path: PathBuf, data: &[u8]) {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.unwrap();
    }
    tokio::fs::write(path, data).await.unwrap();
}

/// Waits for `predicate` to return `true`, polling every ~5 ms until the timeout elapses.
///
/// Exists because large-fill partial cleanup now runs asynchronously on the reaper task rather
/// than in the request path — the old "assert immediately after close" pattern no longer holds.
pub(crate) async fn wait_until<F>(label: &str, mut predicate: F)
where
    F: FnMut() -> bool,
{
    let timeout = std::time::Duration::from_secs(2);
    let poll = std::time::Duration::from_millis(5);
    let start = std::time::Instant::now();
    while !predicate() {
        if start.elapsed() > timeout {
            panic!("timed out waiting for: {label}");
        }
        tokio::time::sleep(poll).await;
    }
}

/// Waits for an async `predicate` to return `true`, polling every ~5 ms until timeout.
pub(crate) async fn wait_until_async<F, Fut>(label: &str, mut predicate: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let timeout = std::time::Duration::from_secs(2);
    let poll = std::time::Duration::from_millis(5);
    let start = std::time::Instant::now();
    loop {
        if predicate().await {
            return;
        }
        if start.elapsed() > timeout {
            panic!("timed out waiting for: {label}");
        }
        tokio::time::sleep(poll).await;
    }
}
