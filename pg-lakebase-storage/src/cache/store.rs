use std::path::PathBuf;

use async_trait::async_trait;

use crate::cache::path::CacheFileKind;
use crate::cache::{CacheIndex, CachePathResolver};
use crate::error::{StorageError, StorageResult};
use crate::object::ObjectLocation;

/// Which on-disk or embedded payload representation holds bytes for a cache entry.
///
/// Complete and partial files share a single directory; the kind tells the orphan/usage logic
/// how to interpret the payload (claimed by metadata vs. transient fill intermediate).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheStoreKind {
    CompleteFile,
    PartialPayload,
    SmallObject,
}

impl From<CacheFileKind> for CacheStoreKind {
    fn from(kind: CacheFileKind) -> Self {
        match kind {
            CacheFileKind::Complete => Self::CompleteFile,
            CacheFileKind::Partial => Self::PartialPayload,
        }
    }
}

/// Stable identifier for one physical payload during directory scans and deletes.
///
/// File-backed caches use the concrete path; small objects use the [`crate::object::ObjectLocation`] because payloads
/// live in the index store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalCacheId {
    Path(PathBuf),
    SmallObject(ObjectLocation),
}

/// One row produced while walking cache storage (directory traversal or paged small-object scan).
///
/// `logical_bytes` may diverge from `physical_bytes` for diagnostics (for example small rows carry both as the same
/// value today). Startup recovery and orphan cleanup use `store_kind`, `id`, and optional `object_key` to decide
/// whether a payload is claimed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalCacheEntry {
    pub store_kind: CacheStoreKind,
    pub id: PhysicalCacheId,
    pub object_key: Option<ObjectLocation>,
    pub bytes: u64,
    pub logical_bytes: Option<u64>,
    pub physical_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PhysicalCacheStat {
    pub bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DeleteReport {
    pub bytes_deleted: u64,
}

/// Lets visitors stop expensive traversals early without treating early exit as an error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanControl {
    Continue,
    Stop,
}

/// Visitor invoked for each physical cache entry during scans.
#[async_trait]
pub trait PhysicalCacheEntryVisitor: Send {
    async fn visit(&mut self, entry: PhysicalCacheEntry) -> StorageResult<ScanControl>;
}

/// Abstract access to one kind of payload storage (file-backed cache directory or small-object rows).
///
/// [`Self::delete_entry`] semantics depend on the implementation: the internal small-object store adapter routes
/// unclaimed small deletes through [`crate::cache::index::CacheIndex::remove_unclaimed_small_payload`],
/// which must not run unless metadata proves the key is not [`crate::cache::CacheState::SmallKv`].
#[async_trait]
pub trait CacheStore {
    async fn visit_entries(&self, visitor: &mut dyn PhysicalCacheEntryVisitor) -> StorageResult<()>;

    async fn stat_entry(&self, id: &PhysicalCacheId) -> StorageResult<PhysicalCacheStat>;
    async fn delete_entry(&self, id: &PhysicalCacheId) -> StorageResult<DeleteReport>;
}

/// Walks the unified `<root>/objects/` tree and classifies each cache file via
/// [`CachePathResolver::parse_cache_path`]. Complete and partial files are emitted with distinct
/// [`CacheStoreKind`]s so downstream consumers (startup recovery, usage accounting) can keep
/// their per-kind branching without the store itself having to pick a kind up front.
#[derive(Clone, Debug)]
pub(crate) struct FileCacheStore {
    pub(crate) paths: CachePathResolver,
}

impl FileCacheStore {
    pub(crate) fn new(paths: CachePathResolver) -> Self {
        Self { paths }
    }
}

/// [`CacheStore`] adapter that pages [`crate::cache::index::CacheIndex::scan_small_entries_page`] instead of reading
/// the filesystem.
pub(crate) struct SmallObjectStore<'a, I: CacheIndex> {
    pub(crate) index: &'a I,
}

#[async_trait]
impl CacheStore for FileCacheStore {
    async fn visit_entries(&self, visitor: &mut dyn PhysicalCacheEntryVisitor) -> StorageResult<()> {
        let mut dirs = vec![self.paths.objects_dir()];
        while let Some(next_dir) = dirs.pop() {
            let mut dir = match tokio::fs::read_dir(&next_dir).await {
                Ok(dir) => dir,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            while let Some(entry) = dir.next_entry().await? {
                let file_type = entry.file_type().await?;
                let path = entry.path();
                if file_type.is_dir() {
                    dirs.push(path);
                    continue;
                }
                if !file_type.is_file() {
                    continue;
                }
                // Unknown filenames (wrong prefix / wrong suffix) are quietly ignored: the cache
                // directory may pick up temp files from partially-completed filesystem operations
                // and startup recovery should not choke on them.
                let Some((object_key, kind)) = self.paths.parse_cache_path(&path) else {
                    continue;
                };
                let stat = self.stat_entry(&PhysicalCacheId::Path(path.clone())).await?;
                let entry = PhysicalCacheEntry {
                    store_kind: kind.into(),
                    id: PhysicalCacheId::Path(path),
                    object_key: Some(object_key),
                    bytes: stat.bytes,
                    logical_bytes: None,
                    physical_bytes: stat.bytes,
                };
                if visitor.visit(entry).await? == ScanControl::Stop {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    async fn stat_entry(&self, id: &PhysicalCacheId) -> StorageResult<PhysicalCacheStat> {
        let PhysicalCacheId::Path(path) = id else {
            return Err(StorageError::cache("file cache store received non-file id"));
        };
        Ok(PhysicalCacheStat {
            bytes: tokio::fs::metadata(path).await?.len(),
        })
    }

    async fn delete_entry(&self, id: &PhysicalCacheId) -> StorageResult<DeleteReport> {
        let PhysicalCacheId::Path(path) = id else {
            return Err(StorageError::cache("file cache store received non-file id"));
        };
        let bytes = match tokio::fs::metadata(path).await {
            Ok(meta) => meta.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(DeleteReport { bytes_deleted: bytes }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(DeleteReport { bytes_deleted: 0 }),
            Err(error) => Err(error.into()),
        }
    }
}

#[async_trait]
impl<I: CacheIndex> CacheStore for SmallObjectStore<'_, I> {
    async fn visit_entries(&self, visitor: &mut dyn PhysicalCacheEntryVisitor) -> StorageResult<()> {
        const PAGE_SIZE: usize = 1024;

        let mut cursor = None;
        loop {
            let page = self.index.scan_small_entries_page(cursor, PAGE_SIZE).await?;
            cursor = page.next_cursor;
            for entry in page.entries {
                let physical = PhysicalCacheEntry {
                    store_kind: CacheStoreKind::SmallObject,
                    id: PhysicalCacheId::SmallObject(entry.key.clone()),
                    object_key: Some(entry.key),
                    bytes: entry.bytes,
                    logical_bytes: Some(entry.bytes),
                    physical_bytes: entry.bytes,
                };
                if visitor.visit(physical).await? == ScanControl::Stop {
                    return Ok(());
                }
            }
            if cursor.is_none() {
                break;
            }
        }
        Ok(())
    }

    async fn stat_entry(&self, id: &PhysicalCacheId) -> StorageResult<PhysicalCacheStat> {
        let PhysicalCacheId::SmallObject(key) = id else {
            return Err(StorageError::cache("small object store received non-small id"));
        };
        let bytes = self.index.get_small(key).await?.map(|data| data.len() as u64).unwrap_or(0);
        Ok(PhysicalCacheStat { bytes })
    }

    async fn delete_entry(&self, id: &PhysicalCacheId) -> StorageResult<DeleteReport> {
        let PhysicalCacheId::SmallObject(key) = id else {
            return Err(StorageError::cache("small object store received non-small id"));
        };
        let bytes = self.stat_entry(id).await?.bytes;
        self.index.remove_unclaimed_small_payload(key).await?;
        Ok(DeleteReport { bytes_deleted: bytes })
    }
}
