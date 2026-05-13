//! Test-only counting wrapper around [`super::client::PersistentCacheIndex`] that exposes
//! per-(table, op) KV counts plus transaction counts.
//!
//! This is the shared instrumentation that the persistent-index unit tests and the service-level
//! contract tests use to assert the exact KV footprint of each OPEN/READ combination. It lives
//! inside the persistent module because it needs access to the private KV boundary
//! ([`super::kv`]) that the rest of the crate is not supposed to reach into.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use super::client::PersistentCacheIndex;
use super::kv::{CacheKv, KvPair, KvReadTxn, KvTable, KvWriteTxn};
use super::redb::RedbKv;
use crate::error::StorageResult;

/// Per-table KV counts plus per-txn counts, collected by [`CountingKv`].
#[derive(Default)]
pub(crate) struct KvCounts {
    pub(crate) read_txns: AtomicUsize,
    pub(crate) write_txns: AtomicUsize,
    pub(crate) meta_get: AtomicUsize,
    pub(crate) small_get: AtomicUsize,
    pub(crate) lru_get: AtomicUsize,
    pub(crate) meta_put: AtomicUsize,
    pub(crate) small_put: AtomicUsize,
    pub(crate) lru_put: AtomicUsize,
    pub(crate) meta_remove: AtomicUsize,
    pub(crate) small_remove: AtomicUsize,
    pub(crate) lru_remove: AtomicUsize,
}

/// Snapshot of [`KvCounts`] with plain `usize` fields for equality assertions.
#[derive(Default, Debug, Eq, PartialEq)]
pub(crate) struct KvCountsSnapshot {
    pub(crate) read_txns: usize,
    pub(crate) write_txns: usize,
    pub(crate) meta_get: usize,
    pub(crate) small_get: usize,
    pub(crate) lru_get: usize,
    pub(crate) meta_put: usize,
    pub(crate) small_put: usize,
    pub(crate) lru_put: usize,
    pub(crate) meta_remove: usize,
    pub(crate) small_remove: usize,
    pub(crate) lru_remove: usize,
}

impl KvCounts {
    pub(crate) fn reset(&self) {
        for field in [
            &self.read_txns,
            &self.write_txns,
            &self.meta_get,
            &self.small_get,
            &self.lru_get,
            &self.meta_put,
            &self.small_put,
            &self.lru_put,
            &self.meta_remove,
            &self.small_remove,
            &self.lru_remove,
        ] {
            field.store(0, Ordering::Relaxed);
        }
    }

    pub(crate) fn snapshot(&self) -> KvCountsSnapshot {
        KvCountsSnapshot {
            read_txns: self.read_txns.load(Ordering::Relaxed),
            write_txns: self.write_txns.load(Ordering::Relaxed),
            meta_get: self.meta_get.load(Ordering::Relaxed),
            small_get: self.small_get.load(Ordering::Relaxed),
            lru_get: self.lru_get.load(Ordering::Relaxed),
            meta_put: self.meta_put.load(Ordering::Relaxed),
            small_put: self.small_put.load(Ordering::Relaxed),
            lru_put: self.lru_put.load(Ordering::Relaxed),
            meta_remove: self.meta_remove.load(Ordering::Relaxed),
            small_remove: self.small_remove.load(Ordering::Relaxed),
            lru_remove: self.lru_remove.load(Ordering::Relaxed),
        }
    }

    fn record_get(&self, table: KvTable) {
        let slot = match table {
            KvTable::Meta => &self.meta_get,
            KvTable::Small => &self.small_get,
            KvTable::Lru => &self.lru_get,
        };
        slot.fetch_add(1, Ordering::Relaxed);
    }

    fn record_put(&self, table: KvTable) {
        let slot = match table {
            KvTable::Meta => &self.meta_put,
            KvTable::Small => &self.small_put,
            KvTable::Lru => &self.lru_put,
        };
        slot.fetch_add(1, Ordering::Relaxed);
    }

    fn record_remove(&self, table: KvTable) {
        let slot = match table {
            KvTable::Meta => &self.meta_remove,
            KvTable::Small => &self.small_remove,
            KvTable::Lru => &self.lru_remove,
        };
        slot.fetch_add(1, Ordering::Relaxed);
    }
}

/// Counting wrapper around any [`CacheKv`] implementation.
///
/// Instrumentation tracks every transaction begin and every `get`, `put`, `remove` routed
/// through the underlying KV. `scan_page` is intentionally not counted: scans have their own
/// (range-scan-level) cost models and do not appear on the OPEN/READ hot paths this harness is
/// designed to measure.
pub(crate) struct CountingKv<K> {
    inner: K,
    counts: Arc<KvCounts>,
}

impl<K> CountingKv<K> {
    pub(crate) fn new(inner: K) -> Self {
        Self {
            inner,
            counts: Arc::new(KvCounts::default()),
        }
    }

    pub(crate) fn counts(&self) -> Arc<KvCounts> {
        self.counts.clone()
    }
}

impl<K: CacheKv> CacheKv for CountingKv<K> {
    type ReadTxn<'a>
        = CountingReadTxn<K::ReadTxn<'a>>
    where
        Self: 'a;
    type WriteTxn<'a>
        = CountingWriteTxn<K::WriteTxn<'a>>
    where
        Self: 'a;

    fn ensure_tables(&self, tables: &[KvTable]) -> StorageResult<()> {
        self.inner.ensure_tables(tables)
    }

    fn begin_read(&self) -> StorageResult<Self::ReadTxn<'_>> {
        self.counts.read_txns.fetch_add(1, Ordering::Relaxed);
        Ok(CountingReadTxn {
            inner: self.inner.begin_read()?,
            counts: self.counts.clone(),
        })
    }

    fn begin_write(&self) -> StorageResult<Self::WriteTxn<'_>> {
        self.counts.write_txns.fetch_add(1, Ordering::Relaxed);
        Ok(CountingWriteTxn {
            inner: self.inner.begin_write()?,
            counts: self.counts.clone(),
        })
    }
}

pub(crate) struct CountingReadTxn<T> {
    inner: T,
    counts: Arc<KvCounts>,
}

impl<T: KvReadTxn> KvReadTxn for CountingReadTxn<T> {
    fn get(&self, table: KvTable, key: &str) -> StorageResult<Option<Vec<u8>>> {
        self.counts.record_get(table);
        self.inner.get(table, key)
    }

    fn get_len(&self, table: KvTable, key: &str) -> StorageResult<Option<u64>> {
        self.counts.record_get(table);
        self.inner.get_len(table, key)
    }

    fn scan_page(&self, table: KvTable, after_exclusive: Option<&str>, limit: usize) -> StorageResult<Vec<KvPair>> {
        self.inner.scan_page(table, after_exclusive, limit)
    }
}

pub(crate) struct CountingWriteTxn<T> {
    inner: T,
    counts: Arc<KvCounts>,
}

impl<T: KvReadTxn> KvReadTxn for CountingWriteTxn<T> {
    fn get(&self, table: KvTable, key: &str) -> StorageResult<Option<Vec<u8>>> {
        self.counts.record_get(table);
        self.inner.get(table, key)
    }

    fn get_len(&self, table: KvTable, key: &str) -> StorageResult<Option<u64>> {
        self.counts.record_get(table);
        self.inner.get_len(table, key)
    }

    fn scan_page(&self, table: KvTable, after_exclusive: Option<&str>, limit: usize) -> StorageResult<Vec<KvPair>> {
        self.inner.scan_page(table, after_exclusive, limit)
    }
}

impl<T: KvWriteTxn> KvWriteTxn for CountingWriteTxn<T> {
    fn put(&mut self, table: KvTable, key: &str, value: &[u8]) -> StorageResult<()> {
        self.counts.record_put(table);
        self.inner.put(table, key, value)
    }

    fn remove(&mut self, table: KvTable, key: &str) -> StorageResult<()> {
        self.counts.record_remove(table);
        self.inner.remove(table, key)
    }

    fn commit(self) -> StorageResult<()> {
        self.inner.commit()
    }
}

/// Opens a redb-backed persistent cache index wrapped in [`CountingKv`]. Returns the instrumented
/// index and the shared [`KvCounts`] handle so tests can `.snapshot()` after each phase.
pub(crate) fn counting_redb_index(path: PathBuf) -> (PersistentCacheIndex<CountingKv<RedbKv>>, Arc<KvCounts>) {
    let kv = CountingKv::new(RedbKv::open(path).unwrap());
    let counts = kv.counts();
    let index = PersistentCacheIndex::from_kv(kv).unwrap();
    (index, counts)
}

/// Returns a unique `/tmp` path for a redb database file. Callers are responsible for the path's
/// lifecycle (the OS temp reaper normally cleans up after the test process).
pub(crate) fn unique_redb_path(tag: &str) -> PathBuf {
    static TEST_DB_ID: AtomicU64 = AtomicU64::new(0);
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let id = TEST_DB_ID.fetch_add(1, Ordering::Relaxed);
    PathBuf::from("/tmp")
        .join(format!("pg-lakebase-storage-kv-contract-{tag}-{}-{stamp}-{id}.redb", std::process::id()))
}
