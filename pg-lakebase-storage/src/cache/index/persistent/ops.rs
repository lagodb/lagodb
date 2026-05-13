//! Blocking KV operations that back the [`CacheIndex`](crate::cache::index::CacheIndex) trait
//! implementation for [`PersistentCacheIndex`](super::client::PersistentCacheIndex).
//!
//! The async trait methods that dispatch here live in `super::api`. Those methods are thin
//! wrappers that clone the cache key onto the blocking pool and call into the sync helpers
//! grouped below. Keeping the real work here means the `impl CacheIndex` block reads as a
//! dispatch table, while each operation's transaction logic stays close to the other operations
//! that touch the same KV tables.
//!
//! Each submodule groups the operations that touch the same table family:
//!
//! * [`meta`]  — `object_meta` reads, scans, publishes, deletes.
//! * [`small`] — `small_object` reads/scans, the atomic insert-if-absent admit path, and small-KV deletes.
//! * [`open`]  — the single-transaction `OPEN` hit path (optional touch + small payload read).
//! * [`usage`] — oldest-access-first LRU scans for capacity eviction.
//!
//! All operations run on the blocking pool via [`super::client::PersistentCacheIndex::run_kv`] or
//! [`super::client::PersistentCacheIndex::run_tracked`]; they do not themselves spawn blocking
//! work or talk to [`tokio`].

pub(super) mod open {
    use std::sync::Arc;

    use super::super::kv::{CacheKv, KvReadTxn, KvTable, KvWriteTxn};
    use super::super::tracking::RuntimeCacheTracking;
    use super::super::txn::MetaTxn;
    use crate::cache::index::OpenHit;
    use crate::cache::meta::{CacheState, CachedObjectMeta};
    use crate::cache::should_touch;
    use crate::error::{StorageError, StorageResult};

    /// Reads metadata (and the small-KV payload when relevant) and optionally touches
    /// `last_access_ns`, all in one transaction.
    ///
    /// The read transaction is upgraded to a write transaction only when the touch policy fires,
    /// and even then the pre-touch meta observed at the start of the call is reused — no second
    /// `get(Meta)` is issued.
    pub(in crate::cache::index::persistent) fn open_hit(
        kv: &impl CacheKv,
        tracking: &RuntimeCacheTracking,
        db_key: &str,
        now_ns: u64,
        touch_granularity_ns: u64,
    ) -> StorageResult<Option<OpenHit>> {
        let txn = kv.begin_read()?;
        let Some(meta) = super::meta::read_meta(&txn, db_key)? else {
            return Ok(None);
        };
        let payload = match meta.cache_state() {
            CacheState::SmallKv => {
                let bytes = txn
                    .get(KvTable::Small, db_key)?
                    .ok_or_else(|| StorageError::cache(format!("small object missing from cache: {db_key}")))?;
                Some(Arc::<[u8]>::from(bytes))
            },
            CacheState::CompleteFile => None,
        };
        if !should_touch(meta.last_access_ns, now_ns, touch_granularity_ns) {
            return Ok(Some(OpenHit { meta, payload }));
        }
        drop(txn);

        let touched = touch_observed_meta(kv, tracking, &meta, now_ns)?;
        Ok(Some(OpenHit { meta: touched, payload }))
    }

    fn touch_observed_meta(
        kv: &impl CacheKv,
        tracking: &RuntimeCacheTracking,
        observed: &CachedObjectMeta,
        now_ns: u64,
    ) -> StorageResult<CachedObjectMeta> {
        let mut txn = kv.begin_write()?;
        let (meta, delta) = {
            let mut meta_txn = MetaTxn::new(&mut txn);
            meta_txn.touch_observed(observed, now_ns)?
        };
        txn.commit()?;
        tracking.apply_delta(delta);
        Ok(meta)
    }
}

pub(super) mod meta {
    use super::super::codec::decode_meta;
    use super::super::kv::{CacheKv, KvReadTxn, KvTable, KvWriteTxn};
    use super::super::tracking::RuntimeCacheTracking;
    use super::super::txn::MetaTxn;
    use crate::cache::index::{MetaScanCursor, MetaScanPage};
    use crate::cache::meta::{CacheState, CachedObjectMeta};
    use crate::error::{StorageError, StorageResult};

    pub(in crate::cache::index::persistent) fn get_meta<K: CacheKv>(
        kv: &K,
        db_key: &str,
    ) -> StorageResult<Option<CachedObjectMeta>> {
        let txn = kv.begin_read()?;
        read_meta(&txn, db_key)
    }

    pub(in crate::cache::index::persistent) fn scan_meta_page(
        kv: &impl CacheKv,
        cursor: Option<MetaScanCursor>,
        limit: usize,
    ) -> StorageResult<MetaScanPage> {
        let txn = kv.begin_read()?;
        let mut metas = Vec::new();
        let mut next_cursor = None;
        let limit = limit.max(1);
        let after_key = cursor.map(|cursor| cursor.key.to_string());
        let rows = txn.scan_page(KvTable::Meta, after_key.as_deref(), limit)?;
        for row in rows {
            let meta = decode_meta(&row.value)?;
            next_cursor = Some(MetaScanCursor {
                key: meta.key().clone(),
            });
            metas.push(meta);
        }
        if metas.len() < limit {
            next_cursor = None;
        }
        Ok(MetaScanPage { metas, next_cursor })
    }

    pub(in crate::cache::index::persistent) fn put_new_complete(
        kv: &impl CacheKv,
        tracking: &RuntimeCacheTracking,
        meta: CachedObjectMeta,
    ) -> StorageResult<CachedObjectMeta> {
        let meta = meta.normalized();
        if meta.cache_state() != CacheState::CompleteFile {
            return Err(StorageError::cache(format!("metadata for {} is not complete-file residency", meta.key())));
        }
        let mut txn = kv.begin_write()?;
        let delta = {
            let mut meta_txn = MetaTxn::new(&mut txn);
            meta_txn.insert_new(&meta)?
        };
        txn.commit()?;
        tracking.apply_delta(delta);
        Ok(meta)
    }

    pub(in crate::cache::index::persistent) fn delete_meta(
        kv: &impl CacheKv,
        tracking: &RuntimeCacheTracking,
        db_key: &str,
    ) -> StorageResult<Option<CachedObjectMeta>> {
        let mut txn = kv.begin_write()?;
        let (old, delta) = {
            let mut meta_txn = MetaTxn::new(&mut txn);
            meta_txn.delete(db_key)?
        };
        txn.commit()?;
        tracking.apply_delta(delta);
        Ok(old)
    }

    pub(in crate::cache::index::persistent) fn read_meta(
        txn: &impl KvReadTxn,
        db_key: &str,
    ) -> StorageResult<Option<CachedObjectMeta>> {
        txn.get(KvTable::Meta, db_key)?.map(|value| decode_meta(&value)).transpose()
    }
}

pub(super) mod small {
    use std::sync::Arc;

    use super::super::keys::parse_db_key;
    use super::super::kv::{CacheKv, KvReadTxn, KvTable, KvWriteTxn};
    use super::super::tracking::RuntimeCacheTracking;
    use super::super::txn::MetaTxn;
    use crate::cache::index::{AdmitSmallOutcome, SmallCacheEntry, SmallScanCursor, SmallScanPage};
    use crate::cache::meta::CachedObjectMeta;
    use crate::error::{StorageError, StorageResult};

    pub(in crate::cache::index::persistent) fn get_small(
        kv: &impl CacheKv,
        db_key: &str,
    ) -> StorageResult<Option<Vec<u8>>> {
        let txn = kv.begin_read()?;
        txn.get(KvTable::Small, db_key)
    }

    pub(in crate::cache::index::persistent) fn stat_small(
        kv: &impl CacheKv,
        db_key: &str,
    ) -> StorageResult<Option<u64>> {
        let txn = kv.begin_read()?;
        txn.get_len(KvTable::Small, db_key)
    }

    /// Insert-if-absent small admission, running entirely inside one write transaction.
    ///
    /// The race window between concurrent OPENs that both miss is resolved here: the winner
    /// observes no meta row inside the write txn and writes `(small, meta, lru)`; the loser
    /// observes the winner's meta row and its payload, and returns
    /// [`AdmitSmallOutcome::AlreadyPresent`] without issuing a second transaction.
    pub(in crate::cache::index::persistent) fn admit_small_if_absent(
        kv: &impl CacheKv,
        tracking: &RuntimeCacheTracking,
        mut meta: CachedObjectMeta,
        payload: Vec<u8>,
        now_ns: u64,
    ) -> StorageResult<AdmitSmallOutcome> {
        let mut txn = kv.begin_write()?;
        let db_key = meta.key().to_string();

        let existing = {
            let meta_txn = MetaTxn::new(&mut txn);
            meta_txn.read(db_key.as_str())?
        };
        if let Some(existing_meta) = existing {
            let existing_payload = txn.get(KvTable::Small, db_key.as_str())?.ok_or_else(|| {
                StorageError::cache(format!("small object missing from cache: {}", existing_meta.key()))
            })?;
            // Write txn aborts on drop; no commit needed on the race-loser path.
            return Ok(AdmitSmallOutcome::AlreadyPresent {
                meta: existing_meta,
                payload: Arc::<[u8]>::from(existing_payload),
            });
        }

        meta.set_small(payload.len() as u64);
        meta.last_access_ns = now_ns;
        meta = meta.normalized();
        txn.put(KvTable::Small, db_key.as_str(), payload.as_slice())?;
        let delta = {
            let mut meta_txn = MetaTxn::new(&mut txn);
            meta_txn.insert_new(&meta)?
        };
        txn.commit()?;
        tracking.apply_delta(delta);
        Ok(AdmitSmallOutcome::Admitted {
            meta,
            payload: Arc::<[u8]>::from(payload),
        })
    }

    pub(in crate::cache::index::persistent) fn scan_small_entries_page(
        kv: &impl CacheKv,
        cursor: Option<SmallScanCursor>,
        limit: usize,
    ) -> StorageResult<SmallScanPage> {
        let txn = kv.begin_read()?;
        let mut entries = Vec::new();
        let mut next_cursor = None;
        let limit = limit.max(1);
        let after_key = cursor.map(|cursor| cursor.key.to_string());
        let rows = txn.scan_page(KvTable::Small, after_key.as_deref(), limit)?;
        for row in rows {
            let object_key = parse_db_key(&row.key)?;
            next_cursor = Some(SmallScanCursor {
                key: object_key.clone(),
            });
            entries.push(SmallCacheEntry {
                key: object_key,
                bytes: row.value.len() as u64,
            });
        }
        if entries.len() < limit {
            next_cursor = None;
        }
        Ok(SmallScanPage { entries, next_cursor })
    }

    pub(in crate::cache::index::persistent) fn remove_unclaimed_small_payload(
        kv: &impl CacheKv,
        db_key: &str,
    ) -> StorageResult<()> {
        let mut txn = kv.begin_write()?;
        txn.remove(KvTable::Small, db_key)?;
        txn.commit()
    }

    pub(in crate::cache::index::persistent) fn delete_meta_and_small(
        kv: &impl CacheKv,
        tracking: &RuntimeCacheTracking,
        db_key: &str,
    ) -> StorageResult<Option<CachedObjectMeta>> {
        let mut txn = kv.begin_write()?;
        let (old, delta) = {
            let mut meta_txn = MetaTxn::new(&mut txn);
            meta_txn.delete(db_key)?
        };
        txn.remove(KvTable::Small, db_key)?;
        txn.commit()?;
        tracking.apply_delta(delta);
        Ok(old)
    }
}

pub(super) mod usage {
    use super::super::codec::decode_meta;
    use super::super::keys::{lru_access_ns, lru_key, parse_db_key};
    use super::super::kv::{CacheKv, KvReadTxn, KvTable};
    use crate::cache::index::{LruScanCursor, LruScanPage};
    use crate::error::{StorageError, StorageResult};

    pub(in crate::cache::index::persistent) fn oldest_cached_metas_page(
        kv: &impl CacheKv,
        cursor: Option<LruScanCursor>,
        limit: usize,
    ) -> StorageResult<LruScanPage> {
        let txn = kv.begin_read()?;
        let mut metas = Vec::new();
        let limit = limit.max(1);
        let mut after_lru_key = cursor.map(|cursor| lru_key(cursor.last_access_ns, &cursor.key));

        loop {
            let rows = txn.scan_page(KvTable::Lru, after_lru_key.as_deref(), limit)?;
            if rows.is_empty() {
                return Ok(LruScanPage {
                    metas,
                    next_cursor: None,
                });
            }
            let rows_len = rows.len();

            for row in rows {
                after_lru_key = Some(row.key.clone());
                let db_key = String::from_utf8(row.value)
                    .map_err(|error| StorageError::cache_source("invalid lru metadata key", error))?;
                let object_key = parse_db_key(&db_key)?;
                let row_cursor = Some(LruScanCursor {
                    last_access_ns: lru_access_ns(&row.key)?,
                    key: object_key.clone(),
                });
                let Some(meta) = txn.get(KvTable::Meta, &db_key)? else {
                    continue;
                };
                metas.push(decode_meta(&meta)?);
                if metas.len() >= limit {
                    return Ok(LruScanPage {
                        metas,
                        next_cursor: row_cursor,
                    });
                }
            }

            if rows_len < limit {
                return Ok(LruScanPage {
                    metas,
                    next_cursor: None,
                });
            }
        }
    }
}
