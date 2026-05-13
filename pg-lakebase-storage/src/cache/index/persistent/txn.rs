//! Low-level metadata writes: keeps `lru_by_access` rows in sync with `object_meta` inside one write txn.

use super::codec::{decode_meta, encode_meta};
use super::keys::lru_key;
use super::kv::{KvTable, KvWriteTxn};
use super::tracking::TrackingDelta;
use crate::cache::meta::CachedObjectMeta;
use crate::error::{StorageError, StorageResult};

/// Metadata view for one KV write transaction: keeps `object_meta`, `lru_by_access`, and resident-byte deltas aligned.
pub(super) struct MetaTxn<'a, T: KvWriteTxn> {
    txn: &'a mut T,
}

impl<'a, T: KvWriteTxn> MetaTxn<'a, T> {
    pub(super) fn new(txn: &'a mut T) -> Self {
        Self { txn }
    }

    pub(super) fn read(&self, key: &str) -> StorageResult<Option<CachedObjectMeta>> {
        self.txn.get(KvTable::Meta, key)?.map(|value| decode_meta(&value)).transpose()
    }

    pub(super) fn insert_new(&mut self, meta: &CachedObjectMeta) -> StorageResult<TrackingDelta> {
        let meta = meta.clone().normalized();
        let db_key = meta.key().to_string();
        self.txn.put(KvTable::Meta, db_key.as_str(), encode_meta(&meta).as_slice())?;
        self.update_tracking(None, Some(&meta))
    }

    pub(super) fn update_existing(
        &mut self,
        old: &CachedObjectMeta,
        meta: &CachedObjectMeta,
    ) -> StorageResult<TrackingDelta> {
        let meta = meta.clone().normalized();
        if old.key() != meta.key() {
            return Err(StorageError::cache(format!(
                "metadata update old key {} does not match new key {}",
                old.key(),
                meta.key()
            )));
        }

        let db_key = meta.key().to_string();
        self.txn.put(KvTable::Meta, db_key.as_str(), encode_meta(&meta).as_slice())?;
        self.update_tracking(Some(old), Some(&meta))
    }

    pub(super) fn touch_observed(
        &mut self,
        observed: &CachedObjectMeta,
        now_ns: u64,
    ) -> StorageResult<(CachedObjectMeta, TrackingDelta)> {
        let mut meta = observed.clone();
        meta.last_access_ns = now_ns;
        let delta = self.update_existing(observed, &meta)?;
        Ok((meta.normalized(), delta))
    }

    pub(super) fn delete(&mut self, key: &str) -> StorageResult<(Option<CachedObjectMeta>, TrackingDelta)> {
        let old = self.read(key)?;
        if old.is_some() {
            self.txn.remove(KvTable::Meta, key)?;
        }
        let delta = self.update_tracking(old.as_ref(), None)?;
        Ok((old, delta))
    }

    fn update_tracking(
        &mut self,
        old: Option<&CachedObjectMeta>,
        new: Option<&CachedObjectMeta>,
    ) -> StorageResult<TrackingDelta> {
        // LRU primary key embeds last_access_ns; touch/delete/update must remove the stale key before inserting the new
        // one.
        if let Some(old) = old {
            self.txn.remove(KvTable::Lru, lru_key(old.last_access_ns, old.key()).as_str())?;
        }
        if let Some(new) = new.filter(|meta| meta.is_cache_resident()) {
            self.txn.put(
                KvTable::Lru,
                lru_key(new.last_access_ns, new.key()).as_str(),
                new.key().to_string().as_bytes(),
            )?;
        }

        Ok(TrackingDelta::from_metas(old, new))
    }
}
