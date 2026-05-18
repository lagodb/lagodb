//! [`CacheManager`] methods that drive large fills and the reaper.
//!
//! These live here rather than in `cache/mod.rs` because they are the sole consumers of the
//! chunks submodules and touching them always means touching session / registry / reaper state
//! in lock-step.

use std::sync::{Arc, Weak};

use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tracing::{debug, warn};

use super::flight::ChunkFillLeader;
use super::reaper::{ReapRequest, run_reaper};
use super::session::LargeFillSession;
use crate::cache::{
    CacheIndex, CacheManager, CacheState, CachedObjectMeta, create_parent_dir,
};
use crate::error::{StorageError, StorageResult};
use crate::object::{ObjectLocation, chunk_count, chunk_index, chunk_range};

impl<I: CacheIndex> CacheManager<I> {
    pub fn chunk_range_for(&self, size: u64, chunk: u64) -> std::ops::Range<u64> {
        chunk_range(size, self.chunk_size, chunk)
    }

    pub fn chunks_for_read(&self, offset: u64, len: u32, size: u64) -> Vec<u64> {
        if len == 0 || offset >= size {
            return Vec::new();
        }
        let end = std::cmp::min(offset.saturating_add(len as u64), size);
        let first = chunk_index(offset, self.chunk_size);
        let last = chunk_index(end.saturating_sub(1), self.chunk_size);
        (first..=last).collect()
    }

    pub(crate) async fn store_large_chunk_for_session(
        &self,
        session: Arc<LargeFillSession>,
        chunk: u64,
        data: &[u8],
        leader: ChunkFillLeader,
    ) -> StorageResult<()> {
        let committed = {
            let _object = session.state().lock().await;

            if let Err(error) =
                self.write_large_chunk_unlocked(&session, chunk, data).await
            {
                self.abort_large_fill(&session).await?;
                return Err(error);
            }

            if session.record_chunk_complete(chunk, leader).await? {
                let _promotion_guard = self.promotion_guard(session.key());
                self.commit_large_fill_unlocked(session).await?;
                true
            } else {
                false
            }
        };

        if committed {
            self.nudge_cleanup_after_write();
        }
        Ok(())
    }

    pub(crate) async fn open_large_range_for_session(
        &self,
        session: &LargeFillSession,
        offset: u64,
        len: u32,
    ) -> StorageResult<(tokio::fs::File, u64, u32, bool)> {
        let _object_guard = session.state().lock().await;
        if let Some(meta) = session.complete_meta().await? {
            return self
                .open_file_range_for_meta(
                    &meta,
                    CacheState::CompleteFile,
                    offset,
                    len,
                )
                .await;
        }

        let info = session.info();
        let start = std::cmp::min(offset, info.size);
        let end = std::cmp::min(start.saturating_add(len as u64), info.size);
        let path = session.partial_path();
        let file = tokio::fs::File::open(path).await?;
        Ok((file, start, (end - start) as u32, end == info.size))
    }

    pub(crate) async fn open_file_range_for_meta(
        &self,
        meta: &CachedObjectMeta,
        state: CacheState,
        offset: u64,
        len: u32,
    ) -> StorageResult<(tokio::fs::File, u64, u32, bool)> {
        if state != CacheState::CompleteFile {
            return Err(StorageError::cache(format!(
                "cache state {state:?} is not complete-file for {}",
                meta.key()
            )));
        }
        let start = std::cmp::min(offset, meta.size());
        let end = std::cmp::min(start.saturating_add(len as u64), meta.size());
        let file = tokio::fs::File::open(self.complete_path(meta.key())?).await?;
        Ok((file, start, (end - start) as u32, end == meta.size()))
    }

    pub(crate) async fn open_complete_file(
        &self,
        key: &ObjectLocation,
    ) -> StorageResult<std::fs::File> {
        let path = self.complete_path(key)?;
        tokio::task::spawn_blocking(move || std::fs::File::open(path))
            .await
            .map_err(|error| {
                StorageError::io(
                    "open complete cache file task failed",
                    std::io::Error::other(error),
                )
            })?
            .map_err(Into::into)
    }

    async fn write_large_chunk_unlocked(
        &self,
        session: &LargeFillSession,
        chunk: u64,
        data: &[u8],
    ) -> StorageResult<()> {
        if chunk >= chunk_count(session.info.size, self.chunk_size) {
            return Err(StorageError::cache(format!(
                "chunk {chunk} out of bounds for {}",
                session.key()
            )));
        }
        session.ensure_filling().await?;
        self.prepare_dirs().await?;
        let path = session.partial_path();
        create_parent_dir(path).await?;
        let range = self.chunk_range_for(session.info.size, chunk);
        let expected = (range.end - range.start) as usize;
        if data.len() != expected {
            return Err(StorageError::cache(format!(
                "chunk {chunk} for {} has {} bytes, expected {expected}",
                session.key(),
                data.len()
            )));
        }

        let fresh = session.claim_partial_bootstrap();
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(fresh)
            .open(path)
            .await?;
        file.seek(std::io::SeekFrom::Start(range.start)).await?;
        file.write_all(data).await?;
        file.flush().await?;
        self.orphan_candidates
            .record_file_candidate(path.to_path_buf());
        Ok(())
    }

    async fn commit_large_fill_unlocked(
        &self,
        session: Arc<LargeFillSession>,
    ) -> StorageResult<CachedObjectMeta> {
        if let Some(meta) = session.complete_meta().await? {
            return Ok(meta);
        }

        self.prepare_dirs().await?;
        let partial = session.partial_path().to_path_buf();
        let complete = self.complete_path(session.key())?;
        create_parent_dir(&complete).await?;

        // Empty object_meta slot proof for this publish:
        // - Large partial fills never write object_meta rows.
        // - LargeFillSession creation happens after admit_large holds the object lock and observes
        //   no meta (via `open_hit`).
        // - The per-object fill slot stores at most one live session per key; this cache does not support multiple
        //   generations.
        // - The final-chunk promotion runs while store_large_chunk_for_session holds the object lock.
        // - Normal small/complete metadata insertions go through CacheManager's object-locked state machine.
        // - Invalidation and eviction also take the object lock and honor activity guards, so an active large fill
        //   cannot be invalidated into a new cache lifecycle.
        // Therefore put_new_complete relies on the CacheManager state proof rather than re-reading meta here.

        // Metadata is the authoritative cache ownership record. While holding the object lock,
        // an existing complete payload with no metadata row is an unclaimed orphan, so promotion
        // may delete it before installing the newly completed fill.
        if tokio::fs::try_exists(&complete).await? {
            self.delete_file_payload(complete.clone()).await?;
        }

        if let Err(error) = tokio::fs::rename(&partial, &complete).await {
            self.abort_large_fill(&session).await?;
            return Err(StorageError::io(
                format!("promote partial cache file for {}", session.key()),
                error,
            ));
        }

        self.orphan_candidates
            .record_promotion(&partial, complete.clone());
        let mut meta =
            CachedObjectMeta::complete(session.key().clone(), session.info.clone());
        meta.last_access_ns = crate::cache::now_ns();
        let meta = match self.index.put_new_complete(meta).await {
            Ok(meta) => meta,
            Err(error) => {
                session.abort().await;
                self.orphan_candidates.record_file_candidate(complete);
                return Err(error);
            }
        };

        session.mark_complete(meta.clone()).await?;
        // Proactively vacate the fill slot while we still hold the object lock. After this
        // point a subsequent OPEN for `key` sees metadata instead of a live fill session, and the
        // reaper will (when this session's last Arc drops) observe `completed == true` and skip
        // the disk cleanup path entirely.
        session.state().clear_fill_slot_if_matches(session.nonce());
        self.orphan_candidates.clear_file_candidate(&complete);
        self.orphan_candidates.clear_file_candidate(&partial);
        Ok(meta)
    }

    pub(crate) async fn abort_large_fill(
        &self,
        session: &Arc<LargeFillSession>,
    ) -> StorageResult<()> {
        session.abort().await;
        self.orphan_candidates
            .record_file_candidate(session.partial_path().to_path_buf());
        Ok(())
    }

    /// Executes one [`ReapRequest`] under the per-object lock carried on the request.
    ///
    /// Ordering vs concurrent OPEN / writes:
    /// * the lock serializes against `store_large_chunk_for_session`, `invalidate_object_cache`, and any other
    ///   per-object critical section — so we never delete a partial while a chunk is being written
    /// * the nonce check refuses to touch anything if the fill slot already points at a newer session for this key (a
    ///   new OPEN raced in after this session's last Arc dropped) or has been cleared by an explicit invalidate
    ///
    /// The `Arc<PerObjectState>` carried on the request is the same state instance the session was attached to, so the
    /// nonce check is observing the exact slot the session registered into — not a freshly recycled state.
    ///
    /// Errors are logged rather than propagated: the reap path runs in a detached task and has
    /// no upstream caller to surface `Result` to. A failed unlink leaves disk ahead of
    /// bookkeeping until the next orphan pass; [`CacheManager::delete_file_payload`] re-registers
    /// the path as an orphan candidate when unlink returns an error.
    pub(crate) async fn reap_large_fill(&self, request: ReapRequest) {
        let ReapRequest {
            state,
            partial_path,
            nonce,
        } = request;
        let _object_guard = state.lock().await;
        if !state.fill_slot_nonce_matches(nonce) {
            // A newer session has taken over this key (or the slot was already cleared by an
            // explicit invalidate). Either way the partial on disk — if any — is no longer ours
            // to delete.
            return;
        }

        if let Err(error) = self.delete_file_payload(partial_path.clone()).await {
            warn!(
                target: "pg_lakebase_storage::cache",
                key = %state.key(),
                partial = %partial_path.display(),
                error = %error,
                "large-fill reaper failed to unlink partial; leaving it as an orphan candidate",
            );
        } else {
            debug!(
                target: "pg_lakebase_storage::cache",
                key = %state.key(),
                partial = %partial_path.display(),
                "large-fill reaper cleaned up partial file",
            );
        }
        state.clear_fill_slot_if_matches(nonce);
    }
}

impl<I: CacheIndex + 'static> CacheManager<I> {
    /// Spawns the large-fill reaper onto the current tokio runtime.
    ///
    /// Must be called exactly once per manager before any OPEN / fill work runs, otherwise
    /// incomplete fills will leak their partials until the next startup orphan scan. The
    /// corresponding inbox is consumed here; calling twice is a programmer error and panics.
    ///
    /// The task holds a [`Weak`] reference to `self` so it does not keep the manager alive. When
    /// the last strong reference to the manager is dropped, the reaper's next `upgrade()` fails
    /// and the task exits cleanly.
    pub(crate) fn spawn_large_fill_reaper(
        self: &Arc<Self>,
    ) -> tokio::task::JoinHandle<()> {
        let inbox = self
            .reaper_inbox
            .lock()
            .expect("reaper inbox mutex poisoned")
            .take()
            .expect("spawn_large_fill_reaper called more than once per CacheManager");
        let weak: Weak<Self> = Arc::downgrade(self);
        tokio::spawn(run_reaper(inbox, weak))
    }
}
