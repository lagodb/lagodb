//! [`LargeFillSession`] and its state machine.
//!
//! A session owns the bookkeeping for one in-progress large-object fill keyed by
//! [`ObjectLocation`]. All long-lived state lives behind a single [`tokio::sync::Mutex`]; the only
//! synchronously-accessible bits are the completion / bootstrap atomics consulted from [`Drop`]
//! and the first-chunk write path.
//!
//! # Session ↔ PerObjectState lifetime
//!
//! Every session holds an `Arc<PerObjectState>`. This is the load-bearing reason the reaper's
//! nonce-based identity check remains sound after the per-object state was unified: a session's
//! `Drop` sends its `Arc<PerObjectState>` along in the [`ReapRequest`], so the state instance the
//! reaper observes is the *same* instance the session was attached to — never a freshly recycled
//! one.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Mutex as AsyncMutex;

use super::flight::{
    ChunkFillClaim, ChunkFillLeader, ChunkFillWaiter, ChunkFlight, ChunkFlightOutcome,
};
use super::reaper::{ReapRequest, ReaperHandle};
use crate::cache::meta::CachedObjectMeta;
use crate::cache::object_state::PerObjectState;
use crate::error::{StorageError, StorageResult};
use crate::object::{ObjectInfo, ObjectLocation};

pub(crate) struct LargeFillSession {
    /// Pins the [`PerObjectState`] this session is attached to for the whole session lifetime
    /// (including the reap window; see module docs).
    state: Arc<PerObjectState>,
    pub(super) info: ObjectInfo,
    partial_path: PathBuf,
    nonce: u64,
    fill_state: AsyncMutex<LargeFillState>,
    /// Synchronously readable completion marker for [`Drop`] — see [`Self::drop`].
    ///
    /// Flipped `false → true` under the per-object lock in [`Self::mark_complete`] after
    /// promotion succeeded. Drop reads it with `Acquire`, so it observes every prior write that
    /// happened under the object lock.
    completed: AtomicBool,
    /// Flipped `false → true` by the first chunk writer under this session's object lock; tells
    /// [`crate::cache::CacheManager::write_large_chunk_unlocked`] to open the partial with
    /// `O_TRUNC`, so stale bytes left by a prior session whose reap could not unlink the partial
    /// are erased before any write lands. Once true, subsequent writes must preserve earlier
    /// chunks.
    partial_bootstrapped: AtomicBool,
    reaper: ReaperHandle,
}

enum LargeFillState {
    Filling { chunks: ChunkProgress },
    Complete { meta: CachedObjectMeta },
    Aborted,
}

struct ChunkProgress {
    slots: Vec<ChunkSlot>,
    completed_count: u64,
}

enum ChunkSlot {
    Missing,
    InFlight(Arc<ChunkFlight>),
    Complete,
}

impl LargeFillSession {
    pub(crate) fn new(
        state: Arc<PerObjectState>,
        info: ObjectInfo,
        chunks: usize,
        partial_path: PathBuf,
        nonce: u64,
        reaper: ReaperHandle,
    ) -> Self {
        Self {
            state,
            info,
            partial_path,
            nonce,
            fill_state: AsyncMutex::new(LargeFillState::Filling {
                chunks: ChunkProgress::new(chunks),
            }),
            completed: AtomicBool::new(false),
            partial_bootstrapped: AtomicBool::new(false),
            reaper,
        }
    }

    pub(crate) fn key(&self) -> &ObjectLocation {
        self.state.key()
    }

    pub(crate) fn state(&self) -> &Arc<PerObjectState> {
        &self.state
    }

    pub(crate) fn info(&self) -> ObjectInfo {
        self.info.clone()
    }

    pub(crate) fn partial_path(&self) -> &Path {
        &self.partial_path
    }

    pub(crate) fn nonce(&self) -> u64 {
        self.nonce
    }

    pub(crate) async fn complete_meta(
        &self,
    ) -> StorageResult<Option<CachedObjectMeta>> {
        let state = self.fill_state.lock().await;
        match &*state {
            LargeFillState::Filling { .. } => Ok(None),
            LargeFillState::Complete { meta } => Ok(Some(meta.clone())),
            LargeFillState::Aborted => {
                Err(StorageError::cache_fill_aborted(self.key()))
            }
        }
    }

    pub(crate) async fn ensure_openable(&self) -> StorageResult<()> {
        let state = self.fill_state.lock().await;
        match &*state {
            LargeFillState::Filling { .. } | LargeFillState::Complete { .. } => {
                Ok(())
            }
            LargeFillState::Aborted => {
                Err(StorageError::cache_fill_aborted(self.key()))
            }
        }
    }

    /// Defensive guard invoked from
    /// [`crate::cache::CacheManager::write_large_chunk_unlocked`] while holding the per-object
    /// lock. In the normal production call-graph the state must be `Filling` here: the caller
    /// took the object lock after already claiming an `InFlight` chunk slot, and only that path
    /// (under the same lock) commits `Complete` via [`Self::mark_complete`]. `Aborted` is likewise
    /// only reached from paths that hold the object lock (abort on write failure, rename failure,
    /// invalidate, reaper). The `Complete` match arm exists to surface a programmer invariant
    /// violation if those assumptions are ever broken by a future refactor; the `debug_assert!`
    /// turns such a bug into a test failure, while release builds still return a clear error
    /// rather than panicking.
    pub(crate) async fn ensure_filling(&self) -> StorageResult<()> {
        let state = self.fill_state.lock().await;
        match &*state {
            LargeFillState::Filling { .. } => Ok(()),
            LargeFillState::Complete { .. } => {
                debug_assert!(
                    false,
                    "ensure_filling saw Complete state for {}; caller must hold the object lock and a Filling-claimed chunk",
                    self.key(),
                );
                Err(StorageError::cache(format!(
                    "large fill invariant violated: session for {} is already Complete while a chunk write is in progress",
                    self.key()
                )))
            }
            LargeFillState::Aborted => {
                Err(StorageError::cache_fill_aborted(self.key()))
            }
        }
    }

    pub(crate) async fn claim_chunk(
        &self,
        chunk: u64,
    ) -> StorageResult<ChunkFillClaim> {
        loop {
            let mut state = self.fill_state.lock().await;
            match &mut *state {
                LargeFillState::Filling { chunks } => {
                    let Some(slot) = chunks.slots.get_mut(chunk as usize) else {
                        return Err(StorageError::cache(format!(
                            "chunk {chunk} out of bounds for {}",
                            self.key()
                        )));
                    };
                    match slot {
                        ChunkSlot::Complete => return Ok(ChunkFillClaim::Complete),
                        ChunkSlot::Missing => {
                            let flight = Arc::new(ChunkFlight::new());
                            *slot = ChunkSlot::InFlight(flight.clone());
                            return Ok(ChunkFillClaim::Leader(ChunkFillLeader::new(
                                flight,
                            )));
                        }
                        ChunkSlot::InFlight(flight) if flight.failed() => {
                            *slot = ChunkSlot::Missing;
                        }
                        ChunkSlot::InFlight(flight) => {
                            return Ok(ChunkFillClaim::Follower(
                                ChunkFillWaiter::new(flight.clone()),
                            ));
                        }
                    }
                }
                LargeFillState::Complete { .. } => {
                    return Ok(ChunkFillClaim::Complete);
                }
                LargeFillState::Aborted => {
                    return Err(StorageError::cache_fill_aborted(self.key()));
                }
            }
        }
    }

    pub(crate) async fn record_chunk_complete(
        &self,
        chunk: u64,
        leader: ChunkFillLeader,
    ) -> StorageResult<bool> {
        let mut state = self.fill_state.lock().await;
        match &mut *state {
            LargeFillState::Filling { chunks } => {
                let Some(slot) = chunks.slots.get_mut(chunk as usize) else {
                    return Err(StorageError::cache(format!(
                        "chunk {chunk} out of bounds for {}",
                        self.key()
                    )));
                };
                match slot {
                    ChunkSlot::InFlight(flight) if leader.flight_ptr_eq(flight) => {
                        *slot = ChunkSlot::Complete;
                        chunks.completed_count =
                            chunks.completed_count.saturating_add(1);
                        leader.finish(ChunkFlightOutcome::Succeeded);
                        Ok(chunks.completed_count as usize == chunks.slots.len())
                    }
                    ChunkSlot::Complete => {
                        leader.finish(ChunkFlightOutcome::Succeeded);
                        Ok(chunks.completed_count as usize == chunks.slots.len())
                    }
                    ChunkSlot::Missing | ChunkSlot::InFlight(_) => {
                        Err(StorageError::cache(format!(
                            "chunk {chunk} completion does not match active fill leader for {}",
                            self.key()
                        )))
                    }
                }
            }
            LargeFillState::Complete { .. } => {
                leader.finish(ChunkFlightOutcome::Succeeded);
                Ok(true)
            }
            LargeFillState::Aborted => {
                Err(StorageError::cache_fill_aborted(self.key()))
            }
        }
    }

    pub(crate) async fn mark_complete(
        &self,
        meta: CachedObjectMeta,
    ) -> StorageResult<()> {
        let mut state = self.fill_state.lock().await;
        match &*state {
            LargeFillState::Filling { .. } | LargeFillState::Complete { .. } => {
                *state = LargeFillState::Complete { meta };
                // Published after the async-mutex write so the plain atomic load in `Drop` is
                // synchronized by the mutex release/acquire chain with every earlier write
                // under this lock.
                self.completed.store(true, Ordering::Release);
                Ok(())
            }
            LargeFillState::Aborted => {
                Err(StorageError::cache_fill_aborted(self.key()))
            }
        }
    }

    pub(crate) async fn abort(&self) {
        let mut state = self.fill_state.lock().await;
        if let LargeFillState::Filling { chunks } = &mut *state {
            chunks.abort_inflight();
            *state = LargeFillState::Aborted;
        }
    }

    /// Returns `true` exactly once per session, for the chunk writer that owns the first physical
    /// write to the partial file. Used to open the partial with `O_TRUNC` so any stale bytes from
    /// a previous session that left the file behind are cleared before the new session writes its
    /// own data. Subsequent calls return `false`, ensuring later chunks within the same session do
    /// not erase previously written data.
    pub(crate) fn claim_partial_bootstrap(&self) -> bool {
        self.partial_bootstrapped
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

impl Drop for LargeFillSession {
    /// The single ownership-closing step for large fills.
    ///
    /// When the last `Arc<Self>` goes away:
    /// * if the session committed (see [`Self::mark_complete`]), there is no disk work left — promotion already moved
    ///   the partial and removed the registry entry
    /// * otherwise, enqueue a [`ReapRequest`] so the reaper task can take the per-object lock, abort state, delete the
    ///   partial, and clear the registry entry
    ///
    /// The `Arc<PerObjectState>` carried on the request pins the state instance the session was
    /// attached to across the reap window, so the nonce check on reap observes the same slot the
    /// session originally occupied.
    fn drop(&mut self) {
        if self.completed.load(Ordering::Acquire) {
            return;
        }
        self.reaper.send(ReapRequest {
            state: self.state.clone(),
            partial_path: self.partial_path.clone(),
            nonce: self.nonce,
        });
    }
}

impl ChunkProgress {
    fn new(chunks: usize) -> Self {
        Self {
            slots: (0..chunks).map(|_| ChunkSlot::Missing).collect(),
            completed_count: 0,
        }
    }

    fn abort_inflight(&mut self) {
        for slot in &mut self.slots {
            if let ChunkSlot::InFlight(flight) = slot {
                flight.finish(ChunkFlightOutcome::Failed);
            }
        }
    }
}
