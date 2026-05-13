//! Per-chunk coordination primitives.
//!
//! A chunk slot goes through `Missing -> InFlight -> Complete`. The single writer that turned
//! `Missing` into `InFlight` is the [`ChunkFillLeader`]; any concurrent reader that found
//! `InFlight` subscribes as a [`ChunkFillWaiter`]. The outcome of the leader's I/O is published
//! through a [`tokio::sync::watch`] channel so followers observe success or failure without
//! holding the session's async mutex.

use std::sync::Arc;

use tokio::sync::watch;

use crate::error::StorageResult;

/// Result of attempting to fill a chunk: either the chunk is already durable, the caller is the
/// elected writer, or the caller joins an in-flight writer as a follower.
pub(crate) enum ChunkFillClaim {
    Complete,
    Leader(ChunkFillLeader),
    Follower(ChunkFillWaiter),
}

/// The single writer authorised to produce the chunk's bytes. Drops to `Failed` automatically if
/// the leader is dropped without calling [`ChunkFillLeader::finish`] so followers never wait
/// forever on a panicked or cancelled task.
pub(crate) struct ChunkFillLeader {
    flight: Arc<ChunkFlight>,
    finished: bool,
}

/// A follower attached to an in-flight chunk; [`Self::wait`] resolves once the leader publishes
/// an outcome.
pub(crate) struct ChunkFillWaiter {
    flight: Arc<ChunkFlight>,
}

/// Internal shared state behind a chunk flight.
pub(super) struct ChunkFlight {
    outcome: watch::Sender<Option<ChunkFlightOutcome>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ChunkFlightOutcome {
    Succeeded,
    Failed,
}

impl ChunkFlight {
    pub(super) fn new() -> Self {
        let (outcome, _) = watch::channel(None);
        Self { outcome }
    }

    pub(super) fn finish(&self, outcome: ChunkFlightOutcome) {
        self.outcome.send_replace(Some(outcome));
    }

    pub(super) fn failed(&self) -> bool {
        matches!(*self.outcome.borrow(), Some(ChunkFlightOutcome::Failed))
    }

    fn subscribe(&self) -> watch::Receiver<Option<ChunkFlightOutcome>> {
        self.outcome.subscribe()
    }
}

impl ChunkFillLeader {
    pub(super) fn new(flight: Arc<ChunkFlight>) -> Self {
        Self {
            flight,
            finished: false,
        }
    }

    pub(super) fn flight_ptr_eq(&self, other: &Arc<ChunkFlight>) -> bool {
        Arc::ptr_eq(&self.flight, other)
    }

    pub(super) fn finish(mut self, outcome: ChunkFlightOutcome) {
        self.flight.finish(outcome);
        self.finished = true;
    }
}

impl Drop for ChunkFillLeader {
    fn drop(&mut self) {
        if !self.finished {
            self.flight.finish(ChunkFlightOutcome::Failed);
        }
    }
}

impl ChunkFillWaiter {
    pub(super) fn new(flight: Arc<ChunkFlight>) -> Self {
        Self { flight }
    }

    pub(crate) async fn wait(self) -> StorageResult<bool> {
        let mut outcome = self.flight.subscribe();
        loop {
            match *outcome.borrow() {
                Some(ChunkFlightOutcome::Succeeded) => return Ok(true),
                Some(ChunkFlightOutcome::Failed) => return Ok(false),
                None => {},
            }
            if outcome.changed().await.is_err() {
                return Ok(false);
            }
        }
    }
}
