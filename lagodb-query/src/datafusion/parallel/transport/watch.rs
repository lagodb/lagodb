//! Process- and run-scoped events that end one `block_on` wait.
//!
//! `ProcessWatch` is installed structurally by every reactor-enabled runtime
//! wait; `RunWatch` is attached only while one DSM run exists. Neither polls:
//! the park hooks, process descriptors and run waker set drive them.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use futures::Future;

use super::reactor::{ParallelReactor, WakerSet};
use super::region::TransportRegion;
use crate::datafusion::parallel::host::ParallelInterrupt;

/// Why a runtime-level wait must leave its interrupt-held Tokio frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ProcessSignal {
    Service,
    Abort,
    PostmasterDied,
}

/// PostgreSQL process events that every reactor-enabled `block_on` observes.
pub(super) struct ProcessWatch<'a> {
    reactor: &'a ParallelReactor,
}

impl<'a> ProcessWatch<'a> {
    pub(super) const fn new(reactor: &'a ParallelReactor) -> Self {
        Self { reactor }
    }
}

impl Future for ProcessWatch<'_> {
    type Output = ProcessSignal;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.reactor.process_waker().register(cx.waker());
        if self.reactor.postmaster().poll_read_ready(cx).is_ready() {
            return Poll::Ready(ProcessSignal::PostmasterDied);
        }
        match self.reactor.host().interrupt() {
            ParallelInterrupt::Abort => Poll::Ready(ProcessSignal::Abort),
            ParallelInterrupt::Service => Poll::Ready(ProcessSignal::Service),
            ParallelInterrupt::None => Poll::Pending,
        }
    }
}

impl Drop for ProcessWatch<'_> {
    fn drop(&mut self) {
        self.reactor.process_waker().take();
    }
}

/// Which side of a run is waiting.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::datafusion) enum RunRole {
    Leader,
    Worker,
}

/// Why a wait ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::datafusion) enum RunSignal {
    /// The run's mapping was detached.
    Abort,
    /// Worker: the leader published the end of the run.
    Shutdown,
}

/// Mapping state that a participant in one attached run must react to.
pub(in crate::datafusion) struct RunWatch<'a> {
    region: &'a TransportRegion,
    live: &'a AtomicBool,
    wakers: &'a WakerSet,
    role: RunRole,
}

impl<'a> RunWatch<'a> {
    pub(in crate::datafusion) fn new(
        region: &'a TransportRegion,
        live: &'a AtomicBool,
        wakers: &'a WakerSet,
        role: RunRole,
    ) -> Self {
        Self {
            region,
            live,
            wakers,
            role,
        }
    }
}

impl Future for RunWatch<'_> {
    type Output = RunSignal;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<RunSignal> {
        // Register first: every condition below can change from another process.
        self.wakers.run_watch().register(cx.waker());
        if !self.live.load(Ordering::Acquire) {
            return Poll::Ready(RunSignal::Abort);
        }
        if self.role == RunRole::Worker
            && self.region.shutdown().load(Ordering::Acquire)
        {
            return Poll::Ready(RunSignal::Shutdown);
        }
        Poll::Pending
    }
}
