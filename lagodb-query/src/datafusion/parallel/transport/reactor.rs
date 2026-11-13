//! The runtime that parks in PostgreSQL's sleep protocol.
//!
//! Two hooks on the current-thread runtime announce the sleep and recheck the
//! latch in `WaitEventSetWait`'s order, and a `SIGURG` descriptor keeps the
//! actual wait wakeable.

use std::future::Future;
use std::os::fd::{AsRawFd, RawFd};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Waker;

use futures::task::AtomicWaker;
use lagodb_core::backend::{BackendLatch, BackendProcess, SignalWakeup};
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
use tokio::runtime::{Builder, Runtime};

use crate::datafusion::error::QueryExecutionError;
use crate::datafusion::parallel::host::{
    ParallelExecutionHost, ParallelInterrupt, ParallelInterruptGuard,
};

use super::watch::{ProcessSignal, ProcessWatch};

/// Wakers of one run, in fixed local slots: two per local stream of this
/// process. `wake_all` walks only the registered prefix, as `WaitEventSetWait`
/// walks `nevents` after `AddWaitEventToSet` appended at a dense position.
pub(in crate::datafusion) struct WakerSet {
    /// The run watch registers here; it is not a connection.
    run_watch: AtomicWaker,
    /// Two per local stream, reserved by [`Self::allocate_stream`].
    stream_slots: Box<[AtomicWaker]>,
    /// Prefix of `stream_slots` handed out so far.
    registered: AtomicUsize,
    active: AtomicBool,
}

/// Position of one waker in its run's [`WakerSet`], not a DSM connection index.
#[derive(Clone, Copy)]
pub(super) struct WakerSlot(usize);

impl WakerSet {
    /// Wakers for a run of `workers` attached workers.
    ///
    /// Capacity is the widest participant's local streams: a worker drives
    /// `W - 1` client and `W` server streams, each with a read and a write slot.
    pub(in crate::datafusion) fn new(workers: u32) -> Self {
        let slots = 2 * (2 * workers as usize - 1);
        Self {
            run_watch: AtomicWaker::new(),
            stream_slots: (0..slots).map(|_| AtomicWaker::new()).collect(),
            registered: AtomicUsize::new(0),
            active: AtomicBool::new(true),
        }
    }

    /// Reserves the read and write slot of one local stream, which is built once
    /// per connection per process.
    pub(super) fn allocate_stream(&self) -> (WakerSlot, WakerSlot) {
        let read = self.registered.fetch_add(2, Ordering::AcqRel);
        debug_assert!(
            read + 1 < self.stream_slots.len(),
            "a run has more local streams than its WakerSet reserved"
        );
        (WakerSlot(read), WakerSlot(read + 1))
    }

    /// Registers `waker` for `slot`.
    pub(super) fn register(&self, slot: WakerSlot, waker: &Waker) {
        self.stream_slots[slot.0].register(waker);
    }

    /// The waker the run watch registers in.
    pub(in crate::datafusion) fn run_watch(&self) -> &AtomicWaker {
        &self.run_watch
    }

    pub(in crate::datafusion) fn wake_all(&self) {
        if !self.active.load(Ordering::Acquire) {
            return;
        }
        for slot in &self.stream_slots[..self.registered.load(Ordering::Acquire)] {
            slot.wake();
        }
        self.run_watch.wake();
    }

    /// Drops every registered waker; the run is no longer attached.
    fn deactivate(&self) {
        self.active.store(false, Ordering::Release);
        for slot in &self.stream_slots[..self.registered.load(Ordering::Acquire)] {
            slot.take();
        }
        self.run_watch.take();
    }
}

/// State the park hooks read: which run is attached, and its host services.
struct ParkState {
    wakeup: &'static SignalWakeup,
    host: Arc<dyn ParallelExecutionHost>,
    process_waker: AtomicWaker,
    wakers: Mutex<Option<Arc<WakerSet>>>,
}

impl ParkState {
    fn attached(&self) -> Option<Arc<WakerSet>> {
        // The hooks and the attachment run on the runtime thread, so this lock
        // is never contended and never held across a yield point.
        self.wakers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Wakes the process watch and, when present, the run's watches.
    fn wake_all(&self) {
        self.process_waker.wake();
        if let Some(wakers) = self.attached() {
            wakers.wake_all();
        }
    }

    /// The sleep half of `WaitEventSetWait`, in its order: drain the signal
    /// descriptor, arm the trampoline, announce the sleep, barrier, then
    /// recheck the latch and the interrupt flags.
    fn park(&self) {
        self.wakeup.begin_park();
        let signalled = BackendLatch::prepare_sleep();
        let interrupt = self.host.interrupt();
        if signalled || interrupt != ParallelInterrupt::None {
            self.wakeup.end_park();
            let pending = BackendLatch::finish_sleep();
            if pending || interrupt != ParallelInterrupt::None {
                self.wake_all();
            }
        }
    }

    /// The wake half; `ResetLatch` asserts that no peer may still signal this
    /// process, so the announcement is withdrawn first.
    fn unpark(&self) {
        self.wakeup.end_park();
        if BackendLatch::finish_sleep() {
            self.wake_all();
        }
    }
}

/// A descriptor this owner registers but does not close.
pub(in crate::datafusion) struct BorrowedFd(RawFd);

impl AsRawFd for BorrowedFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

/// The reactor of one runtime: descriptor registrations plus the park hooks.
pub(in crate::datafusion) struct ParallelReactor {
    /// Never polled: the park hook drains it synchronously, and registration
    /// alone is what makes the kernel wake the parked thread.
    _signal: AsyncFd<BorrowedFd>,
    postmaster: AsyncFd<BorrowedFd>,
    state: Arc<ParkState>,
}

impl ParallelReactor {
    /// Registers the wakeup and postmaster descriptors with the entered runtime.
    fn register(
        host: Arc<dyn ParallelExecutionHost>,
        state: Arc<ParkState>,
    ) -> Result<Self, QueryExecutionError> {
        // `AsyncFd::new` panics without a reactor, so the caller enters the
        // runtime first. Both descriptors are read-only.
        let signal =
            AsyncFd::with_interest(BorrowedFd(state.wakeup.fd()), Interest::READABLE)
                .map_err(QueryExecutionError::Transport)?;
        let postmaster = AsyncFd::with_interest(
            BorrowedFd(host.postmaster_fd()),
            Interest::READABLE,
        )
        .map_err(QueryExecutionError::Transport)?;
        Ok(Self {
            _signal: signal,
            postmaster,
            state,
        })
    }

    pub(in crate::datafusion) fn postmaster(&self) -> &AsyncFd<BorrowedFd> {
        &self.postmaster
    }

    pub(in crate::datafusion) fn host(&self) -> &Arc<dyn ParallelExecutionHost> {
        &self.state.host
    }

    pub(super) fn process_waker(&self) -> &AtomicWaker {
        &self.state.process_waker
    }

    /// Attaches one run's wakers; a current-thread runtime drives one run at a
    /// time and the previous attachment is released before the next launch.
    pub(in crate::datafusion) fn attach(
        &self,
        wakers: Arc<WakerSet>,
    ) -> ReactorAttachment {
        let previous = self
            .state
            .wakers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(wakers);
        assert!(
            previous.is_none(),
            "a current-thread runtime runs one parallel attachment at a time"
        );
        ReactorAttachment {
            state: Arc::clone(&self.state),
        }
    }
}

/// One run's attachment to a reactor.
///
/// Dropping it detaches the run's wakers before the run releases DSM, so no hook
/// can wake a stream whose mapping is going away.
pub(in crate::datafusion) struct ReactorAttachment {
    state: Arc<ParkState>,
}

impl Drop for ReactorAttachment {
    fn drop(&mut self) {
        let attached = self
            .state
            .wakers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(wakers) = attached {
            wakers.deactivate();
        }
    }
}

/// Runtime-scoped file descriptor budget, released after the runtime closes.
struct FdBudget {
    host: Arc<dyn ParallelExecutionHost>,
}

impl Drop for FdBudget {
    fn drop(&mut self) {
        self.host.release_runtime_fds();
    }
}

/// The current-thread runtime a statement executes on.
///
/// A parallel-eligible statement gets the reactor, because the runtime that
/// compiles its plan is the runtime that drives it and whether the plan turns out
/// parallel is only known afterwards. A statement without a parallel option
/// keeps a plain time-enabled runtime.
pub(in crate::datafusion) struct ParallelRuntime {
    // Field order is the release order: the descriptor registrations, then the
    // runtime, then the budget that counts its descriptors.
    reactor: Option<ParallelReactor>,
    runtime: Runtime,
    _budget: Option<FdBudget>,
}

impl ParallelRuntime {
    /// Builds a reactor-enabled runtime, preserving every construction error so
    /// the caller can decide whether its lifecycle permits serial fallback.
    pub(in crate::datafusion) fn with_reactor(
        host: &Arc<dyn ParallelExecutionHost>,
    ) -> Result<Self, QueryExecutionError> {
        host.acquire_runtime_fds()
            .map_err(QueryExecutionError::ParallelHost)?;
        let budget = FdBudget {
            host: Arc::clone(host),
        };
        let wakeup = host
            .acquire_wakeup()
            .map_err(QueryExecutionError::ParallelHost)?;
        let state = Arc::new(ParkState {
            wakeup,
            host: Arc::clone(host),
            process_waker: AtomicWaker::new(),
            wakers: Mutex::new(None),
        });
        let park = Arc::clone(&state);
        let unpark = Arc::clone(&state);
        let runtime = Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .on_thread_park(move || park.park())
            .on_thread_unpark(move || unpark.unpark())
            .build()
            .map_err(QueryExecutionError::Runtime)?;
        // A descriptor can only be registered once the runtime context exists.
        let registered = {
            let _entered = runtime.enter();
            ParallelReactor::register(Arc::clone(host), Arc::clone(&state))
        };
        let reactor = registered?;
        Ok(Self {
            reactor: Some(reactor),
            runtime,
            _budget: Some(budget),
        })
    }

    pub(in crate::datafusion) fn serial() -> Result<Self, QueryExecutionError> {
        Ok(Self {
            reactor: None,
            runtime: Builder::new_current_thread()
                .enable_time()
                .build()
                .map_err(QueryExecutionError::Runtime)?,
            _budget: None,
        })
    }

    /// `None` when the statement runs serially.
    pub(in crate::datafusion) fn reactor(&self) -> Option<&ParallelReactor> {
        self.reactor.as_ref()
    }

    pub(in crate::datafusion) fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    /// Runs `future` with runtime-scoped PostgreSQL event handling.
    ///
    /// Every `block_on` on a reactor-enabled runtime comes through here: the hold
    /// keeps an interrupt from unwinding mid-poll and withdraws a park
    /// announcement the runtime may never have cleared. Process events are
    /// handled after the guard drops, and service events resume the same pinned
    /// future.
    pub(in crate::datafusion) fn block_on<F: Future>(
        &self,
        future: F,
    ) -> Result<F::Output, QueryExecutionError> {
        let Some(reactor) = &self.reactor else {
            return Ok(self.runtime.block_on(future));
        };
        let mut future = pin!(future);
        loop {
            let step = {
                let _held = ParallelInterruptGuard::new(reactor.host().as_ref());
                self.runtime.block_on(async {
                    tokio::select! {
                        biased;
                        signal = ProcessWatch::new(reactor) => BlockOnStep::Process(signal),
                        output = future.as_mut() => BlockOnStep::Output(output),
                    }
                })
            };
            match step {
                BlockOnStep::Output(output) => {
                    reactor
                        .host()
                        .process_interrupts()
                        .map_err(QueryExecutionError::ParallelHost)?;
                    return Ok(output);
                }
                BlockOnStep::Process(ProcessSignal::Service) => {
                    reactor
                        .host()
                        .process_interrupts()
                        .map_err(QueryExecutionError::ParallelHost)?;
                }
                BlockOnStep::Process(ProcessSignal::Abort) => {
                    reactor
                        .host()
                        .process_interrupts()
                        .map_err(QueryExecutionError::ParallelHost)?;
                    return Err(QueryExecutionError::interrupted().into());
                }
                BlockOnStep::Process(ProcessSignal::PostmasterDied) => {
                    BackendProcess::exit(1)
                }
            }
        }
    }
}

/// One exit from the interrupt-held inner Tokio wait.
enum BlockOnStep<T> {
    Output(T),
    Process(ProcessSignal),
}
