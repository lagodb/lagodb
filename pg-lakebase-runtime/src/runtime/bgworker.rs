use std::fmt;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::rc::Rc;
use std::time::Duration;

use pgrx::bgworkers::BackgroundWorkerBuilder;
use pgrx::prelude::*;

use super::LIBRARY_NAME;

const _: () = assert!(usize::BITS >= 64, "worker tokens require a 64-bit Datum");

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct PackedSlotToken {
    index: usize,
    generation: u32,
}

impl PackedSlotToken {
    const fn new(index: usize, generation: u32) -> Self {
        Self { index, generation }
    }

    const fn index(self) -> usize {
        self.index
    }

    const fn generation(self) -> u32 {
        self.generation
    }

    fn from_datum(argument: pg_sys::Datum) -> Self {
        Self::unpack(argument.value())
    }

    fn into_datum(self) -> pg_sys::Datum {
        pg_sys::Datum::from(self.pack())
    }

    fn pack(self) -> usize {
        ((self.generation as u64) << 32 | self.index as u64) as usize
    }

    fn unpack(argument: usize) -> Self {
        Self {
            index: argument as u32 as usize,
            generation: (argument as u64 >> 32) as u32,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct ReconcilerToken(PackedSlotToken);

impl ReconcilerToken {
    pub(super) const fn new(index: usize, generation: u32) -> Self {
        Self(PackedSlotToken::new(index, generation))
    }

    pub(super) const fn index(self) -> usize {
        self.0.index()
    }

    pub(super) const fn generation(self) -> u32 {
        self.0.generation()
    }

    pub(super) fn from_datum(argument: pg_sys::Datum) -> Self {
        Self(PackedSlotToken::from_datum(argument))
    }

    pub(super) fn into_datum(self) -> pg_sys::Datum {
        self.0.into_datum()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct WorkerToken(PackedSlotToken);

impl WorkerToken {
    pub(super) const fn new(index: usize, generation: u32) -> Self {
        Self(PackedSlotToken::new(index, generation))
    }

    pub(super) const fn index(self) -> usize {
        self.0.index()
    }

    pub(super) const fn generation(self) -> u32 {
        self.0.generation()
    }

    pub(super) fn from_datum(argument: pg_sys::Datum) -> Self {
        Self(PackedSlotToken::from_datum(argument))
    }

    pub(super) fn into_datum(self) -> pg_sys::Datum {
        self.0.into_datum()
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum DynamicWorkerStartError {
    Load,
}

impl fmt::Display for DynamicWorkerStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Load => {
                formatter.write_str("RegisterDynamicBackgroundWorker returned false")
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum HandleStatus {
    Started(i32),
    NotYetStarted,
    Stopped,
    PostmasterDied,
}

/// Launcher-local ownership of PostgreSQL's opaque background-worker handle.
///
/// The handle is deliberately neither `Send` nor `Sync`: PostgreSQL allocates
/// it in the registering backend and its APIs must be called by that backend.
pub(super) struct LauncherWorkerHandle {
    raw: NonNull<pg_sys::BackgroundWorkerHandle>,
    _not_send: PhantomData<Rc<()>>,
}

impl LauncherWorkerHandle {
    pub(super) const fn raw_ptr(&self) -> *mut pg_sys::BackgroundWorkerHandle {
        self.raw.as_ptr()
    }

    pub(super) fn register_reconciler(
        function: &str,
        name: &str,
        worker_type: &str,
        token: ReconcilerToken,
    ) -> Result<Self, DynamicWorkerStartError> {
        Self::register(function, name, worker_type, token.into_datum())
    }

    pub(super) fn register_worker(
        function: &str,
        name: &str,
        worker_type: &str,
        token: WorkerToken,
    ) -> Result<Self, DynamicWorkerStartError> {
        Self::register(function, name, worker_type, token.into_datum())
    }

    fn register(
        function: &str,
        name: &str,
        worker_type: &str,
        argument: pg_sys::Datum,
    ) -> Result<Self, DynamicWorkerStartError> {
        let builder = BackgroundWorkerBuilder::new(name)
            .set_type(worker_type)
            .set_library(LIBRARY_NAME)
            .set_function(function)
            .set_argument(Some(argument))
            .set_notify_pid(unsafe { pg_sys::MyProcPid })
            .enable_spi_access()
            .set_restart_time(None);
        let mut worker: pg_sys::BackgroundWorker = (&builder).into();
        let mut raw = std::ptr::null_mut();
        // RegisterDynamicBackgroundWorker pallocs the returned handle in the
        // current memory context. Registration can happen inside a short-lived
        // PostgreSQL transaction, while the launcher must retain the handle until
        // BGWH_STOPPED. Allocate it in TopMemoryContext so the handle remains
        // valid for the launcher's lifetime.
        //
        // SAFETY: worker is fully initialized by pgrx's public conversion. The
        // registration call does not unwind through Rust for this valid worker,
        // and the previous context is restored immediately afterwards.
        let registered = unsafe {
            let previous = pg_sys::MemoryContextSwitchTo(pg_sys::TopMemoryContext);
            let registered =
                pg_sys::RegisterDynamicBackgroundWorker(&mut worker, &mut raw);
            pg_sys::MemoryContextSwitchTo(previous);
            registered
        };
        if !registered {
            return Err(DynamicWorkerStartError::Load);
        }
        let raw = NonNull::new(raw).ok_or(DynamicWorkerStartError::Load)?;
        Ok(Self {
            raw,
            _not_send: PhantomData,
        })
    }

    pub(super) fn status(&self) -> HandleStatus {
        let mut pid = 0;
        // SAFETY: raw remains owned by this launcher and is not released until
        // this method reports BGWH_STOPPED.
        let status =
            unsafe { pg_sys::GetBackgroundWorkerPid(self.raw.as_ptr(), &mut pid) };
        match status {
            pg_sys::BgwHandleStatus::BGWH_STARTED => HandleStatus::Started(pid),
            pg_sys::BgwHandleStatus::BGWH_NOT_YET_STARTED => {
                HandleStatus::NotYetStarted
            }
            pg_sys::BgwHandleStatus::BGWH_STOPPED => HandleStatus::Stopped,
            pg_sys::BgwHandleStatus::BGWH_POSTMASTER_DIED => {
                HandleStatus::PostmasterDied
            }
            _ => HandleStatus::PostmasterDied,
        }
    }

    pub(super) fn request_termination(&mut self) {
        // SAFETY: raw is the live handle returned by PostgreSQL. Termination is
        // idempotent and is valid before startup, while running, or after stop.
        unsafe { pg_sys::TerminateBackgroundWorker(self.raw.as_ptr()) };
    }

    pub(super) fn release_after_stopped(self) {
        debug_assert_eq!(self.status(), HandleStatus::Stopped);
        // SAFETY: PostgreSQL documents this handle as palloc'd and releasable
        // with pfree. The stopped observation is the runtime ownership fence.
        unsafe { pg_sys::pfree(self.raw.as_ptr().cast()) };
    }
}

pub(super) fn timestamp_ms() -> i64 {
    unsafe { pg_sys::GetCurrentTimestamp() / 1_000 }
}

pub(super) fn interruptible_sleep(duration: Duration) {
    let timeout =
        libc::c_long::try_from(duration.as_millis()).unwrap_or(libc::c_long::MAX);
    let events = (pg_sys::WL_LATCH_SET
        | pg_sys::WL_TIMEOUT
        | pg_sys::WL_POSTMASTER_DEATH) as i32;
    // SAFETY: called from a PostgreSQL backend main thread after dropping all
    // runtime LWLock guards. MyLatch is owned by the current backend process.
    unsafe {
        let events = pg_sys::WaitLatch(
            pg_sys::MyLatch,
            events,
            timeout,
            pg_sys::PG_WAIT_EXTENSION,
        );
        pg_sys::ResetLatch(pg_sys::MyLatch);
        if events & pg_sys::WL_POSTMASTER_DEATH as i32 != 0 {
            pg_sys::proc_exit(1);
        }
    }
    check_for_interrupts();
}

pub(super) fn check_for_interrupts() {
    // SAFETY: called from a PostgreSQL backend with no runtime LWLock guard or
    // other Rust-owned resource crossing a possible PostgreSQL ERROR. This is
    // the Rust equivalent of CHECK_FOR_INTERRUPTS().
    unsafe {
        if (&raw const pg_sys::InterruptPending).read_volatile() != 0 {
            pg_sys::ProcessInterrupts();
        }
    }
}
