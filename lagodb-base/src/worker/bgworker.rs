use std::fmt;
use std::ptr::NonNull;

use pgrx::bgworkers::BackgroundWorkerBuilder;
use pgrx::prelude::*;

use crate::worker::state::WorkerKey;

use super::{
    COORDINATOR_FUNCTION, COORDINATOR_TYPE, LIBRARY_NAME, WORKER_FUNCTION,
    WORKER_TYPE,
};

const _: () = assert!(usize::BITS >= 64, "worker keys require a 64-bit Datum");

impl WorkerKey {
    pub(super) fn from_datum(argument: pg_sys::Datum) -> Self {
        let packed = argument.value() as u64;
        Self::new((packed >> 32) as u32, packed as u32 as i32)
    }

    pub(super) fn into_datum(self) -> pg_sys::Datum {
        let packed =
            (u64::from(self.database_oid) << 32) | u64::from(self.worker_id as u32);
        pg_sys::Datum::from(packed as usize)
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

/// PostgreSQL owns a dynamic worker as soon as registration succeeds. Its
/// process-local handle is retained only until the creator observes startup;
/// later runtime state comes from the worker entrypoint and exit callback.
pub(super) struct DynamicWorkerRegistration {
    raw: NonNull<pg_sys::BackgroundWorkerHandle>,
}

struct DynamicRegistrationMemoryContext {
    raw: pg_sys::MemoryContext,
    parent: pg_sys::MemoryContext,
}

impl DynamicRegistrationMemoryContext {
    fn new() -> Self {
        let parent = unsafe { pg_sys::CurrentMemoryContext };
        // SAFETY: the parent is the current live backend context and the static
        // name outlives the child context.
        let raw = unsafe {
            pg_sys::AllocSetContextCreateExtended(
                parent,
                c"lagodb dynamic worker registration".as_ptr(),
                pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
            )
        };
        Self { raw, parent }
    }

    fn run(self, operation: impl FnOnce()) {
        // SAFETY: `raw` is live and owned by this guard. The operation returns
        // no PostgreSQL pointer allocated in the child context.
        unsafe { pg_sys::MemoryContextSwitchTo(self.raw) };
        operation();
        unsafe { pg_sys::MemoryContextSwitchTo(self.parent) };
    }
}

impl Drop for DynamicRegistrationMemoryContext {
    fn drop(&mut self) {
        // SAFETY: if unwinding left the child current, restore its live parent
        // before deleting the uniquely owned child context.
        unsafe {
            if pg_sys::CurrentMemoryContext == self.raw {
                pg_sys::MemoryContextSwitchTo(self.parent);
            }
            pg_sys::MemoryContextDelete(self.raw);
        }
    }
}

impl DynamicWorkerRegistration {
    pub(super) fn with_transient_context(operation: impl FnOnce()) {
        DynamicRegistrationMemoryContext::new().run(operation);
    }

    pub(super) fn register_coordinator(
        database_oid: u32,
    ) -> Result<Self, DynamicWorkerStartError> {
        let name = format!("lagodb coordinator db={database_oid}");
        Self::register(
            COORDINATOR_FUNCTION,
            name,
            COORDINATOR_TYPE,
            pg_sys::Datum::from(database_oid as usize),
        )
    }

    pub(super) fn register_worker(
        key: WorkerKey,
    ) -> Result<Self, DynamicWorkerStartError> {
        let name =
            format!("lagodb worker db={} id={}", key.database_oid, key.worker_id,);
        Self::register(WORKER_FUNCTION, name, WORKER_TYPE, key.into_datum())
    }

    fn register(
        function: &str,
        name: String,
        worker_type: &str,
        argument: pg_sys::Datum,
    ) -> Result<Self, DynamicWorkerStartError> {
        let builder = BackgroundWorkerBuilder::new(&name)
            .set_type(worker_type)
            .set_library(LIBRARY_NAME)
            .set_function(function)
            .set_argument(Some(argument))
            .set_notify_pid(unsafe { pg_sys::MyProcPid })
            .enable_spi_access()
            .set_restart_time(None);
        let mut worker: pg_sys::BackgroundWorker = (&builder).into();
        let mut raw = std::ptr::null_mut();
        // SAFETY: pgrx initialized `worker`; `raw` receives PostgreSQL's handle
        // for this registration in the current backend memory context.
        let registered =
            unsafe { pg_sys::RegisterDynamicBackgroundWorker(&mut worker, &mut raw) };
        if !registered {
            return Err(DynamicWorkerStartError::Load);
        }
        let raw = NonNull::new(raw)
            .expect("successful dynamic worker registration returns a handle");
        Ok(Self { raw })
    }

    pub(super) fn wait_for_startup(self) -> DynamicWorkerStartResult {
        let mut pid = 0;
        // SAFETY: `raw` is the non-null handle returned by this registration,
        // and this is its creator backend. The wait occurs with no runtime
        // LWLock or database lifecycle lock held.
        let status = unsafe {
            pg_sys::WaitForBackgroundWorkerStartup(self.raw.as_ptr(), &mut pid)
        };
        match status {
            pg_sys::BgwHandleStatus::BGWH_STARTED => {
                DynamicWorkerStartResult::Started(pid)
            }
            pg_sys::BgwHandleStatus::BGWH_STOPPED => {
                DynamicWorkerStartResult::Stopped
            }
            pg_sys::BgwHandleStatus::BGWH_POSTMASTER_DIED => {
                DynamicWorkerStartResult::PostmasterDied
            }
            pg_sys::BgwHandleStatus::BGWH_NOT_YET_STARTED => {
                DynamicWorkerStartResult::Stopped
            }
            _ => DynamicWorkerStartResult::PostmasterDied,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DynamicWorkerStartResult {
    Started(i32),
    Stopped,
    PostmasterDied,
}

pub(super) fn timestamp_ms() -> i64 {
    unsafe { pg_sys::GetCurrentTimestamp() / 1_000 }
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
