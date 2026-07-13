use std::fmt;
use std::time::Duration;

use pgrx::bgworkers::{BackgroundWorkerBuilder, BackgroundWorkerStatus};
use pgrx::prelude::*;

use crate::state::MAX_DATABASES;

use super::LIBRARY_NAME;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SlotToken {
    index: usize,
    generation: u32,
}

impl SlotToken {
    pub(super) const fn new(index: usize, generation: u32) -> Self {
        Self { index, generation }
    }

    pub(super) const fn index(self) -> usize {
        self.index
    }

    pub(super) const fn generation(self) -> u32 {
        self.generation
    }

    pub(super) const fn has_database_index(self) -> bool {
        self.index < MAX_DATABASES
    }

    pub(super) fn from_datum(argument: pg_sys::Datum) -> Self {
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

#[derive(Clone, Copy, Debug)]
pub(super) enum DynamicWorkerStartError {
    Load,
    Startup(BackgroundWorkerStatus),
}

impl fmt::Display for DynamicWorkerStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Load => {
                formatter.write_str("RegisterDynamicBackgroundWorker returned false")
            }
            Self::Startup(status) => {
                write!(formatter, "background worker startup status was {status:?}")
            }
        }
    }
}

pub(super) struct DynamicWorkerSpawner;

impl DynamicWorkerSpawner {
    pub(super) fn start(
        function: &str,
        name: &str,
        token: SlotToken,
    ) -> Result<(), DynamicWorkerStartError> {
        let worker = BackgroundWorkerBuilder::new(name)
            .set_type(name)
            .set_library(LIBRARY_NAME)
            .set_function(function)
            .set_argument(Some(token.into_datum()))
            .set_notify_pid(unsafe { pg_sys::MyProcPid })
            .enable_spi_access()
            .set_restart_time(None)
            .load_dynamic()
            .map_err(|_| DynamicWorkerStartError::Load)?;
        worker
            .wait_for_startup()
            .map(|_| ())
            .map_err(DynamicWorkerStartError::Startup)
    }
}

pub(super) fn timestamp_ms() -> i64 {
    unsafe { pg_sys::GetCurrentTimestamp() / 1_000 }
}

pub(super) fn install_terminating_sigterm_handler() {
    // SAFETY: called on the dynamic background-worker main thread after pgrx
    // unblocks signals. PostgreSQL's standard die handler makes lock waits
    // interruptible and runs registered exit callbacks before process exit.
    unsafe {
        pg_sys::pqsignal(pg_sys::SIGTERM as i32, Some(terminating_sigterm));
    }
}

unsafe extern "C-unwind" fn terminating_sigterm(signal: i32) {
    unsafe { pg_sys::die(signal) };
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
