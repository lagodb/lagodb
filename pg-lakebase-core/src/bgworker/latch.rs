//! PostgreSQL latch waits used while draining worker-owned Rust resources.
//!
//! This module is intentionally not a replacement for a normal bgworker main
//! loop. Normal loops should continue using PostgreSQL/pgrx lifecycle-aware
//! waits and interrupt checks. The helpers here are for teardown paths that
//! have already accepted shutdown and only need a bounded, latch-aware tick
//! while waiting for Rust tasks or threads to finish.

use std::ffi::c_long;
use std::time::Duration;

use pgrx::pg_sys;

/// Result of one teardown latch wait.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "handle PostmasterDied or call exit_on_postmaster_death"]
pub enum TeardownLatchWait {
    /// The wait ended because the latch was set or the timeout elapsed.
    Woke,
    /// The postmaster has died; the backend should stop using PostgreSQL.
    PostmasterDied,
}

impl TeardownLatchWait {
    /// Exit the current backend if the wait observed postmaster death.
    pub fn exit_on_postmaster_death(self) {
        if matches!(self, Self::PostmasterDied) {
            // SAFETY: after postmaster death, bgworkers must stop using
            // PostgreSQL services. `proc_exit` follows PostgreSQL's normal
            // process-exit path for this backend.
            unsafe { pg_sys::proc_exit(1) };
        }
    }
}

/// Access to the current backend's `MyLatch`.
pub struct BackendLatch;

impl BackendLatch {
    /// Wait for a bounded teardown tick without consuming pgrx signal flags.
    pub fn teardown_tick(timeout: Duration) -> TeardownLatchWait {
        let timeout_ms = c_long::try_from(timeout.as_millis()).unwrap_or(c_long::MAX);
        let events = (pg_sys::WL_LATCH_SET
            | pg_sys::WL_TIMEOUT
            | pg_sys::WL_POSTMASTER_DEATH) as i32;
        // SAFETY: called from a PostgreSQL backend or bgworker main thread.
        // MyLatch belongs to the current process. This neutral wait
        // intentionally does not call CHECK_FOR_INTERRUPTS or consume pgrx
        // SIGTERM state, because teardown callers have already decided to
        // drain worker-owned Rust resources before returning/exiting.
        let events = unsafe {
            let events = pg_sys::WaitLatch(
                pg_sys::MyLatch,
                events,
                timeout_ms,
                pg_sys::PG_WAIT_EXTENSION,
            );
            pg_sys::ResetLatch(pg_sys::MyLatch);
            events
        };
        if events & (pg_sys::WL_POSTMASTER_DEATH as i32) != 0 {
            TeardownLatchWait::PostmasterDied
        } else {
            TeardownLatchWait::Woke
        }
    }
}
