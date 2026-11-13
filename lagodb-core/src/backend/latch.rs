//! PostgreSQL latch waits: the park protocol of a runtime that sleeps outside
//! `WaitEventSetWait`, plus bounded teardown ticks.

use std::ffi::c_long;
use std::sync::atomic::{Ordering, fence};
use std::time::Duration;

use pgrx::pg_sys;

use super::BackendProcess;

/// Result of one teardown latch wait.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "handle PostmasterDied or call exit_on_postmaster_death"]
pub enum TeardownLatchWait {
    Woke,
    /// The backend must stop using PostgreSQL.
    PostmasterDied,
}

impl TeardownLatchWait {
    /// Exit the current backend if the wait observed postmaster death.
    pub fn exit_on_postmaster_death(self) {
        if matches!(self, Self::PostmasterDied) {
            BackendProcess::exit(1);
        }
    }
}

/// Access to the current backend's `MyLatch`.
pub struct BackendLatch;

/// Restores `MyLatch` when a nested wait consumed an event owned by its caller.
pub(crate) struct DroppedLatchGuard {
    dropped: bool,
}

impl BackendLatch {
    pub(crate) const fn preserve_dropped_latch() -> DroppedLatchGuard {
        DroppedLatchGuard { dropped: false }
    }

    /// The store half of PostgreSQL's sleep protocol: publish `maybe_sleeping`,
    /// barrier, then re-read `is_set`. `true` means a wakeup is pending and the
    /// caller must not block.
    #[must_use = "a published wakeup must be consumed instead of sleeping"]
    pub fn prepare_sleep() -> bool {
        // SAFETY: called from the backend main thread while its runtime is
        // parked; `MyLatch` is process-local and lives for the backend.
        unsafe {
            let latch = pg_sys::MyLatch;
            (&raw mut (*latch).maybe_sleeping).write_volatile(1);
            // PostgreSQL's `pg_memory_barrier()`.
            fence(Ordering::SeqCst);
            (&raw const (*latch).is_set).read_volatile() != 0
        }
    }

    /// Withdraw the sleep announcement and report whether a wakeup is pending.
    /// `maybe_sleeping` is cleared first because `ResetLatch` asserts that no
    /// other process may still signal this one.
    #[must_use = "a pending wakeup must be handed to the parked runtime"]
    pub fn finish_sleep() -> bool {
        // SAFETY: as `prepare_sleep`; the runtime is the only resetter of its
        // own latch.
        unsafe {
            let latch = pg_sys::MyLatch;
            (&raw mut (*latch).maybe_sleeping).write_volatile(0);
            if (&raw const (*latch).is_set).read_volatile() == 0 {
                return false;
            }
            pg_sys::ResetLatch(latch);
            true
        }
    }

    /// Withdraws a sleep announcement without consuming the latch, which only an
    /// unwind out of the park protocol does.
    pub fn clear_sleep_announcement() {
        // SAFETY: as `prepare_sleep`; this field is written only by the backend
        // main thread.
        unsafe {
            (&raw mut (*pg_sys::MyLatch).maybe_sleeping).write_volatile(0);
        }
    }

    /// Wait for a bounded teardown tick without consuming pgrx signal flags.
    pub fn teardown_tick(timeout: Duration) -> TeardownLatchWait {
        let timeout_ms = c_long::try_from(timeout.as_millis()).unwrap_or(c_long::MAX);
        let events = (pg_sys::WL_LATCH_SET
            | pg_sys::WL_TIMEOUT
            | pg_sys::WL_POSTMASTER_DEATH) as i32;
        // SAFETY: called from a PostgreSQL backend or bgworker main thread.
        // Teardown callers have already decided to drain worker-owned resources,
        // so this wait does not check interrupts.
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

impl DroppedLatchGuard {
    pub(crate) fn mark_dropped(&mut self) {
        self.dropped = true;
    }
}

impl Drop for DroppedLatchGuard {
    fn drop(&mut self) {
        if self.dropped {
            // SAFETY: created, marked and dropped by a nested wait on the owning
            // backend thread; `MyLatch` lives for the backend.
            unsafe {
                pg_sys::SetLatch(pg_sys::MyLatch);
            }
        }
    }
}
