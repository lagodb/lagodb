use std::ffi::{c_int, c_long};
use std::os::fd::RawFd;
use std::time::Instant;

use pgrx::pg_sys;
use ureq::unversioned::transport::NextTimeout;
use ureq::unversioned::transport::time::Duration;
use ureq::{Error, Timeout};

#[derive(Clone, Copy)]
pub(super) enum SocketInterest {
    Readable,
    Writable,
}

impl SocketInterest {
    fn event(self) -> c_int {
        match self {
            Self::Readable => pg_sys::WL_SOCKET_READABLE as c_int,
            Self::Writable => pg_sys::WL_SOCKET_WRITEABLE as c_int,
        }
    }
}

/// Waits for one backend-owned socket while preserving PostgreSQL cancel and
/// postmaster-death semantics.
#[derive(Debug, Default)]
pub(super) struct PostgresWait;

impl PostgresWait {
    pub(super) fn wait(
        &self,
        fd: RawFd,
        interest: SocketInterest,
        timeout: NextTimeout,
    ) -> Result<(), Error> {
        let socket_event = interest.event();
        let deadline = match timeout.after {
            Duration::NotHappening => None,
            Duration::Exact(duration) => Instant::now().checked_add(duration),
        };
        let timeout_flag = if deadline.is_some() {
            pg_sys::WL_TIMEOUT as c_int
        } else {
            0
        };
        let wake_events = pg_sys::WL_LATCH_SET as c_int
            | pg_sys::WL_EXIT_ON_PM_DEATH as c_int
            | socket_event
            | timeout_flag;

        loop {
            let timeout_millis = Self::timeout_millis(deadline)
                .ok_or(Error::Timeout(timeout.reason))?;
            // SAFETY: REST calls run on a PostgreSQL backend main thread. MyLatch
            // and the transport-owned descriptor remain valid for this call.
            let occurred = unsafe {
                pg_sys::WaitLatchOrSocket(
                    pg_sys::MyLatch,
                    wake_events,
                    fd,
                    timeout_millis,
                    pg_sys::PG_WAIT_EXTENSION,
                )
            };

            if occurred & pg_sys::WL_LATCH_SET as c_int != 0 {
                // SAFETY: MyLatch is initialized for the lifetime of the backend.
                unsafe { pg_sys::ResetLatch(pg_sys::MyLatch) };
                pg_sys::check_for_interrupts!();
            }
            if occurred & socket_event != 0 {
                return Ok(());
            }
            if occurred & pg_sys::WL_TIMEOUT as c_int != 0 {
                return Err(Error::Timeout(timeout.reason));
            }
        }
    }

    fn timeout_millis(deadline: Option<Instant>) -> Option<c_long> {
        let Some(deadline) = deadline else {
            return Some(0);
        };
        let duration = deadline.checked_duration_since(Instant::now())?;
        if duration.is_zero() {
            return None;
        }
        let millis = duration.as_millis()
            + u128::from(!duration.subsec_nanos().is_multiple_of(1_000_000));
        Some(millis.max(1).min(c_int::MAX as u128) as c_long)
    }
}

pub(super) fn check_deadline(deadline: Option<Instant>) -> Result<(), Error> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        Err(Error::Timeout(Timeout::Global))
    } else {
        Ok(())
    }
}
