//! Readiness-wait contract and the standalone `poll(2)` implementation.

use std::io;
use std::os::fd::{AsRawFd, BorrowedFd};
use std::time::Instant;

/// Socket readiness requested by the synchronous client transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SocketInterest {
    Readable,
    Writable,
}

/// Runtime-interrupt policy for a socket wait.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SocketWaitContext {
    /// Foreground request I/O may process runtime interrupts.
    Foreground,
    /// Best-effort cleanup defers runtime interrupts to a later safe point.
    Cleanup,
}

/// Runtime integration point for waiting on a nonblocking storage socket.
///
/// Foreground implementations may process runtime interrupts while waiting.
/// If interrupt processing unwinds, the active client I/O session poisons its
/// in-flight protocol connection during unwinding. Cleanup waits must defer
/// runtime interrupts to a later safe point. One waiter belongs to one
/// single-threaded connection generation and may retain readiness state across
/// calls.
pub trait SocketWait: 'static {
    /// Waits until the socket is ready for the requested operation.
    fn wait(
        &mut self,
        socket: BorrowedFd<'_>,
        interest: SocketInterest,
        context: SocketWaitContext,
        deadline: Option<Instant>,
    ) -> io::Result<()>;
}

pub(super) struct PollSocketWait;

impl SocketWait for PollSocketWait {
    fn wait(
        &mut self,
        socket: BorrowedFd<'_>,
        interest: SocketInterest,
        _context: SocketWaitContext,
        deadline: Option<Instant>,
    ) -> io::Result<()> {
        let events = match interest {
            SocketInterest::Readable => libc::POLLIN,
            SocketInterest::Writable => libc::POLLOUT,
        };
        let mut poll_fd = libc::pollfd {
            fd: socket.as_raw_fd(),
            events,
            revents: 0,
        };

        loop {
            let timeout = Self::timeout(deadline)?;
            // SAFETY: `poll_fd` points to one initialized pollfd for the
            // duration of the call. `socket` keeps its descriptor borrowed.
            let ready = unsafe { libc::poll(&mut poll_fd, 1, timeout) };
            if ready > 0 {
                return Ok(());
            }
            if ready == 0 {
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "storage socket wait timed out",
                    ));
                }
                continue;
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }
}

impl PollSocketWait {
    fn timeout(deadline: Option<Instant>) -> io::Result<i32> {
        let Some(deadline) = deadline else {
            return Ok(-1);
        };
        let remaining =
            deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        "storage socket wait timed out",
                    )
                })?;
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "storage socket wait timed out",
            ));
        }
        let round_up = if remaining.as_nanos() % 1_000_000 == 0 {
            0
        } else {
            1
        };
        let millis = remaining.as_millis().saturating_add(round_up);
        Ok(i32::try_from(millis).unwrap_or(i32::MAX))
    }
}
