//! PostgreSQL latch integration for storage-service socket readiness waits.

use std::ffi::{c_int, c_long};
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, RawFd};
use std::ptr::{self, NonNull};
use std::time::Instant;

use lagodb_storage::{SocketInterest, SocketWait, SocketWaitContext};
use pgrx::pg_sys;

use crate::backend::{BackendLatch, BackendProcess};

pub(super) struct PostgresSocketWait {
    // Backend-local PostgreSQL state: this waiter must be used and dropped on the
    // backend thread that created its event set.
    event_set: Option<PgWaitEventSet>,
}

impl SocketWait for PostgresSocketWait {
    fn wait(
        &mut self,
        socket: BorrowedFd<'_>,
        interest: SocketInterest,
        context: SocketWaitContext,
        deadline: Option<Instant>,
    ) -> io::Result<()> {
        let socket_fd = socket.as_raw_fd();
        if self.event_set.is_none() {
            self.event_set = Some(PgWaitEventSet::new(socket_fd, interest)?);
        }
        let event_set = self
            .event_set
            .as_mut()
            .expect("event set was initialized above");
        event_set.configure(interest, context)?;
        let mut dropped_latch = BackendLatch::preserve_dropped_latch();

        loop {
            let timeout = Self::timeout_millis(deadline)?;
            let Some(occurred) = event_set.wait(timeout) else {
                if deadline.is_some_and(|deadline| Instant::now() < deadline) {
                    continue;
                }
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "storage socket wait timed out",
                ));
            };

            if occurred & pg_sys::WL_LATCH_SET != 0 {
                let latch = event_set.latch().ok_or_else(|| {
                    io::Error::other(
                        "PostgreSQL socket wait reported a disabled latch",
                    )
                })?;
                // SAFETY: the event set only reports a latch registered to the
                // current backend, and the wait has already stopped sleeping.
                unsafe {
                    pg_sys::ResetLatch(latch.as_ptr());
                }
                let interrupts_held = BackendProcess::interrupts_held();
                if interrupts_held {
                    // The holder services this wakeup once its hold ends, so
                    // PostgreSQL's dropped-latch protocol sets the latch again on
                    // every exit path.
                    dropped_latch.mark_dropped();
                }
                // A hold comes from the parallel runtime polling a future, or
                // from PostgreSQL running transaction-end callbacks: commit,
                // abort and subtransaction abort hold interrupts first. Under
                // either, a cancel or termination ends the request with an error
                // so ClientIo poisons the partial exchange, and the interrupt
                // stays pending for the holder. Without a hold, keep PostgreSQL's
                // normal ERROR path.
                //
                // Every other reason stays pending until this request completes:
                // transaction timeout and recovery conflicts (PostgreSQL 17+), a
                // lost-client check, and benign reasons such as barriers or
                // parallel messages. Foreground requests have no deadline, so a
                // stalled storage service stretches that delay until a cancel or
                // termination arrives. The remedy depends on the holder: the
                // runtime must run this wait outside its held poll, while
                // transaction-end I/O needs a bounded cleanup deadline, as
                // postgres_fdw gives its abort cleanup.
                if interrupts_held && BackendProcess::query_interrupt_pending() {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "storage socket wait deferred a PostgreSQL interrupt",
                    ));
                }
                // A canceling interrupt unwinds through ClientIo::Drop and poisons
                // the connection; a benign one returns and leaves it reusable.
                pg_sys::check_for_interrupts!();
            }
            if occurred & event_set.socket_event() != 0 {
                return Ok(());
            }
        }
    }
}

impl PostgresSocketWait {
    pub(super) const fn new() -> Self {
        Self { event_set: None }
    }

    fn timeout_millis(deadline: Option<Instant>) -> io::Result<c_long> {
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
        // WaitEventSetWait asserts that a finite timeout fits in an int.
        let millis = millis.min(c_int::MAX as u128);
        Ok(millis as c_long)
    }
}

struct PgWaitEventSet {
    raw: NonNull<pg_sys::WaitEventSet>,
    socket_pos: c_int,
    latch_pos: c_int,
    socket_event: u32,
    latch: *mut pg_sys::Latch,
}

impl PgWaitEventSet {
    fn new(socket_fd: RawFd, interest: SocketInterest) -> io::Result<Self> {
        let latch = Self::current_latch()?;
        // A NULL ResourceOwner, or TopMemoryContext where PostgreSQL 15 and 16
        // take a context instead, gives the set session lifetime.
        #[cfg(any(feature = "pg15", feature = "pg16"))]
        let owner = unsafe { pg_sys::TopMemoryContext };
        #[cfg(any(feature = "pg17", feature = "pg18"))]
        let owner = ptr::null_mut();
        // SAFETY: PostgreSQL owns the allocation and returns either a valid
        // opaque pointer or raises ERROR through pgrx's guarded FFI boundary.
        let raw = unsafe { pg_sys::CreateWaitEventSet(owner, 3) };
        let raw = NonNull::new(raw).ok_or_else(|| {
            io::Error::other("PostgreSQL failed to create a socket wait event set")
        })?;
        let socket_event = Self::socket_event_for(interest);
        let mut event_set = Self {
            raw,
            socket_pos: 0,
            latch_pos: 0,
            socket_event,
            latch,
        };

        // PgWaitEventSet owns `raw` from here, so an ERROR from any registration
        // unwinds through its Drop. Capacity three covers the backend latch,
        // postmaster death and this generation's socket.
        // SAFETY: socket_fd stays open for the generation, and every pointer is
        // valid for the call.
        unsafe {
            event_set.latch_pos = pg_sys::AddWaitEventToSet(
                event_set.raw.as_ptr(),
                pg_sys::WL_LATCH_SET,
                pg_sys::PGINVALID_SOCKET,
                latch,
                ptr::null_mut(),
            );
            if pg_sys::IsUnderPostmaster {
                pg_sys::AddWaitEventToSet(
                    event_set.raw.as_ptr(),
                    pg_sys::WL_EXIT_ON_PM_DEATH,
                    pg_sys::PGINVALID_SOCKET,
                    ptr::null_mut(),
                    ptr::null_mut(),
                );
            }
            event_set.socket_pos = pg_sys::AddWaitEventToSet(
                event_set.raw.as_ptr(),
                socket_event,
                socket_fd,
                ptr::null_mut(),
                ptr::null_mut(),
            );
        }

        Ok(event_set)
    }

    fn configure(
        &mut self,
        interest: SocketInterest,
        context: SocketWaitContext,
    ) -> io::Result<()> {
        let socket_event = Self::socket_event_for(interest);
        if socket_event != self.socket_event {
            // SAFETY: socket_pos identifies the socket registered by new(), and
            // the descriptor stays open for this connection generation.
            unsafe {
                pg_sys::ModifyWaitEvent(
                    self.raw.as_ptr(),
                    self.socket_pos,
                    socket_event,
                    ptr::null_mut(),
                );
            }
            self.socket_event = socket_event;
        }

        let latch = match context {
            SocketWaitContext::Foreground => Self::current_latch()?,
            SocketWaitContext::Cleanup => ptr::null_mut(),
        };
        if latch != self.latch {
            // Passing NULL disables latch delivery during cleanup without
            // changing the underlying reusable kernel wait object.
            // SAFETY: latch_pos identifies the event registered by new(), and a
            // non-NULL latch belongs to the current backend.
            unsafe {
                pg_sys::ModifyWaitEvent(
                    self.raw.as_ptr(),
                    self.latch_pos,
                    pg_sys::WL_LATCH_SET,
                    latch,
                );
            }
            self.latch = latch;
        }

        Ok(())
    }

    fn wait(&mut self, timeout: c_long) -> Option<u32> {
        let mut occurred = pg_sys::WaitEvent::default();
        // SAFETY: raw owns a live event set, occurred has space for one event,
        // and timeout is either -1 or a nonnegative value capped to INT_MAX.
        let count = unsafe {
            pg_sys::WaitEventSetWait(
                self.raw.as_ptr(),
                timeout,
                &mut occurred,
                1,
                pg_sys::PG_WAIT_EXTENSION,
            )
        };
        (count != 0).then_some(occurred.events)
    }

    fn latch(&self) -> Option<NonNull<pg_sys::Latch>> {
        NonNull::new(self.latch)
    }

    const fn socket_event(&self) -> u32 {
        self.socket_event
    }

    const fn socket_event_for(interest: SocketInterest) -> u32 {
        match interest {
            SocketInterest::Readable => pg_sys::WL_SOCKET_READABLE,
            SocketInterest::Writable => pg_sys::WL_SOCKET_WRITEABLE,
        }
    }

    fn current_latch() -> io::Result<*mut pg_sys::Latch> {
        // SAFETY: MyLatch is backend-local state initialized by PostgreSQL.
        let latch = unsafe { pg_sys::MyLatch };
        if latch.is_null() {
            Err(io::Error::other(
                "PostgreSQL backend latch is not initialized",
            ))
        } else {
            Ok(latch)
        }
    }
}

impl Drop for PgWaitEventSet {
    fn drop(&mut self) {
        // SAFETY: raw was returned by CreateWaitEventSet and this RAII object
        // owns the only call to FreeWaitEventSet for it.
        unsafe {
            pg_sys::FreeWaitEventSet(self.raw.as_ptr());
        }
    }
}
