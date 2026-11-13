//! Cross-process wakeups for a runtime that parks outside PostgreSQL.
//!
//! A current-thread runtime parks in `epoll_wait`/`kevent`, where PostgreSQL's
//! `SetLatch` signal cannot reach it, so this module owns a second consumer of
//! `SIGURG` plus trampolines around the handlers a parked runtime must observe.

use std::ffi::c_int;
use std::io;
use std::mem;
use std::os::fd::{AsFd, AsRawFd, RawFd};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};

use nix::errno::Errno;
#[cfg(target_os = "macos")]
use nix::sys::event::{EventFilter, EventFlag, FilterFlag, KEvent, Kqueue};
#[cfg(target_os = "linux")]
use nix::sys::signal::{SigSet, Signal};
#[cfg(target_os = "linux")]
use nix::sys::signalfd::{SfdFlags, SignalFd};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::diag::SqlStateError;

/// Signals whose PostgreSQL dispositions can set `MyLatch` while a runtime is
/// parked outside PostgreSQL's wait machinery.
const FORWARDED_SIGNALS: [c_int; 5] = [
    pg_sys::SIGHUP as c_int,
    pg_sys::SIGINT as c_int,
    pg_sys::SIGTERM as c_int,
    pg_sys::SIGALRM as c_int,
    pg_sys::SIGUSR1 as c_int,
];

/// Set while a parked runtime must be woken by the trampoline's `SIGURG`.
static ARMED: AtomicBool = AtomicBool::new(false);

/// PostgreSQL's handler for the signal at the same index in
/// [`FORWARDED_SIGNALS`].
static ORIGINALS: [AtomicUsize; FORWARDED_SIGNALS.len()] =
    [const { AtomicUsize::new(0) }; FORWARDED_SIGNALS.len()];

/// The process-wide listener, created by the first parallel run.
static PROCESS: Mutex<Option<&'static SignalWakeup>> = Mutex::new(None);

/// This backend's `SIGURG` listener and the trampolines around the PostgreSQL
/// handlers a parked runtime must observe. Backend main thread only.
pub struct SignalWakeup {
    listener: Listener,
}

impl SignalWakeup {
    /// Returns this process's listener, creating it and installing the
    /// trampolines on first use.
    pub fn acquire() -> Result<&'static Self, SignalWakeupError> {
        let mut process = PROCESS.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(wakeup) = *process {
            return Ok(wakeup);
        }
        // Nothing under this lock raises a PostgreSQL error, so the singleton is
        // either published completely or not at all.
        Self::install_trampolines()?;
        let wakeup: &'static Self = Box::leak(Box::new(Self {
            listener: Listener::create()?,
        }));
        *process = Some(wakeup);
        Ok(wakeup)
    }

    /// The listener this process already acquired; cleanup paths must not install
    /// anything while unwinding.
    pub fn installed() -> Option<&'static Self> {
        *PROCESS.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The descriptor that becomes readable when `SIGURG` is delivered.
    pub fn fd(&self) -> RawFd {
        self.listener.fd()
    }

    /// Discards every pending `SIGURG` and starts following forwarded signals
    /// with a self-directed one: a standard signal that is still pending produces
    /// no second edge.
    pub fn begin_park(&self) {
        self.listener.drain();
        ARMED.store(true, Ordering::Release);
    }

    /// Stops following forwarded signals with a `SIGURG` edge; the next
    /// [`Self::begin_park`] drains what it leaves.
    pub fn end_park(&self) {
        ARMED.store(false, Ordering::Release);
    }

    /// Confirms that the trampolines are still installed. A parallel worker's
    /// SIGHUP, which PostgreSQL leaves at `SIG_IGN` and the install preserved,
    /// must still be `SIG_IGN`.
    ///
    /// Rewrapping a replaced handler would build a
    /// `trampoline -> handler -> trampoline` chain, so a replaced handler only
    /// makes parallel execution unavailable for the run that observed it.
    pub fn verify(&self) -> Result<(), SignalWakeupError> {
        for (index, signal) in FORWARDED_SIGNALS.into_iter().enumerate() {
            let expected = ORIGINALS[index].load(Ordering::Acquire);
            let installed = Self::action(signal)?.sa_sigaction;
            let valid =
                if signal == pg_sys::SIGHUP as c_int && expected == libc::SIG_IGN {
                    installed == libc::SIG_IGN
                } else {
                    installed == Self::trampoline_address()
                };
            if !valid {
                return Err(SignalWakeupError::HandlerReplaced { signal });
            }
        }
        Ok(())
    }

    /// Installs the trampolines, leaving an already-trampolined disposition
    /// untouched.
    ///
    /// A rejection part-way leaves the installed ones in place: they are inert
    /// until [`Self::begin_park`] arms them, and rolling back would rewrite a
    /// disposition this code cannot prove it owns.
    fn install_trampolines() -> Result<(), SignalWakeupError> {
        for (index, signal) in FORWARDED_SIGNALS.into_iter().enumerate() {
            let action = Self::action(signal)?;
            if action.sa_sigaction == Self::trampoline_address() {
                continue;
            }
            // PostgreSQL parallel workers intentionally ignore SIGHUP in every
            // supported version. It cannot set MyLatch there, so preserve and
            // verify that exact disposition instead of rejecting the worker.
            if signal == pg_sys::SIGHUP as c_int
                && action.sa_sigaction == libc::SIG_IGN
                && action.sa_flags & libc::SA_SIGINFO == 0
                // SAFETY: PostgreSQL initializes this backend-local identity
                // before extension code can build a parallel runtime.
                && unsafe { pg_sys::ParallelWorkerNumber >= 0 }
            {
                ORIGINALS[index].store(libc::SIG_IGN, Ordering::Release);
                continue;
            }
            // PostgreSQL installs a plain `sa_handler`; a `SIG_DFL`/`SIG_IGN`
            // disposition or an `SA_SIGINFO` handler belongs to someone else, and
            // wrapping it would discard that owner or recurse.
            if action.sa_flags & libc::SA_SIGINFO != 0
                || action.sa_sigaction == libc::SIG_DFL
                || action.sa_sigaction == libc::SIG_IGN
            {
                return Err(SignalWakeupError::HandlerUnsupported { signal });
            }
            // Publish the predecessor before the trampoline can run.
            ORIGINALS[index].store(action.sa_sigaction, Ordering::Release);
            let mut replacement = action;
            replacement.sa_sigaction = Self::trampoline_address();
            // SAFETY: `replacement` describes a disposition for `signal` and
            // keeps the flags and mask PostgreSQL installed.
            let installed =
                unsafe { libc::sigaction(signal, &replacement, ptr::null_mut()) };
            if installed < 0 {
                return Err(SignalWakeupError::os(
                    "sigaction",
                    io::Error::last_os_error(),
                ));
            }
        }
        Ok(())
    }

    /// Reads the current disposition of `signal`.
    fn action(signal: c_int) -> Result<libc::sigaction, SignalWakeupError> {
        let mut action: libc::sigaction = unsafe { mem::zeroed() };
        // SAFETY: both pointers are valid for the call; reading a disposition
        // installs nothing.
        let read = unsafe { libc::sigaction(signal, ptr::null(), &mut action) };
        if read < 0 {
            return Err(SignalWakeupError::os(
                "sigaction",
                io::Error::last_os_error(),
            ));
        }
        Ok(action)
    }

    /// The address a `sigaction` holds for the trampoline.
    fn trampoline_address() -> libc::sighandler_t {
        let handler: unsafe extern "C-unwind" fn(c_int) = Self::trampoline;
        handler as libc::sighandler_t
    }

    /// The predecessor slot for `signal`, parallel to [`ORIGINALS`].
    fn original_slot(signal: c_int) -> Option<&'static AtomicUsize> {
        let index = FORWARDED_SIGNALS
            .iter()
            .position(|forwarded| *forwarded == signal)?;
        ORIGINALS.get(index)
    }

    /// Forwards to PostgreSQL's handler and, while a runtime is parked, makes
    /// the listener readable again.
    ///
    /// Must stay async-signal-safe: `errno` is saved because `kill` may change
    /// it, and PostgreSQL's handler expects it intact.
    unsafe extern "C-unwind" fn trampoline(signal: c_int) {
        // SAFETY: installed through `sigaction` as a plain `sa_handler`, and it
        // only calls async-signal-safe functions.
        unsafe {
            let saved_errno = Errno::last_raw();
            // `SIG_DFL` loads as zero: a disposition that was never wrapped has
            // no PostgreSQL handler to forward to.
            let original = Self::original_slot(signal)
                .map(|slot| slot.load(Ordering::Acquire))
                .unwrap_or(0);
            let handler: pg_sys::pqsigfunc = mem::transmute(original);
            if let Some(handler) = handler {
                handler(signal);
            }
            if ARMED.load(Ordering::Acquire) {
                libc::kill(libc::getpid(), libc::SIGURG);
            }
            Errno::set_raw(saved_errno);
        }
    }
}

#[cfg(target_os = "linux")]
type PlatformListener = SignalFd;

#[cfg(target_os = "macos")]
type PlatformListener = Kqueue;

/// The process-wide descriptor that observes `SIGURG` deliveries.
struct Listener {
    // Fields drop in declaration order: close the descriptor before returning
    // its slot to PostgreSQL's external-FD budget.
    source: PlatformListener,
    _slot: ExternalFdSlot,
}

impl Listener {
    fn fd(&self) -> RawFd {
        self.source.as_fd().as_raw_fd()
    }
}

/// One descriptor accounted against PostgreSQL's backend-local FD budget.
struct ExternalFdSlot;

impl ExternalFdSlot {
    fn acquire() -> Result<Self, SignalWakeupError> {
        // SAFETY: created by the backend main thread; fd.c accounting is
        // backend-local.
        if unsafe { pg_sys::AcquireExternalFD() } {
            Ok(Self)
        } else {
            Err(SignalWakeupError::ExternalFdExhausted)
        }
    }
}

impl Drop for ExternalFdSlot {
    fn drop(&mut self) {
        // SAFETY: every instance follows one successful acquisition.
        unsafe { pg_sys::ReleaseExternalFD() };
    }
}

#[cfg(target_os = "linux")]
impl Listener {
    /// PostgreSQL keeps `SIGURG` blocked in every backend, so a second
    /// `signalfd` observes the same deliveries.
    fn create() -> Result<Self, SignalWakeupError> {
        let slot = ExternalFdSlot::acquire()?;
        let mut mask = SigSet::empty();
        mask.add(Signal::SIGURG);
        let source = SignalFd::with_flags(
            &mask,
            SfdFlags::SFD_NONBLOCK | SfdFlags::SFD_CLOEXEC,
        )
        .map_err(|error| SignalWakeupError::os("signalfd", error.into()))?;
        Ok(Self {
            source,
            _slot: slot,
        })
    }

    /// Reads records until the descriptor is empty, as PostgreSQL's own
    /// `drain()` does.
    fn drain(&self) {
        loop {
            match self.source.read_signal() {
                Ok(Some(_)) | Err(Errno::EINTR) => continue,
                // A park hook must not raise a PostgreSQL error, so a failed
                // drain leaves the edge for the next park.
                Ok(None) | Err(_) => return,
            }
        }
    }
}

#[cfg(target_os = "macos")]
impl Listener {
    /// PostgreSQL sets `SIGURG` to `SIG_IGN` and observes it through
    /// `EVFILT_SIGNAL`. `kqueue(2)` records every delivery attempt before normal
    /// signal processing, so a second filter sees the same deliveries.
    fn create() -> Result<Self, SignalWakeupError> {
        let slot = ExternalFdSlot::acquire()?;
        let source = Kqueue::new()
            .map_err(|error| SignalWakeupError::os("kqueue", error.into()))?;
        // `kqueue(2)`: the filter sets `EV_CLEAR` automatically, as PostgreSQL
        // relies on.
        let change = KEvent::new(
            libc::SIGURG as libc::uintptr_t,
            EventFilter::EVFILT_SIGNAL,
            EventFlag::EV_ADD,
            FilterFlag::empty(),
            0,
            0,
        );
        source
            .kevent(&[change], &mut [], None)
            .map_err(|error| SignalWakeupError::os("kevent", error.into()))?;
        Ok(Self {
            source,
            _slot: slot,
        })
    }

    /// Consumes the pending delivery count; `kqueue(2)` reports how many times
    /// the signal fired since the last call, so one zero-timeout call suffices.
    fn drain(&self) {
        let timeout = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let mut event = [KEvent::new(
            0,
            EventFilter::EVFILT_SIGNAL,
            EventFlag::empty(),
            FilterFlag::empty(),
            0,
            0,
        )];
        let _ = self.source.kevent(&[], &mut event, Some(timeout));
    }
}

/// Why a [`SignalWakeup`] could not be created or verified.
#[derive(Debug, thiserror::Error)]
pub enum SignalWakeupError {
    #[error("PostgreSQL external file descriptor budget is exhausted")]
    ExternalFdExhausted,
    #[error("failed to {operation}: {source}")]
    Os {
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    #[error(
        "signal {signal} has no single-argument handler that a trampoline can wrap"
    )]
    HandlerUnsupported { signal: c_int },
    #[error(
        "signal {signal} handler was replaced after the trampoline was installed"
    )]
    HandlerReplaced { signal: c_int },
}

impl SignalWakeupError {
    fn os(operation: &'static str, source: io::Error) -> Self {
        Self::Os { operation, source }
    }
}

impl SqlStateError for SignalWakeupError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::ExternalFdExhausted => {
                PgSqlErrorCode::ERRCODE_INSUFFICIENT_RESOURCES
            }
            Self::Os { .. } => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            Self::HandlerUnsupported { .. } | Self::HandlerReplaced { .. } => {
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
            }
        }
    }
}
