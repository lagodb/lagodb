//! Bridges `tracing` events from multi-threaded Tokio workers into PostgreSQL's
//! standard log system via a bounded channel.
//!
//! # Design
//!
//! Tokio/storage threads format events into [`LogEvent`] structs and enqueue them
//! via a bounded `SyncSender`.  The bgworker main thread (the only Postgres-facing
//! thread) drains the channel and emits each event through the low-level PostgreSQL
//! ereport API ([`emit_pg_log`]).
//!
//! Reference: Neon communicator logging bridge
//! <https://github.com/neondatabase/neon/blob/main/pgxn/neon/communicator/src/worker_process/logging.rs>

use std::ffi::CString;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};

use pgrx::pg_sys;
use tracing::level_filters::LevelFilter;
use tracing::{Level, Metadata};
use tracing_subscriber::fmt::MakeWriter;

// ---------------------------------------------------------------------------
// LogEvent
// ---------------------------------------------------------------------------

struct LogEvent {
    elevel: i32,
    message: String,
}

// ---------------------------------------------------------------------------
// PgLogBridge: owned by the bgworker main thread
// ---------------------------------------------------------------------------

pub(super) struct PgLogBridge {
    receiver: mpsc::Receiver<LogEvent>,
    dropped: Arc<AtomicU64>,
    last_reported_dropped: u64,
}

// ---------------------------------------------------------------------------
// PgLogWriter: installed as tracing_subscriber writer
// ---------------------------------------------------------------------------

pub(super) struct PgLogWriter {
    sender: mpsc::SyncSender<LogEvent>,
    dropped: Arc<AtomicU64>,
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

pub(super) fn new_bridge(capacity: usize) -> (PgLogBridge, PgLogWriter) {
    let (sender, receiver) = mpsc::sync_channel(capacity);
    let dropped = Arc::new(AtomicU64::new(0));

    (
        PgLogBridge {
            receiver,
            dropped: dropped.clone(),
            last_reported_dropped: 0,
        },
        PgLogWriter { sender, dropped },
    )
}

// ---------------------------------------------------------------------------
// PgLogBridge: drain channel to PG log
// ---------------------------------------------------------------------------

impl PgLogBridge {
    /// Drain all pending log events into PostgreSQL's log system.
    ///
    /// Must only be called from the bgworker main thread.
    pub fn drain_to_pg_log(&mut self) {
        while let Ok(event) = self.receiver.try_recv() {
            emit_pg_log(event.elevel, &event.message);
        }

        let total = self.dropped.load(Ordering::Relaxed);
        if total > self.last_reported_dropped {
            let delta = total - self.last_reported_dropped;
            self.last_reported_dropped = total;
            emit_pg_log(
                pg_sys::WARNING as i32,
                &format!("{delta} log messages dropped (channel full)"),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// emit_pg_log: low-level PG ereport without errfinish
// ---------------------------------------------------------------------------

/// Emit a single log line into PostgreSQL's standard log at the given severity.
///
/// # Safety contract (Neon communicator pattern)
///
/// We intentionally skip `errfinish()`.  In PostgreSQL's error machinery:
///
/// - `errstart_cold` pushes an ErrorData entry and returns true if the level is
///   interesting.  It does **not** longjmp.
/// - `errmsg_internal` populates the message text.
/// - `EmitErrorReport` sends the formatted report to all log destinations.
/// - `FlushErrorState` pops and frees the ErrorData entry.
///
/// `errfinish` is what triggers `longjmp` (for ERROR) or `proc_exit` (for
/// FATAL/PANIC).  By omitting it we emit the log line, including at ERROR
/// severity, without disrupting bgworker control flow.
///
/// `InterruptHoldoffCount` is incremented around the sequence to prevent a
/// concurrent `CHECK_FOR_INTERRUPTS` from firing while the error stack is open.
/// If `EmitErrorReport` itself raises a nested error (e.g., log-destination I/O
/// failure), PostgreSQL's error recursion will call `proc_exit`, which is
/// acceptable: the logging subsystem is broken and the bgworker should exit.
pub(super) fn emit_pg_log(elevel: i32, message: &str) {
    unsafe {
        if !pg_sys::message_level_is_interesting(elevel) {
            return;
        }

        let message = message.replace('\0', "\\0");
        let Ok(c_message) = CString::new(format!("[pg-lakebase-storage] {message}"))
        else {
            return;
        };

        let _hold = InterruptHoldoffGuard::enter();

        if pg_sys::errstart_cold(elevel, std::ptr::null()) {
            pg_sys::errmsg_internal(c"%s".as_ptr(), c_message.as_ptr());
            pg_sys::EmitErrorReport();
            pg_sys::FlushErrorState();
        }
    }
}

// ---------------------------------------------------------------------------
// InterruptHoldoffGuard
// ---------------------------------------------------------------------------

struct InterruptHoldoffGuard;

impl InterruptHoldoffGuard {
    /// Increment `InterruptHoldoffCount`, equivalent to C `HOLD_INTERRUPTS()`.
    unsafe fn enter() -> Self {
        unsafe { pg_sys::InterruptHoldoffCount += 1 };
        Self
    }
}

impl Drop for InterruptHoldoffGuard {
    fn drop(&mut self) {
        unsafe { pg_sys::InterruptHoldoffCount -= 1 };
    }
}

// ---------------------------------------------------------------------------
// tracing level to PG elevel mapping
// ---------------------------------------------------------------------------

fn tracing_level_to_pg(level: &Level) -> i32 {
    match *level {
        Level::ERROR => pg_sys::PGERROR as i32,
        Level::WARN => pg_sys::WARNING as i32,
        Level::INFO => pg_sys::INFO as i32,
        Level::DEBUG => pg_sys::DEBUG1 as i32,
        Level::TRACE => pg_sys::DEBUG5 as i32,
    }
}

// ---------------------------------------------------------------------------
// MakeWriter adapter: tracing_subscriber to bounded channel
// ---------------------------------------------------------------------------

/// Per-event write buffer.  When dropped, the accumulated bytes are sent as a
/// [`LogEvent`] to the bounded channel.
pub(super) struct EventBuffer<'a> {
    writer: &'a PgLogWriter,
    elevel: i32,
    bytes: Vec<u8>,
}

impl std::io::Write for EventBuffer<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for EventBuffer<'_> {
    fn drop(&mut self) {
        let message = String::from_utf8_lossy(&self.bytes).trim().to_string();
        if message.is_empty() {
            return;
        }

        let event = LogEvent {
            elevel: self.elevel,
            message,
        };

        if self.writer.sender.try_send(event).is_err() {
            self.writer.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl<'a> MakeWriter<'a> for PgLogWriter {
    type Writer = EventBuffer<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        EventBuffer {
            writer: self,
            elevel: pg_sys::LOG as i32,
            bytes: Vec::new(),
        }
    }

    fn make_writer_for(&'a self, meta: &Metadata<'_>) -> Self::Writer {
        EventBuffer {
            writer: self,
            elevel: tracing_level_to_pg(meta.level()),
            bytes: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Public helpers for the supervisor
// ---------------------------------------------------------------------------

/// The recommended tracing level filter for the storage worker subscriber.
pub(super) fn tracing_level_filter() -> LevelFilter {
    LevelFilter::INFO
}

/// Install the tracing subscriber backed by the PG log bridge.
///
/// Uses `try_init` to tolerate a global subscriber inherited from the
/// postmaster process after `fork()`.  Returns `true` if the bridge
/// subscriber was successfully installed.
pub(super) fn install_tracing_subscriber(writer: PgLogWriter) -> bool {
    use tracing_subscriber::prelude::*;

    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .without_time()
            .with_writer(writer)
            .with_filter(tracing_level_filter()),
    );

    subscriber.try_init().is_ok()
}
