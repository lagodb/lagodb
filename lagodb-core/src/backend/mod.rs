//! PostgreSQL backend-process runtime primitives: process identity and exit,
//! latch waits, `SIGURG` wakeups, wait events, VFD I/O, temp files and memory
//! contexts. Each type is a thin owner or handle over one backend-local
//! facility.

mod injection_point;
mod latch;
mod memory_context;
mod process;
mod signal_wakeup;
mod temp_file;
mod vfd;
mod wait_event;

pub use injection_point::InjectionPoint;
pub use latch::{BackendLatch, TeardownLatchWait};
pub(crate) use memory_context::MemoryContextHandle;
pub use memory_context::OwnedBackendContext;
pub use process::BackendProcess;
pub use signal_wakeup::{SignalWakeup, SignalWakeupError};
pub use temp_file::PgTempFile;
pub use vfd::VfdHandle;
pub use wait_event::ExtensionWaitEvent;
