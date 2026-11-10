//! PostgreSQL backend-process runtime primitives.
//!
//! Process identity, interrupts and exit (`process`), teardown latch waits
//! (`latch`), extension wait events, injection points, VFD I/O, temporary
//! files, and memory contexts.
//! Each type is a thin owner or handle over one backend-local PostgreSQL
//! facility.

mod injection_point;
mod latch;
mod memory_context;
mod process;
mod temp_file;
mod vfd;
mod wait_event;

pub use injection_point::InjectionPoint;
pub use latch::{BackendLatch, TeardownLatchWait};
pub(crate) use memory_context::MemoryContextHandle;
pub use memory_context::OwnedBackendContext;
pub use process::BackendProcess;
pub use temp_file::PgTempFile;
pub use vfd::VfdHandle;
pub use wait_event::ExtensionWaitEvent;
