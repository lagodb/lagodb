//! PostgreSQL backend-process runtime primitives.
//!
//! Process identity, interrupts and exit (`process`), teardown latch waits
//! (`latch`), extension wait events, injection points, and temporary files.
//! Each type is a thin owner over one backend-local PostgreSQL facility.

mod injection_point;
mod latch;
mod process;
mod temp_file;
mod wait_event;

pub use injection_point::InjectionPoint;
pub use latch::{BackendLatch, TeardownLatchWait};
pub use process::BackendProcess;
pub use temp_file::PgTempFile;
pub use wait_event::ExtensionWaitEvent;
