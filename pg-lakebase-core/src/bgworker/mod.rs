//! PostgreSQL bgworker/backend process primitives.

mod latch;

pub use latch::{BackendLatch, TeardownLatchWait};
