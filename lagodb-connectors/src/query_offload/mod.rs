//! Query-offload provider for Parquet foreign tables.
//!
//! A bound scan exposes the relation's declared columns as one canonical
//! Arrow schema (`ParquetColumnarPlan`), lists its object location once, and
//! reads it through the Parquet batch source and predicate backend shared
//! with foreign scans. Parallel workers rebind the leader's inventory without
//! listing storage again.

mod columns;
mod error;
mod plan;
mod provider;
mod scan;
mod stream;
mod worker;

pub(crate) use provider::register;

/// A value confined to the PostgreSQL backend process that runs the scan.
struct BackendLocal<T>(T);

// SAFETY: the table-scan SPI requires `Send + Sync` only for the query
// engine's plan traits; the engine creates, polls and drops bound scans, task
// plans, worker sources and streams on the owning backend's thread alone.
// The wrapped connector storage handles and read-progress counters use `Rc`
// and `Cell` and are never reached from another thread.
unsafe impl<T> Send for BackendLocal<T> {}
unsafe impl<T> Sync for BackendLocal<T> {}

impl<T> BackendLocal<T> {
    const fn new(value: T) -> Self {
        Self(value)
    }

    const fn get(&self) -> &T {
        &self.0
    }

    fn get_mut(&mut self) -> &mut T {
        &mut self.0
    }
}
