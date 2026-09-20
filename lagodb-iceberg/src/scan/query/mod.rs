//! Statement-bound and run-local Iceberg scan data plane.

mod lifecycle;
mod runtime_predicate;
mod stream;

pub(crate) use lifecycle::{BoundScan, PlannedScan};
pub(crate) use stream::ArrowStream;
