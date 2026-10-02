//! Statement-bound and run-local Iceberg columnar scan data plane.

mod lifecycle;
mod runtime_predicate;
mod source;
mod stream;

pub(crate) use lifecycle::{BoundScan, PlannedScan};
pub(crate) use source::{ScanSourceBinding, ScanTaskPlanner};
pub(crate) use stream::ArrowStream;
