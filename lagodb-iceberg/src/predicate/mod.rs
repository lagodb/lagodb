//! Iceberg predicate domain shared by relation scans, runtime pruning, and
//! write-conflict filtering.

mod binding;
mod error;
mod ir;
mod planning;
mod runtime;
mod semantics;

pub(crate) use binding::IcebergBoundPredicate;
pub(crate) use error::IcebergFilterError;
pub(crate) use ir::IcebergPlannedPredicate;
pub(crate) use planning::{IcebergFilterPlanner, PredicateStorageContract};
pub(crate) use runtime::IcebergRuntimePlanner;
