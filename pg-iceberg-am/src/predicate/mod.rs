//! Iceberg predicate domain shared by scans and write-conflict filtering.

mod classifier;
pub(crate) mod policy;
pub(crate) mod translator;

pub use classifier::IcebergPredicateClassifier;
pub use translator::IcebergPredicateTranslator;
