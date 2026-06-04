//! Arrow `RecordBatch` <=> PostgreSQL `Row` conversion.
//!
//! Two converter objects bind the per-column dispatch to a specific
//! Iceberg schema:
//!
//! - [`RecordBatchRowReader`] — Arrow -> Row (scan path).
//! - [`RowRecordBatchBuilder`] — Row -> Arrow (DML path).
//!
//! Both build on the per-`Type` [`traits::ArrowToCell`] /
//! [`traits::RowsToArrow`] dispatch implementations. The converters are the
//! point at which those traits stop being free dispatch and become bound to
//! a known column layout.

mod complex;
mod converter;
mod primitive;
mod schema;
mod traits;

#[cfg(test)]
mod tests;

#[cfg(feature = "pg_test")]
mod pg_test;

pub use converter::{LiveColumn, RecordBatchRowReader, RowRecordBatchBuilder};

/// Shared PG→Unix epoch offset helpers, re-exported so the runtime predicate
/// translator (`customscan::predicate_translator::IcebergDatumDecoder`) applies the *same* offset to pushed
/// `date` / `timestamp` / `timestamptz` bounds that the storage write side
/// applies to stored values — keeping pushed predicate bounds aligned with
/// stored manifest bounds (Requirement 3.5). One conversion, one source of
/// truth.
pub(crate) use primitive::{
    pg_epoch_days_to_unix_days, pg_epoch_micros_to_unix_micros,
};
