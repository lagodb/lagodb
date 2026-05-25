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

pub use converter::{RecordBatchRowReader, RowRecordBatchBuilder};
