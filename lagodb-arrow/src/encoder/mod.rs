//! Arrow column builders for PostgreSQL datums, organized by type family.
//!
//! Each submodule owns one logical column type's builder and the value math
//! that appends a present datum to it. Source binding and per-variant dispatch
//! live in `datum_codec::DatumEncoder`; the read direction lives in
//! `datum_codec::BoundDatumReader`.

use arrow_array::ArrayRef;

use crate::error::ArrowConversionResult;

pub(crate) mod binary;
pub(crate) mod decimal;
pub(crate) mod primitive;
pub(crate) mod string;
pub(crate) mod timestamp;

/// The builder lifecycle shared by every scalar Arrow column encoder. Present
/// values are appended through each encoder's source-specific method.
pub(crate) trait ColumnBuilder {
    /// Append a SQL NULL.
    fn append_null(&mut self);

    /// Finish the current column and reset the builder for reuse.
    fn finish(&mut self) -> ArrowConversionResult<ArrayRef>;

    /// Number of values appended for the current batch.
    fn len(&self) -> usize;
}
