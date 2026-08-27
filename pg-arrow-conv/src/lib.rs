//! # pg-arrow-conv
//!
//! Format-neutral Arrow⇆PostgreSQL value conversion. Dispatches on the pair
//! `(arrow_schema::DataType, PgColumnType)` and depends only on `arrow`,
//! `pgrx`, and `lagodb-core` — never on a table-format crate.
//!
//! - [`resolve_column_rule`] picks a [`ColumnRule`] for a column once.
//! - [`ColumnReader::bind`] resolves a semantic batch column's concrete Arrow
//!   array once; its row reads are explicitly unchecked after the caller
//!   establishes the batch row bound. Provider-selected physical codecs are
//!   bound by the `ArrowColumnDecoder` plan and never materialized as `Cell` values;
//!   [`ColumnRule::build`] writes buffered rows back into an Arrow array.
//! - [`validate_supported`] rejects unmaterializable column shapes up front.
//! - [`BoundWriteBuffer`] binds source slot codecs and Arrow encoders once for
//!   relation-backed mutation writers. Its row API is explicitly unsafe because
//!   the bound relation layout is a runtime invariant supplied by the provider.
//! - [`BoundDatumBuffer`] provides the same one-time binding for adapters whose
//!   typed datum stream is not backed by a relation slot, such as native COPY.
//! - [`pg_epoch_days_to_unix_days`] / [`pg_epoch_micros_to_unix_micros`] expose
//!   the shared PG↔Unix epoch offsets for the consumer's predicate translator.
//!
//! Semantic UTF-8 rules require a UTF-8 PostgreSQL server encoding; bound
//! readers and writers validate that capability once during construction.

mod convert;
mod datum;
mod error;
mod read;
mod rule;
mod types;
mod write;

pub use datum::DatumCodec;
pub use error::{ArrowConversionError, ArrowConversionResult};
pub use read::{
    ArrowBatchSource, ArrowColumnDecoder, BoundBatch, ColumnReader, DecodedColumn,
};
pub use rule::{
    ColumnRule, ListElementRule, PgColumnType, resolve_column_rule,
    resolve_list_element_rule, validate_supported,
};
pub use types::{
    ArrowColumnEncoder, pg_epoch_days_to_unix_days, pg_epoch_micros_to_unix_micros,
};
pub use write::{
    BoundDatumBuffer, BoundDatumColumnPlan, BoundWriteBuffer, BoundWriteColumnPlan,
};
