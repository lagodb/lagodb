//! # lagodb-arrow
//!
//! PostgreSQL⇆Arrow interoperability shared by LagoDB runtimes and providers.
//! It owns format-neutral value conversion and the typed table-scan adapter
//! over LagoDB's Arrow C Stream ABI, but no table-format or DataFusion logic.
//!
//! - [`BoundDatumPlan::bind`] binds a declared PostgreSQL type and Arrow field once
//!   for Datum conversion. [`resolve_cell_rule`] covers the owned Cell subset.
//! - [`ColumnReader::bind`] resolves a semantic batch column's concrete Arrow
//!   array once; its row reads are explicitly unchecked after the caller
//!   establishes the batch row bound. Provider-selected physical codecs are
//!   bound by the `ArrowColumnDecoder` plan and never materialized as `Cell` values;
//!   [`CellRule::build`] writes buffered rows back into an Arrow array.
//! - [`validate_cell_schema`] rejects shapes the owned Cell model cannot express.
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

mod column_encoder;
mod convert;
mod datum;
mod datum_codec;
mod error;
mod jsonb;
mod read;
mod rule;
mod scalar;
pub mod scan;
mod write;

pub use column_encoder::{
    ArrowColumnEncoder, pg_epoch_days_to_unix_days, pg_epoch_micros_to_unix_micros,
};
pub use datum::DatumCodec;
pub use datum_codec::{
    ArrowColumnDecoder, BoundBatch, BoundColumn, BoundDatumPlan, DecodedColumn,
};
pub use error::{ArrowConversionError, ArrowConversionResult};
pub use jsonb::{NestedJsonbDefault, NestedJsonbDefaults, NestedJsonbPlan};
pub use read::{ArrowBatchSource, ColumnReader};
pub use rule::{
    CellRule, ListElementRule, PgColumnType, resolve_cell_rule,
    resolve_list_element_rule, validate_cell_schema,
};
pub use scalar::{PgDatumArrayBuilder, PgDatumArrayReader};
pub use write::{
    BoundDatumBuffer, BoundDatumColumnPlan, BoundWriteBuffer, BoundWriteColumnPlan,
};
