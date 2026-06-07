//! # pg-arrow-conv
//!
//! Format-neutral Arrow⇆PostgreSQL value conversion. Dispatches on the pair
//! `(arrow_schema::DataType, PgColumnType)` and depends only on `arrow`,
//! `pgrx`, and `pg-lakebase-core` — never on a table-format crate.
//!
//! - [`resolve_column_rule`] picks a [`ColumnRule`] for a column once.
//! - [`ColumnRule::extract`] reads an Arrow value into a `Cell`;
//!   [`ColumnRule::build`] writes buffered rows back into an Arrow array.
//! - [`validate_supported`] rejects unmaterializable column shapes up front.
//! - [`pg_epoch_days_to_unix_days`] / [`pg_epoch_micros_to_unix_micros`] expose
//!   the shared PG↔Unix epoch offsets for the consumer's predicate translator.

mod buffer;
mod convert;
mod error;
mod read;
mod rule;
mod types;

pub use buffer::SlotRecordBatchBuffer;
pub use error::{ConvError, ConvResult};
pub use read::{ArrowBatchSource, ArrowColumnDecoder, BoundBatch, DecodedColumn};
pub use rule::{
    ColumnRule, ListElementRule, PgColumnType, resolve_column_rule,
    resolve_list_element_rule, validate_supported,
};
pub use types::{
    ArrowColumnEncoder, pg_epoch_days_to_unix_days, pg_epoch_micros_to_unix_micros,
};
