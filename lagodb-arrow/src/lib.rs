//! # lagodb-arrow
//!
//! PostgreSQL⇆Arrow interoperability shared by LagoDB's storage providers and
//! query runtime. It owns format-neutral datum conversion and the typed
//! table-scan adapter over LagoDB's Arrow C Stream ABI; table formats, file
//! formats and query planning stay in the consuming crates.
//!
//! Every conversion binds once and then runs per row without re-resolving
//! types:
//!
//! - [`BoundDatumPlan::bind`] binds a declared PostgreSQL type to an Arrow
//!   field, recursing through arrays, composites, intervals and nested JSONB.
//!   [`CellRule::resolve`] decides the native leaf `(Arrow, PostgreSQL)` pairs
//!   it reuses.
//! - [`DecodedColumn`] / [`ArrowColumnDecoder`] read Arrow batches into slot
//!   datums. All fallible setup happens when the columns are bound; a batch
//!   only selects concrete arrays. [`DatumCodec`] selects the explicit
//!   physical representations a query engine may emit.
//! - [`BoundWriteBuffer`] appends relation slot rows, and [`BoundDatumBuffer`]
//!   appends slot-free datum streams such as COPY, into Arrow batches.
//! - [`PgDatumArrayReader`] / [`PgDatumArrayBuilder`] bind one Arrow array to
//!   a PostgreSQL expression argument or result type.
//! - [`ArrowBatchSource`] adapts an Arrow batch iterator to the core
//!   access-method scan loop.
//! - [`scan`] is the typed table-scan boundary between table providers and
//!   the query engine.
//! - [`pg_epoch_days_to_unix_days`] / [`pg_epoch_micros_to_unix_micros`] expose
//!   the shared PG↔Unix epoch offsets for predicate translators.
//!
//! Semantic UTF-8 conversions require a UTF-8 PostgreSQL server encoding;
//! bound readers and writers validate that capability once during binding.
//! Conversion errors are returned as [`ArrowConversionError`] and reported
//! once at the caller's PostgreSQL boundary.

mod batch_source;
mod datum_codec;
mod encoder;
mod error;
mod jsonb;
mod rule;
pub mod scan;
mod temporal;

pub use batch_source::ArrowBatchSource;
pub use datum_codec::{
    ArrowColumnDecoder, BoundBatch, BoundColumn, BoundDatumBuffer, BoundDatumPlan,
    BoundWriteBuffer, BoundWriteColumnPlan, DatumCodec, DecodedColumn,
    PgDatumArrayBuilder, PgDatumArrayReader,
};
pub use error::{ArrowConversionError, ArrowConversionResult};
pub use jsonb::{NestedJsonbDefault, NestedJsonbDefaults, NestedJsonbPlan};
pub use rule::{CellRule, ListElementRule, PgColumnType};
pub use temporal::{pg_epoch_days_to_unix_days, pg_epoch_micros_to_unix_micros};
