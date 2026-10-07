//! PostgreSQL tuple value abstractions.
//!
//! This module provides high-level abstractions for PostgreSQL tuple values:
//!
//! - [`cell`]: the `Cell` enum representing a single column value, plus the
//!   non-owning view types and `Datum` conversions.
//! - [`datum`]: cached destination-attribute conversion plans.
//! - [`json`]: owned semantic values for PostgreSQL `json` and `jsonb`.
//! - [`row`]: owned `Row` values and `TupleSlotWriter`.
//! - [`slot_row`]: callback-scoped `TupleSlotRow` / `TupleSlotBatch` views,
//!   `PgDatumRef`, and relation-bound slot access.
//! - [`numeric`]: NUMERIC typmod helpers and PG/Unix epoch constants.
//!
//! `TupleSlotRow`, `TupleSlotBatch`, and `PgDatumRef` are source views over
//! PostgreSQL-owned memory. They are valid only for the TableAM callback that
//! created them. Use `Row` when values must be buffered beyond that callback.

mod array;
mod cell;
mod codec_context;
mod composite;
mod datum;
mod decimal128_semantics;
mod json;
mod json_encode;
mod jsonb_builder;
mod jsonb_reader;
mod numeric;
mod pg_type;
mod row;
mod row_codec;
mod slot_columns;
mod slot_row;
mod text_io;
mod varlena;

pub use array::{ArrayDatumBuffer, ArrayDatumIter, ArrayDatumLayout};
pub use cell::{ByteaView, Cell, StringView};
pub use composite::CompositeDatumCodec;
pub use datum::{
    ColumnDatumCodec, ColumnDatumTarget, DatumConversionError, Utf8ServerEncoding,
    Utf8ServerEncodingError,
};
pub use decimal128_semantics::Decimal128Semantics;
pub use json::{JsonText, JsonValueError, JsonbValue};
pub use json_encode::{BoundJsonObjectEncoder, JsonDatumEncoder, JsonDatumKind};
pub use jsonb_builder::{JsonbBuilder, JsonbPosition};
pub use jsonb_reader::{
    JsonbContainer, JsonbDatetime, JsonbNumeric, JsonbObject, JsonbReader, JsonbRef,
};
pub use numeric::{
    Decimal128ComparisonValue, Decimal128NumericCodec, DecimalCodecError,
    NumericTypmod, PG_EPOCH_DAYS_DIFF, PG_EPOCH_USECS_DIFF, PostgresNumericCodec,
    numeric_precision_scale, numeric_typmod,
};
pub use pg_type::{PgCompositeField, PgType};
pub use row::{Row, TupleSlotWriter};
pub use row_codec::RowDatumCodec;
pub use slot_columns::SlotColumns;
pub use slot_row::{
    PgDatumRef, SlotDatumIndex, SlotDatums, TupleSlotBatch, TupleSlotRow,
};
pub use text_io::PgTextIo;
pub use varlena::DetoastedVarlena;
