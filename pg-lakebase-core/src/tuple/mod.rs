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

mod cell;
mod datum;
mod json;
mod numeric;
mod row;
mod row_codec;
mod slot_columns;
mod slot_row;

pub use cell::{ByteaView, Cell, StringView};
pub use datum::{ColumnDatumCodec, ColumnDatumTarget, DatumConversionError};
pub use json::{JsonText, JsonValueError, JsonbValue};
pub use numeric::{
    Decimal128NumericCodec, DecimalCodecError, NumericTypmod, PG_EPOCH_DAYS_DIFF,
    PG_EPOCH_USECS_DIFF, numeric_precision_scale, numeric_typmod,
};
pub use row::{Row, TupleSlotWriter};
pub use row_codec::RowDatumCodec;
pub use slot_columns::SlotColumns;
pub use slot_row::{
    PgDatumRef, SlotDatumIndex, SlotDatums, TupleSlotBatch, TupleSlotRow,
};
