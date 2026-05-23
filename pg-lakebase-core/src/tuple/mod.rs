//! PostgreSQL tuple value abstractions.
//!
//! This module provides high-level abstractions for PostgreSQL tuple values:
//!
//! - [`cell`]: the `Cell` enum representing a single column value, plus the
//!   non-owning view types and `Datum` conversions.
//! - [`row`]: owned `Row` values, callback-scoped `TupleSlotRow` /
//!   `TupleSlotBatch` views, `PgDatumRef`, and `TupleSlotWriter`.
//! - [`numeric`]: NUMERIC typmod helpers and PG/Unix epoch constants.
//!
//! `TupleSlotRow`, `TupleSlotBatch`, and `PgDatumRef` are source views over
//! PostgreSQL-owned memory. They are valid only for the TableAM callback that
//! created them. Use `Row` when values must be buffered beyond that callback.

mod cell;
mod numeric;
mod row;

pub use cell::{ByteaView, Cell, StringView};
pub use numeric::{
    Decimal128NumericCodec, DecimalCodecError, NumericTypmod, PG_EPOCH_DAYS_DIFF,
    PG_EPOCH_USECS_DIFF, numeric_precision_scale, numeric_typmod,
};
pub use row::{PgDatumRef, Row, TupleSlotBatch, TupleSlotRow, TupleSlotWriter};
