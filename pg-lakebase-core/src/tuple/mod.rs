//! PostgreSQL tuple value abstractions for `Cell` and `Row`.
//!
//! This module provides high-level abstractions for PostgreSQL tuple values:
//!
//! - [`cell`]: the `Cell` enum representing a single column value, plus the
//!   non-owning view types and `Datum` conversions.
//! - [`row`]: the `Row` buffer and the `TupleSlotWriter` that materializes a
//!   row into a virtual tuple slot.
//! - [`numeric`]: NUMERIC typmod helpers and PG/Unix epoch constants.

mod cell;
mod numeric;
mod row;

pub use cell::{ByteaView, Cell, StringView};
pub use numeric::{
    NumericTypmod, PG_EPOCH_DAYS_DIFF, PG_EPOCH_USECS_DIFF, numeric_precision_scale,
    numeric_typmod,
};
pub use row::{Row, TupleSlotWriter};
