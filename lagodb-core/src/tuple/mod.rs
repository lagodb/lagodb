//! PostgreSQL row and tuple-slot abstractions.
//!
//! This module owns tuple structure and callback-scoped slot access:
//!
//! - [`row`]: owned `Row` values and `RowSlotWriter`.
//! - [`slot_row`]: callback-scoped `TupleSlotRow` / `TupleSlotBatch` views,
//!   `PgDatumRef`, and relation-bound slot access.
//!
//! `TupleSlotRow`, `TupleSlotBatch`, and `PgDatumRef` are source views over
//! PostgreSQL-owned memory. They are valid only for the TableAM callback that
//! created them. Use `Row` when values must be buffered beyond that callback.
//!
//! Value types and PostgreSQL `Datum` codecs live in [`crate::datum`].

mod row;
mod row_codec;
mod slot_datum_writer;
mod slot_row;

pub use row::{Row, RowSlotWriter};
pub use row_codec::RowDatumCodec;
pub use slot_datum_writer::SlotDatumWriter;
pub use slot_row::{
    PgDatumRef, SlotDatumIndex, SlotDatumView, TupleSlotBatch, TupleSlotRow,
};
