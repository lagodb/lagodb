//! Backend `#[pg_test]` tests for `pg-arrow-conv`.
//!
//! These need a live PostgreSQL backend because datum construction for the
//! numeric/temporal/uuid/varlena arms calls into PG (`numeric_recv`, `palloc`,
//! detoast, slot writes, ...), so they cannot run as host `#[test]`s. The pure
//! resolution-table / codec-math tests stay as host tests in
//! `pg-arrow-conv/tests/`.

mod buffer_behavior;
mod decoder_equivalence;
mod decoder_to_slot;
mod encoder_equivalence;
