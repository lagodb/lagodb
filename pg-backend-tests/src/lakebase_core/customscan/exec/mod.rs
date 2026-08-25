//! Backend tests for CustomScan execution helpers.
//! Full `ExecInitCustomScan` coverage lives in lagodb-iceberg regressions.
//!
//! Split by subsystem so each file targets one exec helper / trampoline:
//! - [`slice`]: binding/recheck expression sections + relation identity.
//! - [`emit`]: `RelationHandle` accessors + `emit_row` / `emit_columns`.
//! - [`state`]: the `CustomScanStateWrapper` base-pointer invariant.
//!
//! Fixtures shared by more than one submodule live in [`support`].

#[cfg(any(test, feature = "pg_test"))]
mod support;

mod emit;
mod slice;
mod state;
