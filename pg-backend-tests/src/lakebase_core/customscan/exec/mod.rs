//! Backend tests for customscan exec helpers and rescan trampolines.
//! Full `ExecInitCustomScan` coverage lives in pg-iceberg-am regressions.
//!
//! Split by subsystem so each file targets one exec helper / trampoline:
//! - [`slice`]: `slice_pushed_recheck` + `check_scan_relation_oid`.
//! - [`param_refs`]: the cached pushed-expression parameter domain.
//! - [`rescan`]: the ReScan trampoline's chgParam gating + `bms_overlap`.
//! - [`runtime_params`]: `RuntimeParamResolver` EXTERN/EXEC resolution.
//! - [`emit`]: `RelationHandle` accessors + `emit_row` / `emit_columns`.
//! - [`state`]: the `CustomScanStateWrapper` base-pointer invariant.
//!
//! Fixtures shared by more than one submodule live in [`support`].

#[cfg(any(test, feature = "pg_test"))]
mod support;

mod emit;
mod param_refs;
mod rescan;
mod runtime_params;
mod slice;
mod state;
