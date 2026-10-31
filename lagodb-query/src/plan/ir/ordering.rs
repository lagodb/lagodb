//! Ordering semantics shared by query-level and aggregate-local sorts.

pub use lagodb_core::expr::PgOrderingDirection as SortDirection;
use lagodb_core::expr::{ExprType, PgTextComparisonSemantics};
use pgrx::pg_sys;

use super::Decimal128Semantics;

/// Exact PostgreSQL ordering capabilities implemented by Arrow-backed plans.
///
/// Every built-in btree ordering operator for an admitted type must agree with
/// Arrow's native order. This includes the non-default text pattern operators
/// (`~<~` and `~>~`). Re-audit that operator set whenever either allowlist
/// grows.
pub(super) struct OrderingSemantics;

impl OrderingSemantics {
    pub(super) fn supports_query_sort(result_type: ExprType) -> bool {
        // Arrow orders floats with IEEE totalOrder. PostgreSQL instead treats
        // -0/+0 as equal and every NaN as equal and greater than non-NaNs, so
        // FLOAT4/FLOAT8 are intentionally absent from this contract.
        match result_type.type_oid {
            pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID => {
                result_type.typmod == -1
                    && result_type.collation == pg_sys::InvalidOid
            }
            pg_sys::TEXTOID => {
                result_type.typmod == -1
                    // SAFETY: IR construction and wire reconstruction run in
                    // the PostgreSQL backend with live catalog state.
                    && unsafe {
                        PgTextComparisonSemantics::is_byte_ordered(
                            result_type.collation,
                        )
                    }
            }
            pg_sys::VARCHAROID => {
                result_type.typmod >= -1
                    // SAFETY: IR construction and wire reconstruction run in
                    // the PostgreSQL backend with live catalog state.
                    && unsafe {
                        PgTextComparisonSemantics::is_byte_ordered(
                            result_type.collation,
                        )
                    }
            }
            pg_sys::NUMERICOID => {
                Decimal128Semantics::for_type(result_type).is_some()
            }
            _ => false,
        }
    }

    pub(super) fn supports_aggregate_order(result_type: ExprType) -> bool {
        if Self::supports_query_sort(result_type) {
            return true;
        }

        // TIME has only a microsecond Arrow source. TIMESTAMP/TIMESTAMPTZ can
        // also come from nanoseconds, which DataFusion admission rejects: two
        // distinct nanosecond values can decode to the same PostgreSQL
        // microsecond, where PG must continue with later sort keys after Arrow
        // already ordered by nanoseconds.
        match result_type.type_oid {
            pg_sys::BOOLOID
            | pg_sys::DATEOID
            | pg_sys::UUIDOID
            | pg_sys::BYTEAOID => {
                result_type.typmod == -1
                    && result_type.collation == pg_sys::InvalidOid
            }
            pg_sys::TIMEOID | pg_sys::TIMESTAMPOID | pg_sys::TIMESTAMPTZOID => {
                result_type.typmod >= -1
                    && result_type.collation == pg_sys::InvalidOid
            }
            _ => false,
        }
    }
}
