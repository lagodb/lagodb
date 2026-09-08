//! Proven lossless PostgreSQL integer widening casts.

use pgrx::pg_sys;

/// The complete set of built-in integer casts that are monotonic and preserve
/// every source value exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgIntegerWidening {
    /// `smallint` to `integer`.
    Int2ToInt4,
    /// `smallint` to `bigint`.
    Int2ToInt8,
    /// `integer` to `bigint`.
    Int4ToInt8,
}

impl PgIntegerWidening {
    /// Resolve a built-in cast function together with its operand types.
    pub fn for_function(
        function: pg_sys::Oid,
        source: pg_sys::Oid,
        target: pg_sys::Oid,
    ) -> Option<Self> {
        match (u32::from(function), source, target) {
            (pg_sys::F_INT4_INT2, pg_sys::INT2OID, pg_sys::INT4OID) => {
                Some(Self::Int2ToInt4)
            }
            (pg_sys::F_INT8_INT2, pg_sys::INT2OID, pg_sys::INT8OID) => {
                Some(Self::Int2ToInt8)
            }
            (pg_sys::F_INT8_INT4, pg_sys::INT4OID, pg_sys::INT8OID) => {
                Some(Self::Int4ToInt8)
            }
            _ => None,
        }
    }

    /// Resolve the semantic widening from its source and target types.
    pub const fn for_types(source: pg_sys::Oid, target: pg_sys::Oid) -> Option<Self> {
        match (source, target) {
            (pg_sys::INT2OID, pg_sys::INT4OID) => Some(Self::Int2ToInt4),
            (pg_sys::INT2OID, pg_sys::INT8OID) => Some(Self::Int2ToInt8),
            (pg_sys::INT4OID, pg_sys::INT8OID) => Some(Self::Int4ToInt8),
            _ => None,
        }
    }
}
