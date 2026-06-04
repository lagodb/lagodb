//! Error type surfaced by the Iceberg runtime predicate translator.

use pgrx::pg_sys;

use super::scalar::ScalarKind;

/// Errors surfaced by the Iceberg runtime predicate translator.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IcebergTranslationError {
    #[error(
        "iceberg-am translator: get_attname returned NULL for \
         (rel_oid={}, attno={attno})",
        u32::from(*rel_oid)
    )]
    ColumnLookupFailed {
        rel_oid: pg_sys::Oid,
        attno: pg_sys::AttrNumber,
    },

    #[error(
        "iceberg-am translator: column name for (rel_oid={}, \
         attno={attno}) is not valid UTF-8: {cause}",
        u32::from(*rel_oid)
    )]
    ColumnNameNotUtf8 {
        rel_oid: pg_sys::Oid,
        attno: pg_sys::AttrNumber,
        cause: String,
    },

    #[error(
        "iceberg-am translator: refusing to translate system / \
         whole-row column (rel_oid={}, attno={attno})",
        u32::from(*rel_oid)
    )]
    SystemOrWholeRowColumn {
        rel_oid: pg_sys::Oid,
        attno: pg_sys::AttrNumber,
    },

    #[error(
        "iceberg-am translator: comparison expects column op literal/param; \
         got left={left:?}, right={right:?}"
    )]
    ComparisonShape { left: ScalarKind, right: ScalarKind },

    #[error(
        "iceberg-am translator: IS NULL / IS NOT NULL is only \
         supported on a column term"
    )]
    NullTestOnNonColumn,

    #[error(
        "iceberg-am translator: unsupported operator triple \
         (opno={}, opcollid={}, inputcollid={})",
        u32::from(*opno),
        u32::from(*opcollid),
        u32::from(*inputcollid)
    )]
    UnsupportedOperator {
        opno: pg_sys::Oid,
        opcollid: pg_sys::Oid,
        inputcollid: pg_sys::Oid,
    },

    #[error(
        "iceberg-am translator: unsupported PG type OID {}",
        u32::from(*type_oid)
    )]
    UnsupportedType { type_oid: pg_sys::Oid },

    /// Value cannot be represented as an iceberg `Datum` (ConservativePruning-drop path).
    #[error(
        "iceberg-am translator: value of PG type OID {} is not \
         representable as an iceberg Datum (dropped to residual)",
        u32::from(*type_oid)
    )]
    ValueNotRepresentable { type_oid: pg_sys::Oid },

    #[error(
        "iceberg-am translator: failed to decode PG Datum for type OID {}",
        u32::from(*type_oid)
    )]
    DatumDecode { type_oid: pg_sys::Oid },

    #[error(
        "iceberg-am translator: AND/OR called with an empty \
         children list"
    )]
    EmptyBoolExpr,
}
