//! Runtime scalar leaf model ([`IcebergScalar`]) and its operand-shape tag.
//!
//! Pure model layer: carries no dependency on the translator's error type.
//! Error mapping (e.g. rejecting a non-column operand) lives in the translator.

use iceberg_lite::expr::Reference;
use iceberg_lite::spec::Datum;
use pgrx::pg_sys;

/// Runtime scalar leaf: column, non-null literal/param, or NULL operand.
#[derive(Debug, Clone)]
pub enum IcebergScalar {
    Column {
        reference: Reference,
        atttypid: pg_sys::Oid,
    },
    Datum(Datum),
    /// NULL literal or NULL-resolved param; strict comparisons fold to `AlwaysFalse`.
    Null {
        type_oid: pg_sys::Oid,
    },
}

impl IcebergScalar {
    pub(super) fn kind(&self) -> ScalarKind {
        match self {
            IcebergScalar::Column { .. } => ScalarKind::Column,
            IcebergScalar::Datum(_) => ScalarKind::Datum,
            IcebergScalar::Null { .. } => ScalarKind::Null,
        }
    }
}

/// Operand shape tag for
/// [`IcebergTranslationError::ComparisonShape`](super::IcebergTranslationError::ComparisonShape).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScalarKind {
    Column,
    Datum,
    Null,
}
