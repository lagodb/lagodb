//! Shared Iceberg capability policy and PostgreSQL operator mapping.

mod integer;
mod postgres;

use lagodb_core::expr::{
    PgComparisonIdentity, PgComparisonSignature, PgTextComparisonSemantics,
};
use pgrx::{PgBuiltInOids, PgOid, pg_sys};

pub(crate) use integer::Int32OutOfRange;
pub(crate) use lagodb_core::expr::PgComparisonKind as ComparisonOpClass;
pub(crate) use postgres::PgPredicatePushdownPolicy;

/// One comparison accepted by the PostgreSQL-facing Iceberg policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SupportedComparison {
    pub(crate) operator: ComparisonOpClass,
    pub(crate) capability: SupportedPredicateCapability,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SupportedPredicateCapability {
    Exact,
    Conservative,
}

/// Logical scalar kinds understood by Iceberg predicate construction.
/// PostgreSQL OIDs and Arrow types are translated by their respective
/// adapters before entering this provider policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PredicateValueKind {
    Boolean,
    Integer,
    Long,
    Date,
    Timestamp,
    Timestamptz,
    Decimal,
    Float,
    String,
}

/// Pure Iceberg predicate policy used by planned-predicate construction.
///
/// Set membership is deliberately not part of this policy. iceberg-lite's
/// current Arrow row filter evaluates one full-batch equality and OR per
/// literal, giving `O(batch_rows * literals)` work and intermediate Boolean
/// arrays. PostgreSQL can select hashed ScalarArrayOp execution, and DataFusion
/// builds a hash-based static filter. Compact temporal arrays also cannot be
/// costed until plan-time admission can prove that every value will remain
/// representable when the predicate binds.
pub(crate) struct PredicatePushdownPolicy;

impl PredicatePushdownPolicy {
    pub(crate) fn comparison_capability(
        value_kind: PredicateValueKind,
        operator: ComparisonOpClass,
    ) -> Option<SupportedPredicateCapability> {
        match value_kind {
            PredicateValueKind::Boolean => match operator {
                ComparisonOpClass::Equal | ComparisonOpClass::NotEqual => {
                    Some(SupportedPredicateCapability::Exact)
                }
                _ => None,
            },
            PredicateValueKind::Float => None,
            PredicateValueKind::Integer
            | PredicateValueKind::Long
            | PredicateValueKind::Date
            | PredicateValueKind::Timestamp
            | PredicateValueKind::Timestamptz
            | PredicateValueKind::Decimal
            | PredicateValueKind::String => Some(SupportedPredicateCapability::Exact),
        }
    }

    pub(crate) const fn supports_starts_with(value_kind: PredicateValueKind) -> bool {
        matches!(value_kind, PredicateValueKind::String)
    }

    pub(crate) const fn supports_nan_test(value_kind: PredicateValueKind) -> bool {
        matches!(value_kind, PredicateValueKind::Float)
    }

    /// Iceberg null predicates inspect only validity, so every logical kind
    /// represented by the provider schema has exact null-test semantics.
    pub(crate) const fn supports_null_test(value_kind: PredicateValueKind) -> bool {
        match value_kind {
            PredicateValueKind::Boolean
            | PredicateValueKind::Integer
            | PredicateValueKind::Long
            | PredicateValueKind::Date
            | PredicateValueKind::Timestamp
            | PredicateValueKind::Timestamptz
            | PredicateValueKind::Decimal
            | PredicateValueKind::Float
            | PredicateValueKind::String => true,
        }
    }

    /// Map a comparison from an Iceberg-supported operator family.
    ///
    /// PostgreSQL normalization already guarantees compatibility between the
    /// selected signature and the comparison operands.
    pub(crate) fn op_class(opno: pg_sys::Oid) -> Option<ComparisonOpClass> {
        let signature = PgComparisonSignature::for_operator(opno)?;
        match (signature.left_type(), signature.right_type()) {
            (pg_sys::INT2OID, pg_sys::INT2OID)
            | (pg_sys::INT4OID, pg_sys::INT4OID)
            | (pg_sys::INT8OID, pg_sys::INT8OID)
            | (pg_sys::DATEOID, pg_sys::DATEOID)
            | (pg_sys::TIMESTAMPOID, pg_sys::TIMESTAMPOID)
            | (pg_sys::TIMESTAMPTZOID, pg_sys::TIMESTAMPTZOID)
            | (pg_sys::NUMERICOID, pg_sys::NUMERICOID)
            | (pg_sys::TEXTOID, pg_sys::TEXTOID) => Some(signature.kind()),
            _ => None,
        }
    }

    fn capability_for_class(
        type_oid: pg_sys::Oid,
        op_key: PgComparisonIdentity,
        text_semantics: Option<PgTextComparisonSemantics>,
        class: ComparisonOpClass,
    ) -> Option<SupportedPredicateCapability> {
        match PgOid::from(type_oid) {
            PgOid::BuiltIn(
                PgBuiltInOids::INT2OID
                | PgBuiltInOids::INT4OID
                | PgBuiltInOids::INT8OID,
            ) => {
                if Self::is_collation_free(op_key) {
                    Self::comparison_capability(
                        if type_oid == pg_sys::INT8OID {
                            PredicateValueKind::Long
                        } else {
                            PredicateValueKind::Integer
                        },
                        class,
                    )
                } else {
                    // Integer comparisons with a tagged collation are not
                    // translatable; do not mark pushable.
                    None
                }
            }

            PgOid::BuiltIn(
                PgBuiltInOids::DATEOID
                | PgBuiltInOids::TIMESTAMPOID
                | PgBuiltInOids::TIMESTAMPTZOID,
            ) => {
                let kind = match PgOid::from(type_oid) {
                    PgOid::BuiltIn(PgBuiltInOids::DATEOID) => {
                        PredicateValueKind::Date
                    }
                    PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
                        PredicateValueKind::Timestamp
                    }
                    PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => {
                        PredicateValueKind::Timestamptz
                    }
                    _ => unreachable!("matched temporal PostgreSQL type"),
                };
                Self::comparison_capability(kind, class)?;
                Self::conservative_pruning_for_eq_and_ordered(class)
            }

            PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => {
                if Self::is_collation_free(op_key) {
                    Self::comparison_capability(PredicateValueKind::Decimal, class)
                } else {
                    None
                }
            }

            PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID) => {
                text_semantics?;
                Self::comparison_capability(PredicateValueKind::String, class)
            }

            // `char(n)` / `bpchar` comparison pushdown is gated off (falls
            // through to `Unsupported` below, alongside every other type).
            // `bpchar` maps to an Iceberg `String` column (see
            // `schema_builder`), but unlike `text` / `varchar` its comparison
            // semantics are *blank-padded*: PostgreSQL stores the value padded
            // with trailing spaces to the declared length, while `bpchareq` /
            // `bpcharlt` ignore trailing spaces. The only filter API this
            // provider has is a byte-wise Iceberg/Arrow string comparison, so a
            // stored `'ab   '` would not match a planned `col = 'ab'` even though
            // PostgreSQL treats them as equal — a silent false negative (wrong
            // results), the same failure class as float comparisons.
            // This hazard is orthogonal to collation: it persists even under
            // `C` / `POSIX`. `IS NULL` / `IS NOT NULL` on `bpchar` is unaffected
            // (see `supports_null_test`): a null test inspects only the null
            // bitmap, never a value, so the padding semantics do not apply.
            //
            // Re-enabling would require a trailing-space normalization invariant
            // applied identically on the storage write path and the planned
            // literal, after which the `text` collation gate above (deterministic
            // for `=`, C/POSIX for ordered) could be reused for `bpchar`.

            // Every other comparison type is intentionally unsupported.
            _ => None,
        }
    }

    /// Types whose const literals lack plan-time datum inspection
    /// (ConservativePruning is uncosted).
    pub(crate) fn is_value_sensitive_type(type_oid: pg_sys::Oid) -> bool {
        matches!(
            PgOid::from(type_oid),
            PgOid::BuiltIn(
                PgBuiltInOids::DATEOID
                    | PgBuiltInOids::TIMESTAMPOID
                    | PgBuiltInOids::TIMESTAMPTZOID
            )
        )
    }

    /// Accept equality and ordered temporal comparisons as conservative
    /// pruning; reject `<>`.
    #[inline]
    fn conservative_pruning_for_eq_and_ordered(
        class: ComparisonOpClass,
    ) -> Option<SupportedPredicateCapability> {
        match class {
            ComparisonOpClass::NotEqual => None,
            ComparisonOpClass::Equal
            | ComparisonOpClass::Less
            | ComparisonOpClass::LessEqual
            | ComparisonOpClass::Greater
            | ComparisonOpClass::GreaterEqual => {
                Some(SupportedPredicateCapability::Conservative)
            }
        }
    }

    /// `(opcollid, inputcollid) == (InvalidOid, InvalidOid)` — integer Exact guard.
    #[inline]
    fn is_collation_free(op_key: PgComparisonIdentity) -> bool {
        op_key.opcollid == pg_sys::Oid::INVALID
            && op_key.inputcollid == pg_sys::Oid::INVALID
    }

    /// Capability oracle for `IS NULL` / `IS NOT NULL` on a scan-column type.
    ///
    /// Iceberg natively supports `IsNull` / `NotNull` unary predicates. The
    /// contract is `ExactRowFilter`: the predicate is SQL-equivalent to PG's
    /// `NullTest`, so residual is not needed.
    ///
    /// Null tests only inspect the null bitmap — no value comparison is
    /// involved — so the NaN ordering divergence that disables float
    /// *comparison* pushdown does not apply here. Float IS NULL / IS NOT NULL
    /// remains safe even though float comparisons are unsupported.
    ///
    /// The type allowlist admits every logical scalar kind that the provider
    /// schema can map without inspecting the value. Types outside this set
    /// (for example `bytea`) remain unsupported.
    fn null_test_value_kind(type_oid: pg_sys::Oid) -> Option<PredicateValueKind> {
        // IS NULL / IS NOT NULL only inspects the null bitmap — there is no
        // value comparison, so NaN ordering/equality divergence does not apply.
        // Float null-tests are safe even though float comparisons are unsupported.
        match PgOid::from(type_oid) {
            PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => {
                Some(PredicateValueKind::Boolean)
            }
            PgOid::BuiltIn(PgBuiltInOids::INT2OID | PgBuiltInOids::INT4OID) => {
                Some(PredicateValueKind::Integer)
            }
            PgOid::BuiltIn(PgBuiltInOids::INT8OID) => Some(PredicateValueKind::Long),
            PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => {
                Some(PredicateValueKind::Decimal)
            }
            PgOid::BuiltIn(PgBuiltInOids::DATEOID) => Some(PredicateValueKind::Date),
            PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
                Some(PredicateValueKind::Timestamp)
            }
            PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => {
                Some(PredicateValueKind::Timestamptz)
            }
            PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID) => {
                Some(PredicateValueKind::String)
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID | PgBuiltInOids::FLOAT8OID) => {
                Some(PredicateValueKind::Float)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod test;
