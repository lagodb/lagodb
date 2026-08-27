//! Shared Iceberg capability policy and PostgreSQL operator mapping.

use lagodb_core::expr::PgComparisonIdentity;
use lagodb_core::expr::pushdown::{FilterColumn, FilterValueSlot};
use pgrx::{PgBuiltInOids, PgOid, pg_sys};

use super::plan::PlannedValueType;

/// Semantic comparison-operator class from `opno`
/// ([`PredicatePushdownPolicy::op_class`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ComparisonOpClass {
    /// `=`
    Eq,
    /// `<>`
    NotEq,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
}

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

/// Collation facts consumed by the pure capability policy.
///
/// PostgreSQL catalog lookup belongs to [`PgPredicatePushdownPolicy`]; the
/// policy itself reasons only about this resolved value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CollationSemantics {
    /// `InvalidOid`: no collation applies.
    None,
    /// Built-in `C` or `POSIX`; byte ordering matches Iceberg string ordering.
    COrPosix,
    /// A deterministic non-C PostgreSQL collation.
    Deterministic,
    /// A non-deterministic PostgreSQL collation.
    NonDeterministic,
}

/// Built-in `pg_operator` OIDs supported by the predicate policy.
///
/// `PgBuiltInOids` models PostgreSQL type/catalog OIDs. `OpExpr.opno` is a
/// `pg_operator` OID, and pgrx exposes only the subset generated from
/// PostgreSQL headers. Keep the missing built-in comparison operators named
/// here instead of embedding magic numbers in the policy logic.
mod pg_operator_oid {
    use pgrx::pg_sys;

    pub const INT2_EQ: u32 = 94;
    pub const INT2_NE: u32 = 519;
    pub const INT2_LT: u32 = 95;
    pub const INT2_LE: u32 = 522;
    pub const INT2_GT: u32 = 520;
    pub const INT2_GE: u32 = 524;

    pub const INT4_EQ: u32 = pg_sys::Int4EqualOperator;
    pub const INT4_NE: u32 = 518;
    pub const INT4_LT: u32 = pg_sys::Int4LessOperator;
    pub const INT4_LE: u32 = 523;
    pub const INT4_GT: u32 = 521;
    pub const INT4_GE: u32 = 525;

    pub const INT8_EQ: u32 = 410;
    pub const INT8_NE: u32 = 411;
    pub const INT8_LT: u32 = pg_sys::Int8LessOperator;
    pub const INT8_LE: u32 = 414;
    pub const INT8_GT: u32 = 413;
    pub const INT8_GE: u32 = 415;

    pub const DATE_EQ: u32 = 1093;
    pub const DATE_NE: u32 = 1094;
    pub const DATE_LT: u32 = 1095;
    pub const DATE_LE: u32 = 1096;
    pub const DATE_GT: u32 = 1097;
    pub const DATE_GE: u32 = 1098;

    pub const TIMESTAMP_EQ: u32 = 2060;
    pub const TIMESTAMP_NE: u32 = 2061;
    pub const TIMESTAMP_LT: u32 = 2062;
    pub const TIMESTAMP_LE: u32 = 2063;
    pub const TIMESTAMP_GT: u32 = 2064;
    pub const TIMESTAMP_GE: u32 = 2065;

    pub const TIMESTAMPTZ_EQ: u32 = 1320;
    pub const TIMESTAMPTZ_NE: u32 = 1321;
    pub const TIMESTAMPTZ_LT: u32 = 1322;
    pub const TIMESTAMPTZ_LE: u32 = 1323;
    pub const TIMESTAMPTZ_GT: u32 = 1324;
    pub const TIMESTAMPTZ_GE: u32 = 1325;

    pub const TEXT_EQ: u32 = pg_sys::TextEqualOperator;
    pub const TEXT_NE: u32 = 531;
    pub const TEXT_LT: u32 = pg_sys::TextLessOperator;
    pub const TEXT_LE: u32 = 665;
    pub const TEXT_GT: u32 = 666;
    pub const TEXT_GE: u32 = pg_sys::TextGreaterEqualOperator;
}

/// Pure Iceberg predicate policy used by planned-predicate construction.
pub(crate) struct PredicatePushdownPolicy;

impl PredicatePushdownPolicy {
    /// Map comparison `opno` to [`ComparisonOpClass`], or `None` if unrecognized.
    pub(crate) fn op_class(opno: pg_sys::Oid) -> Option<ComparisonOpClass> {
        use pg_operator_oid as op;

        let class = match u32::from(opno) {
            op::INT2_EQ
            | op::INT4_EQ
            | op::INT8_EQ
            | op::DATE_EQ
            | op::TIMESTAMP_EQ
            | op::TIMESTAMPTZ_EQ
            | op::TEXT_EQ => ComparisonOpClass::Eq,

            op::INT2_NE
            | op::INT4_NE
            | op::INT8_NE
            | op::DATE_NE
            | op::TIMESTAMP_NE
            | op::TIMESTAMPTZ_NE
            | op::TEXT_NE => ComparisonOpClass::NotEq,

            op::INT2_LT
            | op::INT4_LT
            | op::INT8_LT
            | op::DATE_LT
            | op::TIMESTAMP_LT
            | op::TIMESTAMPTZ_LT
            | op::TEXT_LT => ComparisonOpClass::Lt,

            op::INT2_LE
            | op::INT4_LE
            | op::INT8_LE
            | op::DATE_LE
            | op::TIMESTAMP_LE
            | op::TIMESTAMPTZ_LE
            | op::TEXT_LE => ComparisonOpClass::Le,

            op::INT2_GT
            | op::INT4_GT
            | op::INT8_GT
            | op::DATE_GT
            | op::TIMESTAMP_GT
            | op::TIMESTAMPTZ_GT
            | op::TEXT_GT => ComparisonOpClass::Gt,

            op::INT2_GE
            | op::INT4_GE
            | op::INT8_GE
            | op::DATE_GE
            | op::TIMESTAMP_GE
            | op::TIMESTAMPTZ_GE
            | op::TEXT_GE => ComparisonOpClass::Ge,

            _ => return None,
        };
        Some(class)
    }

    fn capability_for_class(
        type_oid: pg_sys::Oid,
        op_key: PgComparisonIdentity,
        input_collation: CollationSemantics,
        class: ComparisonOpClass,
    ) -> Option<SupportedPredicateCapability> {
        match PgOid::from(type_oid) {
            PgOid::BuiltIn(
                PgBuiltInOids::INT2OID
                | PgBuiltInOids::INT4OID
                | PgBuiltInOids::INT8OID,
            ) => {
                if Self::is_collation_free(op_key) {
                    Some(SupportedPredicateCapability::Exact)
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
            ) => Self::conservative_pruning_for_eq_and_ordered(class),

            PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID) => {
                match class {
                    ComparisonOpClass::Eq => {
                        if matches!(
                            input_collation,
                            CollationSemantics::COrPosix
                                | CollationSemantics::Deterministic
                        ) {
                            Some(SupportedPredicateCapability::Conservative)
                        } else {
                            None
                        }
                    }
                    ComparisonOpClass::Lt
                    | ComparisonOpClass::Le
                    | ComparisonOpClass::Gt
                    | ComparisonOpClass::Ge => {
                        if input_collation == CollationSemantics::COrPosix {
                            Some(SupportedPredicateCapability::Conservative)
                        } else {
                            None
                        }
                    }
                    ComparisonOpClass::NotEq => None,
                }
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
            // results), the same failure class as numeric/float comparisons.
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
            ComparisonOpClass::NotEq => None,
            ComparisonOpClass::Eq
            | ComparisonOpClass::Lt
            | ComparisonOpClass::Le
            | ComparisonOpClass::Gt
            | ComparisonOpClass::Ge => {
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
    /// The type allowlist admits the same scan value types as the comparison
    /// policy plus floats. Types outside this set (e.g. `bool`, `bytea`) return
    /// `false` as a conservative policy choice.
    pub(crate) fn supports_null_test(type_oid: pg_sys::Oid) -> bool {
        // IS NULL / IS NOT NULL only inspects the null bitmap — there is no
        // value comparison, so NaN ordering/equality divergence does not apply.
        // Float null-tests are safe even though float comparisons are unsupported.
        matches!(
            PgOid::from(type_oid),
            PgOid::BuiltIn(
                PgBuiltInOids::INT2OID
                    | PgBuiltInOids::INT4OID
                    | PgBuiltInOids::INT8OID
                    | PgBuiltInOids::NUMERICOID
                    | PgBuiltInOids::DATEOID
                    | PgBuiltInOids::TIMESTAMPOID
                    | PgBuiltInOids::TIMESTAMPTZOID
                    | PgBuiltInOids::TEXTOID
                    | PgBuiltInOids::VARCHAROID
                    | PgBuiltInOids::FLOAT4OID
                    | PgBuiltInOids::FLOAT8OID,
            )
        )
    }
}

/// PostgreSQL-facing adapter that resolves catalog-backed collation facts
/// before delegating to [`PredicatePushdownPolicy`].
pub(crate) struct PgPredicatePushdownPolicy;

impl PgPredicatePushdownPolicy {
    pub(crate) fn plan_comparison(
        column: &FilterColumn,
        value: &FilterValueSlot,
        op_key: PgComparisonIdentity,
    ) -> Option<(SupportedComparison, PlannedValueType)> {
        let value_type = Self::planned_value_type(column, value)?;
        let type_oid = column.value_type.type_oid;
        let operator = PredicatePushdownPolicy::op_class(op_key.opno)?;
        let collation = Self::resolved_collation(type_oid, op_key.inputcollid);
        let capability = PredicatePushdownPolicy::capability_for_class(
            type_oid, op_key, collation, operator,
        )?;
        Some((
            SupportedComparison {
                operator,
                capability,
            },
            value_type,
        ))
    }

    fn planned_value_type(
        column: &FilterColumn,
        value: &FilterValueSlot,
    ) -> Option<PlannedValueType> {
        let declared = PgOid::from(column.declared_type.type_oid);
        let effective = PgOid::from(column.value_type.type_oid);
        let value = PgOid::from(value.value_type.type_oid);
        match (declared, effective, value) {
            (
                PgOid::BuiltIn(PgBuiltInOids::INT2OID),
                PgOid::BuiltIn(PgBuiltInOids::INT2OID),
                PgOid::BuiltIn(PgBuiltInOids::INT2OID),
            ) => Some(PlannedValueType::Int2),
            (
                PgOid::BuiltIn(PgBuiltInOids::INT4OID),
                PgOid::BuiltIn(PgBuiltInOids::INT4OID),
                PgOid::BuiltIn(PgBuiltInOids::INT4OID),
            ) => Some(PlannedValueType::Int4),
            (
                PgOid::BuiltIn(PgBuiltInOids::INT8OID),
                PgOid::BuiltIn(PgBuiltInOids::INT8OID),
                PgOid::BuiltIn(PgBuiltInOids::INT8OID),
            ) => Some(PlannedValueType::Int8),
            (
                PgOid::BuiltIn(PgBuiltInOids::DATEOID),
                PgOid::BuiltIn(PgBuiltInOids::DATEOID),
                PgOid::BuiltIn(PgBuiltInOids::DATEOID),
            ) => Some(PlannedValueType::Date),
            (
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID),
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID),
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID),
            ) => Some(PlannedValueType::Timestamp),
            (
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID),
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID),
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID),
            ) => Some(PlannedValueType::Timestamptz),
            (
                PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID),
                PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID),
                PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID),
            ) => Some(PlannedValueType::String),
            _ => None,
        }
    }

    fn resolved_collation(
        type_oid: pg_sys::Oid,
        input_collation: pg_sys::Oid,
    ) -> CollationSemantics {
        if matches!(
            PgOid::from(type_oid),
            PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID)
        ) {
            Self::collation_semantics(input_collation)
        } else {
            // Non-text policy branches never consume catalog collation facts.
            // In particular, an invalid synthetic integer tag must be rejected
            // by the pure `(opcollid, inputcollid)` gate, not looked up in
            // pg_collation first.
            CollationSemantics::None
        }
    }

    /// Resolve `pg_collation.collisdeterministic` for one analyzed expression.
    pub(crate) fn collation_semantics(oid: pg_sys::Oid) -> CollationSemantics {
        if oid == pg_sys::Oid::INVALID {
            return CollationSemantics::None;
        }
        if oid == pg_sys::C_COLLATION_OID || oid == pg_sys::POSIX_COLLATION_OID {
            return CollationSemantics::COrPosix;
        }
        // SAFETY: non-zero `inputcollid` comes from PostgreSQL's analyzed
        // expression tree and therefore names a live `pg_collation` row.
        // `get_collation_isdeterministic` reports catalog corruption through
        // PostgreSQL ERROR; that error reaches the framework's FFI boundary.
        if unsafe { pg_sys::get_collation_isdeterministic(oid) } {
            CollationSemantics::Deterministic
        } else {
            CollationSemantics::NonDeterministic
        }
    }
}

#[cfg(test)]
mod test;
