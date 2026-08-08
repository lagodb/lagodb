//! Shared Iceberg capability policy and PostgreSQL operator mapping.

use pg_lakebase_core::expr::PgComparisonIdentity;
use pgrx::{PgBuiltInOids, PgOid, pg_sys};

/// Pushdown verdict: admissibility (`Unsupported` vs pushable) and contract tier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PredicateCapability {
    /// Row-level SQL-equivalent filter during the normal scan. Classifier maps to
    /// `QualPushdownDecision::Pushable { contract: ExactRowFilter, costing: CostedPruning }`.
    ExactRowFilter,
    /// Conservative pruning only (no false negatives; false positives allowed).
    /// Residual `plan.qual` keeps correctness. Classifier maps to
    /// `QualPushdownDecision::Pushable { contract: ConservativePruning, costing: ... }`;
    /// leaf rules assign `CostedPruning` or `UncostedBestEffort`.
    ConservativePruning,
    /// Not pushable. Maps to `QualPushdownDecision::Unsupported`, and
    /// gates the translator away from attempting a `Datum` build.
    Unsupported,
}

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

/// Pure Iceberg predicate pushdown policy shared by planner classification and
/// runtime translation.
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

    /// Capability oracle: pushability of `column op operand` for `type_oid` and
    /// `op_key`. Classifier and translator must agree; NULL operands are handled
    /// at translate time.
    pub(crate) fn capability_for(
        type_oid: pg_sys::Oid,
        op_key: PgComparisonIdentity,
        input_collation: CollationSemantics,
    ) -> PredicateCapability {
        // Unrecognized opno => never pushable (also rejects cross-category comparisons).
        let Some(class) = Self::op_class(op_key.opno) else {
            return PredicateCapability::Unsupported;
        };

        match PgOid::from(type_oid) {
            PgOid::BuiltIn(
                PgBuiltInOids::INT2OID
                | PgBuiltInOids::INT4OID
                | PgBuiltInOids::INT8OID,
            ) => {
                if Self::is_collation_free(op_key) {
                    PredicateCapability::ExactRowFilter
                } else {
                    // Integer comparisons with a tagged collation are not
                    // translatable; do not mark pushable.
                    PredicateCapability::Unsupported
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
                            PredicateCapability::ConservativePruning
                        } else {
                            PredicateCapability::Unsupported
                        }
                    }
                    ComparisonOpClass::Lt
                    | ComparisonOpClass::Le
                    | ComparisonOpClass::Gt
                    | ComparisonOpClass::Ge => {
                        if input_collation == CollationSemantics::COrPosix {
                            PredicateCapability::ConservativePruning
                        } else {
                            PredicateCapability::Unsupported
                        }
                    }
                    ComparisonOpClass::NotEq => PredicateCapability::Unsupported,
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
            // stored `'ab   '` would not match a pushed `col = 'ab'` even though
            // PostgreSQL treats them as equal — a silent false negative (wrong
            // results), the same failure class as numeric/float comparisons.
            // This hazard is orthogonal to collation: it persists even under
            // `C` / `POSIX`. `IS NULL` / `IS NOT NULL` on `bpchar` is unaffected
            // (see `supports_null_test`): a null test inspects only the null
            // bitmap, never a value, so the padding semantics do not apply.
            //
            // Re-enabling would require a trailing-space normalization invariant
            // applied identically on the storage write path and the pushed
            // literal, after which the `text` collation gate above (deterministic
            // for `=`, C/POSIX for ordered) could be reused for `bpchar`.

            // Every other comparison type is intentionally unsupported.
            _ => PredicateCapability::Unsupported,
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

    /// Returns [`PredicateCapability::ConservativePruning`] for equality and
    /// the four ordered classes; `Unsupported` for `<>`. Shared by the
    /// collation-agnostic temporal category (date / timestamp / timestamptz).
    #[inline]
    fn conservative_pruning_for_eq_and_ordered(
        class: ComparisonOpClass,
    ) -> PredicateCapability {
        match class {
            ComparisonOpClass::NotEq => PredicateCapability::Unsupported,
            ComparisonOpClass::Eq
            | ComparisonOpClass::Lt
            | ComparisonOpClass::Le
            | ComparisonOpClass::Gt
            | ComparisonOpClass::Ge => PredicateCapability::ConservativePruning,
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
    /// `false` as a conservative policy choice, not because the translator
    /// cannot construct a column reference for them.
    ///
    /// Only user columns (attno > 0) are pushable; the caller (classifier)
    /// verifies that.
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
    pub(crate) fn capability_for(
        type_oid: pg_sys::Oid,
        op_key: PgComparisonIdentity,
    ) -> PredicateCapability {
        let collation = if matches!(
            PgOid::from(type_oid),
            PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID)
        ) {
            Self::collation_semantics(op_key.inputcollid)
        } else {
            // Non-text policy branches never consume catalog collation facts.
            // In particular, an invalid synthetic integer tag must be rejected
            // by the pure `(opcollid, inputcollid)` gate, not looked up in
            // pg_collation first.
            CollationSemantics::None
        };
        PredicatePushdownPolicy::capability_for(type_oid, op_key, collation)
    }

    /// Whether the runtime translator should attempt a native predicate build.
    pub(crate) fn can_build(
        type_oid: pg_sys::Oid,
        op_key: PgComparisonIdentity,
    ) -> bool {
        !matches!(
            Self::capability_for(type_oid, op_key),
            PredicateCapability::Unsupported
        )
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

#[cfg(any(test, feature = "pg_test"))]
pub(super) mod test_opno_table {
    //! Shared comparison-operator fixture data for host and backend tests.

    use super::pg_operator_oid as op;

    // Per-type rows use `[Eq, NotEq, Lt, Le, Gt, Ge]` column order.
    pub(crate) const INT4: [u32; 6] = [
        op::INT4_EQ,
        op::INT4_NE,
        op::INT4_LT,
        op::INT4_LE,
        op::INT4_GT,
        op::INT4_GE,
    ];
    pub(crate) const DATE: [u32; 6] = [
        op::DATE_EQ,
        op::DATE_NE,
        op::DATE_LT,
        op::DATE_LE,
        op::DATE_GT,
        op::DATE_GE,
    ];
    pub(crate) const TIMESTAMP: [u32; 6] = [
        op::TIMESTAMP_EQ,
        op::TIMESTAMP_NE,
        op::TIMESTAMP_LT,
        op::TIMESTAMP_LE,
        op::TIMESTAMP_GT,
        op::TIMESTAMP_GE,
    ];
    pub(crate) const TEXT: [u32; 6] = [
        op::TEXT_EQ,
        op::TEXT_NE,
        op::TEXT_LT,
        op::TEXT_LE,
        op::TEXT_GT,
        op::TEXT_GE,
    ];

    #[cfg(test)]
    pub(crate) mod host_matrix {
        use pgrx::pg_sys;

        use super::{DATE, INT4, TEXT, TIMESTAMP, op};
        use crate::predicate::policy::ComparisonOpClass;

        const INT2: [u32; 6] = [
            op::INT2_EQ,
            op::INT2_NE,
            op::INT2_LT,
            op::INT2_LE,
            op::INT2_GT,
            op::INT2_GE,
        ];
        pub(crate) const INT8: [u32; 6] = [
            op::INT8_EQ,
            op::INT8_NE,
            op::INT8_LT,
            op::INT8_LE,
            op::INT8_GT,
            op::INT8_GE,
        ];
        const TIMESTAMPTZ: [u32; 6] = [
            op::TIMESTAMPTZ_EQ,
            op::TIMESTAMPTZ_NE,
            op::TIMESTAMPTZ_LT,
            op::TIMESTAMPTZ_LE,
            op::TIMESTAMPTZ_GT,
            op::TIMESTAMPTZ_GE,
        ];

        pub(crate) const CLASS_BY_COLUMN: [ComparisonOpClass; 6] = [
            ComparisonOpClass::Eq,
            ComparisonOpClass::NotEq,
            ComparisonOpClass::Lt,
            ComparisonOpClass::Le,
            ComparisonOpClass::Gt,
            ComparisonOpClass::Ge,
        ];

        /// Complete built-in comparison matrix mirrored from `pg_operator.dat`.
        pub(crate) fn opno_table() -> [(pg_sys::Oid, [u32; 6]); 7] {
            [
                (pg_sys::INT2OID, INT2),
                (pg_sys::INT4OID, INT4),
                (pg_sys::INT8OID, INT8),
                (pg_sys::DATEOID, DATE),
                (pg_sys::TIMESTAMPOID, TIMESTAMP),
                (pg_sys::TIMESTAMPTZOID, TIMESTAMPTZ),
                (pg_sys::TEXTOID, TEXT),
            ]
        }
    }
}

#[cfg(test)]
mod tests {
    //! Host-only tests for policy logic that does not reference backend symbols.

    use std::collections::HashSet;

    use pg_lakebase_core::expr::PgComparisonOp;
    use pgrx::pg_sys;
    use pgrx::pg_sys::Oid;
    use proptest::prelude::*;

    use super::test_opno_table as op;
    use super::test_opno_table::host_matrix::{CLASS_BY_COLUMN, INT8, opno_table};
    use super::{
        CollationSemantics, ComparisonOpClass, PredicateCapability,
        PredicatePushdownPolicy,
    };

    fn op_class(opno: pg_sys::Oid) -> Option<ComparisonOpClass> {
        PredicatePushdownPolicy::op_class(opno)
    }

    fn triple(opno: u32) -> PgComparisonOp {
        PgComparisonOp {
            opno: Oid::from(opno),
            opfuncid: Oid::INVALID,
            opresulttype: Oid::INVALID,
            opcollid: Oid::INVALID,
            inputcollid: Oid::INVALID,
        }
    }

    fn capability(
        type_oid: pg_sys::Oid,
        op: PgComparisonOp,
        collation: CollationSemantics,
    ) -> PredicateCapability {
        PredicatePushdownPolicy::capability_for(type_oid, op.identity(), collation)
    }

    #[test]
    fn op_class_maps_every_known_opno() {
        for (type_oid, opnos) in opno_table() {
            for (column, &opno) in opnos.iter().enumerate() {
                assert_eq!(
                    op_class(Oid::from(opno)),
                    Some(CLASS_BY_COLUMN[column]),
                    "opno {opno} (type {}, column {column}) must map to {:?}",
                    u32::from(type_oid),
                    CLASS_BY_COLUMN[column],
                );
            }
        }
    }

    #[test]
    fn op_class_table_has_no_duplicate_opnos() {
        let mut seen = HashSet::new();
        for (_, opnos) in opno_table() {
            for opno in opnos {
                assert!(seen.insert(opno), "opno {opno} appears twice in the table");
            }
        }
    }

    #[test]
    fn op_class_rejects_unknown_opnos() {
        assert_eq!(
            op_class(Oid::from(558u32)),
            None,
            "oidvector <> is not mapped",
        );
        assert_eq!(op_class(Oid::INVALID), None, "InvalidOid is not mapped");
        assert_eq!(
            op_class(Oid::from(9_999_999u32)),
            None,
            "unused OID is not mapped",
        );
    }

    #[test]
    fn supported_predicate_integers_are_exact_under_zero_collation() {
        for (type_oid, opnos) in opno_table().into_iter().take(3) {
            for opno in opnos {
                assert_eq!(
                    capability(type_oid, triple(opno), CollationSemantics::None),
                    PredicateCapability::ExactRowFilter,
                    "integer type {} opno {opno} must be exact",
                    u32::from(type_oid),
                );
            }
        }
    }

    const INTEGER_EXACT_SET: &[(pg_sys::Oid, u32)] = &[
        (pg_sys::INT4OID, op::INT4[0]),
        (pg_sys::INT4OID, op::INT4[1]),
        (pg_sys::INT4OID, op::INT4[2]),
        (pg_sys::INT4OID, op::INT4[3]),
        (pg_sys::INT4OID, op::INT4[4]),
        (pg_sys::INT4OID, op::INT4[5]),
        (pg_sys::INT8OID, INT8[0]),
        (pg_sys::INT8OID, INT8[1]),
        (pg_sys::INT8OID, INT8[2]),
        (pg_sys::INT8OID, INT8[3]),
        (pg_sys::INT8OID, INT8[4]),
        (pg_sys::INT8OID, INT8[5]),
    ];

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 256,
            failure_persistence: None,
            ..ProptestConfig::default()
        })]

        #[test]
        fn supported_predicate_integer_collation_gate_property(
            idx in 0usize..INTEGER_EXACT_SET.len(),
            collid in 1u32..=u32::MAX,
            tag_input in any::<bool>(),
        ) {
            let (type_oid, opno) = INTEGER_EXACT_SET[idx];
            let mut tagged = triple(opno);
            if tag_input {
                tagged.inputcollid = Oid::from(collid);
            } else {
                tagged.opcollid = Oid::from(collid);
            }
            prop_assert_eq!(
                capability(type_oid, tagged, CollationSemantics::Deterministic),
                PredicateCapability::Unsupported,
            );
        }
    }

    #[test]
    fn supported_predicate_ignores_diagnostic_fields() {
        let mut tagged_integer = triple(op::INT4[0]);
        tagged_integer.opcollid = Oid::from(50_000u32);
        let cases = [
            (
                pg_sys::INT4OID,
                triple(op::INT4[0]),
                CollationSemantics::None,
            ),
            (
                pg_sys::INT4OID,
                triple(op::INT4[2]),
                CollationSemantics::None,
            ),
            (pg_sys::INT4OID, tagged_integer, CollationSemantics::None),
            (pg_sys::NUMERICOID, triple(1752), CollationSemantics::None),
            (
                pg_sys::TEXTOID,
                triple(op::TEXT[0]),
                CollationSemantics::COrPosix,
            ),
            (
                pg_sys::TEXTOID,
                triple(op::TEXT[1]),
                CollationSemantics::COrPosix,
            ),
        ];

        for (type_oid, baseline, collation) in cases {
            let expected = capability(type_oid, baseline, collation);
            for (opfuncid, opresulttype) in [
                (Oid::from(65u32), pg_sys::BOOLOID),
                (Oid::from(9_999u32), Oid::from(9_999u32)),
            ] {
                assert_eq!(
                    capability(
                        type_oid,
                        PgComparisonOp {
                            opfuncid,
                            opresulttype,
                            ..baseline
                        },
                        collation,
                    ),
                    expected,
                );
            }
        }
    }

    #[test]
    fn supported_predicate_numeric_temporal_float_matrix() {
        for (type_oid, opnos) in opno_table().into_iter().skip(3).take(3) {
            for (column, opno) in opnos.into_iter().enumerate() {
                let expected = if CLASS_BY_COLUMN[column] == ComparisonOpClass::NotEq
                {
                    PredicateCapability::Unsupported
                } else {
                    PredicateCapability::ConservativePruning
                };
                assert_eq!(
                    capability(type_oid, triple(opno), CollationSemantics::None),
                    expected,
                );
            }
        }

        for opno in [1752, 1753, 1754, 1755, 1756, 1757] {
            assert_eq!(
                capability(
                    pg_sys::NUMERICOID,
                    triple(opno),
                    CollationSemantics::None
                ),
                PredicateCapability::Unsupported,
            );
        }
        for (type_oid, opnos) in [
            (pg_sys::FLOAT4OID, [620, 621, 622, 624, 623, 625]),
            (pg_sys::FLOAT8OID, [670, 671, 672, 673, 674, 675]),
        ] {
            for opno in opnos {
                assert_eq!(
                    capability(type_oid, triple(opno), CollationSemantics::None),
                    PredicateCapability::Unsupported,
                );
            }
        }
    }

    #[test]
    fn supported_predicate_only_integers_are_exact() {
        for (type_oid, opnos) in opno_table().into_iter().skip(3) {
            let collation = if type_oid == pg_sys::TEXTOID {
                CollationSemantics::COrPosix
            } else {
                CollationSemantics::None
            };
            for opno in opnos {
                assert_ne!(
                    capability(type_oid, triple(opno), collation),
                    PredicateCapability::ExactRowFilter,
                );
            }
        }
    }

    #[test]
    fn supported_predicate_unknown_inputs_are_unsupported() {
        for type_oid in [pg_sys::BOOLOID, pg_sys::BYTEAOID] {
            assert_eq!(
                capability(type_oid, triple(op::INT4[0]), CollationSemantics::None),
                PredicateCapability::Unsupported,
            );
        }
        assert_eq!(
            capability(pg_sys::INT4OID, triple(558), CollationSemantics::None),
            PredicateCapability::Unsupported,
        );
    }

    #[test]
    fn text_capability_depends_only_on_resolved_collation_semantics() {
        for type_oid in [pg_sys::TEXTOID, pg_sys::VARCHAROID] {
            for semantics in [
                CollationSemantics::COrPosix,
                CollationSemantics::Deterministic,
            ] {
                assert_eq!(
                    capability(type_oid, triple(op::TEXT[0]), semantics),
                    PredicateCapability::ConservativePruning,
                );
            }
            for semantics in [
                CollationSemantics::None,
                CollationSemantics::NonDeterministic,
            ] {
                assert_eq!(
                    capability(type_oid, triple(op::TEXT[0]), semantics),
                    PredicateCapability::Unsupported,
                );
            }
            for opno in op::TEXT[2..].iter().copied() {
                assert_eq!(
                    capability(type_oid, triple(opno), CollationSemantics::COrPosix),
                    PredicateCapability::ConservativePruning,
                );
                assert_eq!(
                    capability(
                        type_oid,
                        triple(opno),
                        CollationSemantics::Deterministic
                    ),
                    PredicateCapability::Unsupported,
                );
            }
            assert_eq!(
                capability(
                    type_oid,
                    triple(op::TEXT[1]),
                    CollationSemantics::COrPosix
                ),
                PredicateCapability::Unsupported,
            );
        }
    }

    #[test]
    fn null_tests_admit_supported_types_including_float() {
        for type_oid in [
            pg_sys::INT2OID,
            pg_sys::INT4OID,
            pg_sys::INT8OID,
            pg_sys::NUMERICOID,
            pg_sys::DATEOID,
            pg_sys::TIMESTAMPOID,
            pg_sys::TIMESTAMPTZOID,
            pg_sys::TEXTOID,
            pg_sys::VARCHAROID,
            pg_sys::FLOAT4OID,
            pg_sys::FLOAT8OID,
        ] {
            assert!(
                PredicatePushdownPolicy::supports_null_test(type_oid),
                "null tests must be supported for type {}",
                u32::from(type_oid),
            );
        }
    }

    #[test]
    fn null_tests_reject_unsupported_types() {
        for type_oid in [pg_sys::BOOLOID, pg_sys::BYTEAOID, Oid::from(9_999_999u32)] {
            assert!(
                !PredicatePushdownPolicy::supports_null_test(type_oid),
                "null tests must be unsupported for type {}",
                u32::from(type_oid),
            );
        }
    }
}
