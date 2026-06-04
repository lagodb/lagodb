//! Shared predicate pushdown policy for classifier and translator.

use pg_lakebase_core::expr::nodes::PgComparisonOp;
use pgrx::{PgBuiltInOids, PgOid, PgTryBuilder, pg_sys};

/// Pushdown verdict: admissibility (`Unsupported` vs pushable) and contract tier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredicateCapability {
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

/// Semantic comparison-operator class from `opno` ([`op_class`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComparisonOpClass {
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

impl ComparisonOpClass {
    /// Whether this is one of the four ordered classes
    /// (`<` / `<=` / `>` / `>=`). Equality and inequality are *not*
    /// ordered.
    #[inline]
    fn is_ordered(self) -> bool {
        matches!(
            self,
            ComparisonOpClass::Lt
                | ComparisonOpClass::Le
                | ComparisonOpClass::Gt
                | ComparisonOpClass::Ge
        )
    }
}

/// Float comparison pushdown toggle (disabled in v1 due to NaN semantic
/// divergence between Arrow IEEE 754 and PostgreSQL ordering) and numeric
/// comparison pushdown toggle (disabled in v1 due to decimal scale downcast
/// in the row-level filter).
use super::{FLOAT_PUSHDOWN_ENABLED, NUMERIC_COMPARISON_PUSHDOWN_ENABLED};

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

    pub const NUMERIC_EQ: u32 = 1752;
    pub const NUMERIC_NE: u32 = 1753;
    pub const NUMERIC_LT: u32 = 1754;
    pub const NUMERIC_LE: u32 = 1755;
    pub const NUMERIC_GT: u32 = 1756;
    pub const NUMERIC_GE: u32 = 1757;

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

    pub const FLOAT4_EQ: u32 = 620;
    pub const FLOAT4_NE: u32 = 621;
    pub const FLOAT4_LT: u32 = 622;
    pub const FLOAT4_LE: u32 = 624;
    pub const FLOAT4_GT: u32 = 623;
    pub const FLOAT4_GE: u32 = 625;

    pub const FLOAT8_EQ: u32 = 670;
    pub const FLOAT8_NE: u32 = 671;
    pub const FLOAT8_LT: u32 = pg_sys::Float8LessOperator;
    pub const FLOAT8_LE: u32 = 673;
    pub const FLOAT8_GT: u32 = 674;
    pub const FLOAT8_GE: u32 = 675;

    pub const TEXT_EQ: u32 = pg_sys::TextEqualOperator;
    pub const TEXT_NE: u32 = 531;
    pub const TEXT_LT: u32 = pg_sys::TextLessOperator;
    pub const TEXT_LE: u32 = 665;
    pub const TEXT_GT: u32 = 666;
    pub const TEXT_GE: u32 = pg_sys::TextGreaterEqualOperator;
}

/// Iceberg predicate pushdown policy shared by planner classification and
/// runtime translation.
#[derive(Clone, Copy, Debug, Default)]
pub struct PredicatePushdownPolicy;

impl PredicatePushdownPolicy {
    pub const fn new() -> Self {
        Self
    }

    /// Map comparison `opno` to [`ComparisonOpClass`], or `None` if unrecognized.
    pub fn op_class(&self, opno: pg_sys::Oid) -> Option<ComparisonOpClass> {
        use pg_operator_oid as op;

        let class = match u32::from(opno) {
            op::INT2_EQ
            | op::INT4_EQ
            | op::INT8_EQ
            | op::NUMERIC_EQ
            | op::DATE_EQ
            | op::TIMESTAMP_EQ
            | op::TIMESTAMPTZ_EQ
            | op::FLOAT4_EQ
            | op::FLOAT8_EQ
            | op::TEXT_EQ => ComparisonOpClass::Eq,

            op::INT2_NE
            | op::INT4_NE
            | op::INT8_NE
            | op::NUMERIC_NE
            | op::DATE_NE
            | op::TIMESTAMP_NE
            | op::TIMESTAMPTZ_NE
            | op::FLOAT4_NE
            | op::FLOAT8_NE
            | op::TEXT_NE => ComparisonOpClass::NotEq,

            op::INT2_LT
            | op::INT4_LT
            | op::INT8_LT
            | op::NUMERIC_LT
            | op::DATE_LT
            | op::TIMESTAMP_LT
            | op::TIMESTAMPTZ_LT
            | op::FLOAT4_LT
            | op::FLOAT8_LT
            | op::TEXT_LT => ComparisonOpClass::Lt,

            op::INT2_LE
            | op::INT4_LE
            | op::INT8_LE
            | op::NUMERIC_LE
            | op::DATE_LE
            | op::TIMESTAMP_LE
            | op::TIMESTAMPTZ_LE
            | op::FLOAT4_LE
            | op::FLOAT8_LE
            | op::TEXT_LE => ComparisonOpClass::Le,

            op::INT2_GT
            | op::INT4_GT
            | op::INT8_GT
            | op::NUMERIC_GT
            | op::DATE_GT
            | op::TIMESTAMP_GT
            | op::TIMESTAMPTZ_GT
            | op::FLOAT4_GT
            | op::FLOAT8_GT
            | op::TEXT_GT => ComparisonOpClass::Gt,

            op::INT2_GE
            | op::INT4_GE
            | op::INT8_GE
            | op::NUMERIC_GE
            | op::DATE_GE
            | op::TIMESTAMP_GE
            | op::TIMESTAMPTZ_GE
            | op::FLOAT4_GE
            | op::FLOAT8_GE
            | op::TEXT_GE => ComparisonOpClass::Ge,

            _ => return None,
        };
        Some(class)
    }

    /// Capability oracle: pushability of `column op operand` for `type_oid` and
    /// `op_key`. Classifier and translator must agree; NULL operands are handled
    /// at translate time.
    pub fn capability_for(
        &self,
        type_oid: pg_sys::Oid,
        op_key: PgComparisonOp,
    ) -> PredicateCapability {
        // Unrecognized opno => never pushable (also rejects cross-category comparisons).
        let Some(class) = self.op_class(op_key.opno) else {
            return PredicateCapability::Unsupported;
        };

        match PgOid::from(type_oid) {
            PgOid::BuiltIn(
                PgBuiltInOids::INT2OID
                | PgBuiltInOids::INT4OID
                | PgBuiltInOids::INT8OID,
            ) => {
                if self.is_collation_free(op_key) {
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
            ) => self.conservative_pruning_for_eq_and_ordered(class),

            // `numeric` comparison pushdown is gated: the row-level Arrow filter
            // can drop rows under a decimal scale downcast (see
            // `NUMERIC_COMPARISON_PUSHDOWN_ENABLED`). When disabled, `numeric`
            // comparisons fall through to `Unsupported` and stay in the residual
            // qual. `numeric` null-tests remain enabled via `null_test_capability`.
            PgOid::BuiltIn(PgBuiltInOids::NUMERICOID)
                if NUMERIC_COMPARISON_PUSHDOWN_ENABLED =>
            {
                self.conservative_pruning_for_eq_and_ordered(class)
            }

            PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID | PgBuiltInOids::FLOAT8OID)
                if FLOAT_PUSHDOWN_ENABLED =>
            {
                self.conservative_pruning_for_eq_and_ordered(class)
            }

            PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID) => {
                match class {
                    ComparisonOpClass::Eq => {
                        if self.is_deterministic_collation(op_key.inputcollid) {
                            PredicateCapability::ConservativePruning
                        } else {
                            PredicateCapability::Unsupported
                        }
                    }
                    ComparisonOpClass::Lt
                    | ComparisonOpClass::Le
                    | ComparisonOpClass::Gt
                    | ComparisonOpClass::Ge => {
                        if self.is_c_or_posix_collation(op_key.inputcollid) {
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
            // results), the same failure class that disables
            // `NUMERIC_COMPARISON_PUSHDOWN_ENABLED` / `FLOAT_PUSHDOWN_ENABLED`.
            // This hazard is orthogonal to collation: it persists even under
            // `C` / `POSIX`. `IS NULL` / `IS NOT NULL` on `bpchar` is unaffected
            // (see `null_test_capability`): a null test inspects only the null
            // bitmap, never a value, so the padding semantics do not apply.
            //
            // Re-enabling would require a trailing-space normalization invariant
            // applied identically on the storage write path and the pushed
            // literal, after which the `text` collation gate above (deterministic
            // for `=`, C/POSIX for ordered) could be reused for `bpchar`.

            // Every other type (and float when the toggle is off).
            _ => PredicateCapability::Unsupported,
        }
    }

    /// Whether the runtime translator should attempt a native predicate build
    /// for this type/op. Single source of truth: [`Self::capability_for`] — only
    /// `Unsupported` is non-buildable.
    pub fn can_build(&self, type_oid: pg_sys::Oid, op: PgComparisonOp) -> bool {
        !matches!(
            self.capability_for(type_oid, op),
            PredicateCapability::Unsupported
        )
    }

    /// Types whose const literals lack plan-time datum inspection
    /// (ConservativePruning is uncosted).
    pub fn is_value_sensitive_type(&self, type_oid: pg_sys::Oid) -> bool {
        matches!(
            PgOid::from(type_oid),
            PgOid::BuiltIn(
                PgBuiltInOids::NUMERICOID
                    | PgBuiltInOids::DATEOID
                    | PgBuiltInOids::TIMESTAMPOID
                    | PgBuiltInOids::TIMESTAMPTZOID
            )
        )
    }

    /// True for built-in `C` or `POSIX` collation (byte order matches PG ordering).
    pub fn is_c_or_posix_collation(&self, oid: pg_sys::Oid) -> bool {
        oid == pg_sys::C_COLLATION_OID || oid == pg_sys::POSIX_COLLATION_OID
    }

    /// Whether `oid` is deterministic (`pg_collation.collisdeterministic`).
    /// Fail-safe: `InvalidOid` and syscache errors => `false`.
    pub fn is_deterministic_collation(&self, oid: pg_sys::Oid) -> bool {
        // Fail-safe: unresolved collation is never deterministic.
        if oid == pg_sys::Oid::INVALID {
            return false;
        }
        // C/POSIX short-circuit without syscache.
        if self.is_c_or_posix_collation(oid) {
            return true;
        }
        // Syscache lookup; fail-safe to false on error.
        PgTryBuilder::new(move || unsafe {
            pg_sys::get_collation_isdeterministic(oid)
        })
        .catch_others(|_| false)
        .execute()
    }

    /// Returns [`PredicateCapability::ConservativePruning`] for equality and
    /// the four ordered classes; `Unsupported` for `<>`. Shared by the
    /// collation-agnostic temporal category (date / timestamp / timestamptz)
    /// and, when their respective toggles are enabled, the numeric
    /// ([`NUMERIC_COMPARISON_PUSHDOWN_ENABLED`]) and float
    /// ([`FLOAT_PUSHDOWN_ENABLED`]) categories.
    #[inline]
    fn conservative_pruning_for_eq_and_ordered(
        &self,
        class: ComparisonOpClass,
    ) -> PredicateCapability {
        match class {
            ComparisonOpClass::NotEq => PredicateCapability::Unsupported,
            ComparisonOpClass::Eq => PredicateCapability::ConservativePruning,
            _ if class.is_ordered() => PredicateCapability::ConservativePruning,
            // Unreachable given `ComparisonOpClass` is exhaustively
            // Eq/NotEq/ordered, but keeps the function total without an
            // `unreachable!`.
            _ => PredicateCapability::Unsupported,
        }
    }

    /// `(opcollid, inputcollid) == (InvalidOid, InvalidOid)` — integer Exact guard.
    #[inline]
    fn is_collation_free(&self, op_key: PgComparisonOp) -> bool {
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
    /// is safe and enabled independently of `FLOAT_PUSHDOWN_ENABLED`.
    ///
    /// The type allowlist admits the same scan value types as the comparison
    /// policy plus floats. Types outside this set (e.g. `bool`, `bytea`) are
    /// `Unsupported` as a conservative policy choice, not because the
    /// translator cannot construct a column reference for them.
    ///
    /// Only user columns (attno > 0) are pushable; the caller (classifier)
    /// verifies that.
    pub fn null_test_capability(&self, type_oid: pg_sys::Oid) -> PredicateCapability {
        // IS NULL / IS NOT NULL only inspects the null bitmap — there is no
        // value comparison, so NaN ordering/equality divergence does not apply.
        // Float null-tests are safe regardless of FLOAT_PUSHDOWN_ENABLED.
        match PgOid::from(type_oid) {
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
            ) => PredicateCapability::ExactRowFilter,
            _ => PredicateCapability::Unsupported,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::pg_operator_oid as op;
    use super::*;
    use pgrx::pg_sys::Oid;

    fn op_class(opno: pg_sys::Oid) -> Option<ComparisonOpClass> {
        PredicatePushdownPolicy::new().op_class(opno)
    }

    fn supported_predicate(
        type_oid: pg_sys::Oid,
        op_key: PgComparisonOp,
    ) -> PredicateCapability {
        PredicatePushdownPolicy::new().capability_for(type_oid, op_key)
    }

    fn is_c_or_posix_collation(oid: pg_sys::Oid) -> bool {
        PredicatePushdownPolicy::new().is_c_or_posix_collation(oid)
    }

    /// Build a collation-free `(opno, 0, 0)` comparison triple.
    fn triple(opno: u32) -> PgComparisonOp {
        PgComparisonOp {
            opno: Oid::from(opno),
            opfuncid: Oid::INVALID,
            opresulttype: Oid::INVALID,
            opcollid: Oid::INVALID,
            inputcollid: Oid::INVALID,
        }
    }

    /// Build a comparison triple carrying a non-zero collation on both
    /// slots.
    fn triple_with_collation(opno: u32, collid: u32) -> PgComparisonOp {
        PgComparisonOp {
            opno: Oid::from(opno),
            opfuncid: Oid::INVALID,
            opresulttype: Oid::INVALID,
            opcollid: Oid::from(collid),
            inputcollid: Oid::from(collid),
        }
    }

    /// The full `(type, [eq, ne, lt, le, gt, ge])` opno table, mirroring
    /// `pg_operator.dat`. Used to drive both the `op_class` and
    /// `supported_predicate` matrix tests.
    ///
    /// Order within each row is `[Eq, NotEq, Lt, Le, Gt, Ge]`.
    const OPNO_TABLE: &[(pg_sys::Oid, [u32; 6])] = &[
        (
            pg_sys::INT2OID,
            [
                op::INT2_EQ,
                op::INT2_NE,
                op::INT2_LT,
                op::INT2_LE,
                op::INT2_GT,
                op::INT2_GE,
            ],
        ),
        (
            pg_sys::INT4OID,
            [
                op::INT4_EQ,
                op::INT4_NE,
                op::INT4_LT,
                op::INT4_LE,
                op::INT4_GT,
                op::INT4_GE,
            ],
        ),
        (
            pg_sys::INT8OID,
            [
                op::INT8_EQ,
                op::INT8_NE,
                op::INT8_LT,
                op::INT8_LE,
                op::INT8_GT,
                op::INT8_GE,
            ],
        ),
        (
            pg_sys::NUMERICOID,
            [
                op::NUMERIC_EQ,
                op::NUMERIC_NE,
                op::NUMERIC_LT,
                op::NUMERIC_LE,
                op::NUMERIC_GT,
                op::NUMERIC_GE,
            ],
        ),
        (
            pg_sys::DATEOID,
            [
                op::DATE_EQ,
                op::DATE_NE,
                op::DATE_LT,
                op::DATE_LE,
                op::DATE_GT,
                op::DATE_GE,
            ],
        ),
        (
            pg_sys::TIMESTAMPOID,
            [
                op::TIMESTAMP_EQ,
                op::TIMESTAMP_NE,
                op::TIMESTAMP_LT,
                op::TIMESTAMP_LE,
                op::TIMESTAMP_GT,
                op::TIMESTAMP_GE,
            ],
        ),
        (
            pg_sys::TIMESTAMPTZOID,
            [
                op::TIMESTAMPTZ_EQ,
                op::TIMESTAMPTZ_NE,
                op::TIMESTAMPTZ_LT,
                op::TIMESTAMPTZ_LE,
                op::TIMESTAMPTZ_GT,
                op::TIMESTAMPTZ_GE,
            ],
        ),
        (
            pg_sys::FLOAT4OID,
            [
                op::FLOAT4_EQ,
                op::FLOAT4_NE,
                op::FLOAT4_LT,
                op::FLOAT4_LE,
                op::FLOAT4_GT,
                op::FLOAT4_GE,
            ],
        ),
        (
            pg_sys::FLOAT8OID,
            [
                op::FLOAT8_EQ,
                op::FLOAT8_NE,
                op::FLOAT8_LT,
                op::FLOAT8_LE,
                op::FLOAT8_GT,
                op::FLOAT8_GE,
            ],
        ),
        (
            pg_sys::TEXTOID,
            [
                op::TEXT_EQ,
                op::TEXT_NE,
                op::TEXT_LT,
                op::TEXT_LE,
                op::TEXT_GT,
                op::TEXT_GE,
            ],
        ),
    ];

    /// The six op-classes in the same column order as [`OPNO_TABLE`].
    const CLASS_BY_COLUMN: [ComparisonOpClass; 6] = [
        ComparisonOpClass::Eq,
        ComparisonOpClass::NotEq,
        ComparisonOpClass::Lt,
        ComparisonOpClass::Le,
        ComparisonOpClass::Gt,
        ComparisonOpClass::Ge,
    ];

    /// `op_class` must map every opno in the consolidated table to the
    /// expected class — every integer opno from the exact integer policy /
    /// `map_comparison_operator`, plus the new types' opnos.
    #[test]
    fn op_class_maps_every_known_opno() {
        for (type_oid, opnos) in OPNO_TABLE {
            for (col, &opno) in opnos.iter().enumerate() {
                assert_eq!(
                    op_class(Oid::from(opno)),
                    Some(CLASS_BY_COLUMN[col]),
                    "opno {opno} (type {}, column {col}) must map to {:?}",
                    u32::from(*type_oid),
                    CLASS_BY_COLUMN[col],
                );
            }
        }
    }

    /// Distinct opnos must never collide on the same table entry — a
    /// duplicate would point at a copy-paste bug in the consolidated map.
    #[test]
    fn op_class_table_has_no_duplicate_opnos() {
        let mut seen = std::collections::HashSet::new();
        for (_type_oid, opnos) in OPNO_TABLE {
            for &opno in opnos {
                assert!(seen.insert(opno), "opno {opno} appears twice in the table");
            }
        }
    }

    /// `op_class` returns `None` for operators outside the map (e.g.
    /// `oidvector` `<>` opno 558, or a clearly-unused OID).
    #[test]
    fn op_class_rejects_unknown_opnos() {
        assert_eq!(
            op_class(Oid::from(558u32)),
            None,
            "oidvector <> is not mapped"
        );
        assert_eq!(op_class(Oid::INVALID), None, "InvalidOid is not mapped");
        assert_eq!(
            op_class(Oid::from(9_999_999u32)),
            None,
            "unused OID is not mapped"
        );
    }

    /// Integers are `Exact` for all six op-classes under the
    /// collation-free `(0, 0)` triple (preserving the integer
    /// exact integer policy behavior, including `<>`).
    #[test]
    fn supported_predicate_integers_are_exact_under_zero_collation() {
        for &type_oid in &[pg_sys::INT2OID, pg_sys::INT4OID, pg_sys::INT8OID] {
            let row = OPNO_TABLE
                .iter()
                .find(|(t, _)| *t == type_oid)
                .expect("integer type present in table");
            for &opno in &row.1 {
                assert_eq!(
                    supported_predicate(type_oid, triple(opno)),
                    PredicateCapability::ExactRowFilter,
                    "integer type {} opno {opno} must be ExactRowFilter under (0,0)",
                    u32::from(type_oid),
                );
            }
        }
    }

    /// Integer `(0, 0)` comparisons are `Exact`; any non-zero collation on
    /// **either** `opcollid` or `inputcollid` is `Unsupported` (not translatable).
    #[test]
    fn supported_predicate_integer_single_slot_collation_is_unsupported() {
        let collid = Oid::from(50_000u32);

        let int4eq = triple(op::INT4_EQ);
        let input_only = PgComparisonOp {
            inputcollid: collid,
            ..int4eq
        };
        assert_eq!(
            supported_predicate(pg_sys::INT4OID, input_only),
            PredicateCapability::Unsupported,
            "non-zero inputcollid alone must make int4eq Unsupported",
        );
        let opcoll_only = PgComparisonOp {
            opcollid: collid,
            ..int4eq
        };
        assert_eq!(
            supported_predicate(pg_sys::INT4OID, opcoll_only),
            PredicateCapability::Unsupported,
            "non-zero opcollid alone must make int4eq Unsupported",
        );

        let int8lt = triple(op::INT8_LT);
        let input_only = PgComparisonOp {
            inputcollid: collid,
            ..int8lt
        };
        assert_eq!(
            supported_predicate(pg_sys::INT8OID, input_only),
            PredicateCapability::Unsupported,
        );
        let opcoll_only = PgComparisonOp {
            opcollid: collid,
            ..int8lt
        };
        assert_eq!(
            supported_predicate(pg_sys::INT8OID, opcoll_only),
            PredicateCapability::Unsupported,
        );
    }

    /// Both collation slots non-zero (via [`triple_with_collation`]) likewise
    /// rejects integer pushdown.
    #[test]
    fn supported_predicate_integer_with_collation_is_unsupported() {
        assert_eq!(
            supported_predicate(
                pg_sys::INT4OID,
                triple_with_collation(op::INT4_EQ, 50_000),
            ),
            PredicateCapability::Unsupported,
        );
        assert_eq!(
            supported_predicate(
                pg_sys::INT8OID,
                triple_with_collation(op::INT8_LT, 50_000),
            ),
            PredicateCapability::Unsupported,
        );
    }

    /// Preservation set: every int4/int8 comparison from the previous exact
    /// integer set stays `Exact` under `(opcollid, inputcollid) == (0, 0)`.
    /// Broader integer matrix coverage lives in
    /// [`supported_predicate_integers_are_exact_under_zero_collation`].
    const INTEGER_EXACT_PRESERVATION_SET: &[(pg_sys::Oid, u32)] = &[
        (pg_sys::INT4OID, op::INT4_EQ),
        (pg_sys::INT4OID, op::INT4_NE),
        (pg_sys::INT4OID, op::INT4_LT),
        (pg_sys::INT4OID, op::INT4_LE),
        (pg_sys::INT4OID, op::INT4_GT),
        (pg_sys::INT4OID, op::INT4_GE),
        (pg_sys::INT8OID, op::INT8_EQ),
        (pg_sys::INT8OID, op::INT8_NE),
        (pg_sys::INT8OID, op::INT8_LT),
        (pg_sys::INT8OID, op::INT8_LE),
        (pg_sys::INT8OID, op::INT8_GT),
        (pg_sys::INT8OID, op::INT8_GE),
    ];

    #[test]
    fn supported_predicate_integer_exact_preservation_set_is_exact() {
        assert!(!INTEGER_EXACT_PRESERVATION_SET.is_empty());
        for &(type_oid, opno) in INTEGER_EXACT_PRESERVATION_SET {
            assert_eq!(
                supported_predicate(type_oid, triple(opno)),
                PredicateCapability::ExactRowFilter,
                "integer preservation (type {}, opno {opno}) must stay ExactRowFilter under (0,0)",
                u32::from(type_oid),
            );
        }
    }

    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 256,
            ..ProptestConfig::default()
        })]

        /// Any non-zero collation on either slot makes integer comparisons Unsupported.
        #[test]
        fn prop_integer_exact_is_unsupported_when_either_collation_slot_nonzero(
            idx in 0usize..INTEGER_EXACT_PRESERVATION_SET.len(),
            collid in 1u32..=u32::MAX,
            tag_input in any::<bool>(),
        ) {
            let (type_oid, opno) = INTEGER_EXACT_PRESERVATION_SET[idx];
            let exact = triple(opno);
            prop_assert_eq!(
                supported_predicate(type_oid, exact),
                PredicateCapability::ExactRowFilter,
            );

            let mut tagged = exact;
            if tag_input {
                tagged.inputcollid = Oid::from(collid);
            } else {
                tagged.opcollid = Oid::from(collid);
            }
            prop_assert_eq!(
                supported_predicate(type_oid, tagged),
                PredicateCapability::Unsupported,
            );
        }
    }

    /// Capability key uses `(type_oid, opno, opcollid, inputcollid)` only;
    /// `opfuncid` / `opresulttype` are PG diagnostic metadata and must not
    /// change the oracle verdict (classifier/translator share this oracle).
    #[test]
    fn supported_predicate_ignores_diagnostic_fields() {
        let text_eq_c = PgComparisonOp {
            opno: Oid::from(op::TEXT_EQ),
            opfuncid: Oid::INVALID,
            opresulttype: Oid::INVALID,
            opcollid: Oid::INVALID,
            inputcollid: pg_sys::C_COLLATION_OID,
        };

        let baselines: &[(pg_sys::Oid, PgComparisonOp)] = &[
            (pg_sys::INT4OID, triple(op::INT4_EQ)), // Exact
            (pg_sys::INT4OID, triple(op::INT4_LT)), // Exact (different opno)
            (pg_sys::INT4OID, triple_with_collation(op::INT4_EQ, 50_000)), // Unsupported: integer + tagged collation
            (pg_sys::NUMERICOID, triple(op::NUMERIC_EQ)), // numeric: ConservativePruning when enabled, else Unsupported
            (pg_sys::TEXTOID, text_eq_c), // texteq under C, ConservativePruning
            (
                pg_sys::TEXTOID,
                PgComparisonOp {
                    opno: Oid::from(op::TEXT_NE),
                    opfuncid: Oid::INVALID,
                    opresulttype: Oid::INVALID,
                    opcollid: Oid::INVALID,
                    inputcollid: pg_sys::C_COLLATION_OID,
                },
            ), // text <>, Unsupported
        ];

        for &(type_oid, baseline) in baselines {
            let expected = supported_predicate(type_oid, baseline);

            let planner_like = PgComparisonOp {
                opfuncid: Oid::from(65u32),
                opresulttype: pg_sys::BOOLOID,
                ..baseline
            };
            assert_eq!(
                supported_predicate(type_oid, planner_like),
                expected,
                "planner-like diagnostic OIDs must not change verdict for type {} opno {}",
                u32::from(type_oid),
                u32::from(baseline.opno),
            );

            let arbitrary = PgComparisonOp {
                opfuncid: Oid::from(9_999u32),
                opresulttype: Oid::from(9_999u32),
                ..baseline
            };
            assert_eq!(
                supported_predicate(type_oid, arbitrary),
                expected,
                "arbitrary diagnostic OIDs must not change verdict for type {} opno {}",
                u32::from(type_oid),
                u32::from(baseline.opno),
            );
        }
    }

    /// The collation-agnostic temporal types (date / timestamp /
    /// timestamptz) are `ConservativePruning` for `Eq` and the
    /// four ordered classes, and `Unsupported` for `<>`.
    /// Numeric comparison follows `NUMERIC_COMPARISON_PUSHDOWN_ENABLED`
    /// (all-`Unsupported` when disabled), and float types follow
    /// `FLOAT_PUSHDOWN_ENABLED` (all-`Unsupported` when disabled).
    #[test]
    fn supported_predicate_numeric_temporal_float_matrix() {
        let temporal = [
            pg_sys::DATEOID,
            pg_sys::TIMESTAMPOID,
            pg_sys::TIMESTAMPTZOID,
        ];
        for &type_oid in &temporal {
            let row = OPNO_TABLE
                .iter()
                .find(|(t, _)| *t == type_oid)
                .expect("type present in table");
            for (col, &opno) in row.1.iter().enumerate() {
                let class = CLASS_BY_COLUMN[col];
                let expected = if class == ComparisonOpClass::NotEq {
                    PredicateCapability::Unsupported
                } else {
                    PredicateCapability::ConservativePruning
                };
                assert_eq!(
                    supported_predicate(type_oid, triple(opno)),
                    expected,
                    "type {} class {class:?} (opno {opno}) mismatch",
                    u32::from(type_oid),
                );
            }
        }

        // Numeric comparison respects the NUMERIC_COMPARISON_PUSHDOWN_ENABLED toggle.
        let numeric_row = OPNO_TABLE
            .iter()
            .find(|(t, _)| *t == pg_sys::NUMERICOID)
            .expect("numeric present in table");
        for (col, &opno) in numeric_row.1.iter().enumerate() {
            let expected = if super::super::NUMERIC_COMPARISON_PUSHDOWN_ENABLED {
                let class = CLASS_BY_COLUMN[col];
                if class == ComparisonOpClass::NotEq {
                    PredicateCapability::Unsupported
                } else {
                    PredicateCapability::ConservativePruning
                }
            } else {
                PredicateCapability::Unsupported
            };
            assert_eq!(
                supported_predicate(pg_sys::NUMERICOID, triple(opno)),
                expected,
                "numeric opno {opno} mismatch (NUMERIC_COMPARISON_PUSHDOWN_ENABLED={})",
                super::super::NUMERIC_COMPARISON_PUSHDOWN_ENABLED,
            );
        }

        // Float types respect the FLOAT_PUSHDOWN_ENABLED toggle.
        let float_types = [pg_sys::FLOAT4OID, pg_sys::FLOAT8OID];
        for &type_oid in &float_types {
            let row = OPNO_TABLE
                .iter()
                .find(|(t, _)| *t == type_oid)
                .expect("type present in table");
            for (_col, &opno) in row.1.iter().enumerate() {
                let expected = if super::super::FLOAT_PUSHDOWN_ENABLED {
                    let class = CLASS_BY_COLUMN[_col];
                    if class == ComparisonOpClass::NotEq {
                        PredicateCapability::Unsupported
                    } else {
                        PredicateCapability::ConservativePruning
                    }
                } else {
                    PredicateCapability::Unsupported
                };
                assert_eq!(
                    supported_predicate(type_oid, triple(opno)),
                    expected,
                    "float type {} opno {opno} mismatch (FLOAT_PUSHDOWN_ENABLED={})",
                    u32::from(type_oid),
                    super::super::FLOAT_PUSHDOWN_ENABLED,
                );
            }
        }
    }

    /// Only the integer set is ever `Exact`; the newly-supported value
    /// types are never `Exact` for any op-class.
    #[test]
    fn supported_predicate_only_integers_are_exact() {
        for (type_oid, opnos) in OPNO_TABLE {
            let is_integer = matches!(
                *type_oid,
                pg_sys::INT2OID | pg_sys::INT4OID | pg_sys::INT8OID
            );
            if is_integer {
                continue;
            }
            for &opno in opnos {
                assert_ne!(
                    supported_predicate(*type_oid, triple(opno)),
                    PredicateCapability::ExactRowFilter,
                    "non-integer type {} opno {opno} must not be ExactRowFilter",
                    u32::from(*type_oid),
                );
            }
        }
    }

    /// An unknown type OID is always `Unsupported`, regardless of the
    /// operator class (here `int4eq`, a recognized opno).
    #[test]
    fn supported_predicate_unknown_type_is_unsupported() {
        // BOOLOID has no comparison entry in our op_class map under these
        // opnos, but even pairing it with a recognized integer opno must
        // be Unsupported because the type is outside every category.
        assert_eq!(
            supported_predicate(pg_sys::BOOLOID, triple(op::INT4_EQ)),
            PredicateCapability::Unsupported,
        );
        // A clearly-unrelated type (bytea) is likewise Unsupported.
        assert_eq!(
            supported_predicate(pg_sys::BYTEAOID, triple(op::INT4_EQ)),
            PredicateCapability::Unsupported,
        );
    }

    /// An unrecognized operator makes any clause `Unsupported`, even for
    /// an otherwise-supported type.
    #[test]
    fn supported_predicate_unknown_operator_is_unsupported() {
        assert_eq!(
            supported_predicate(pg_sys::INT4OID, triple(558)),
            PredicateCapability::Unsupported,
            "unmapped opno 558 must be Unsupported even for int4",
        );
    }

    /// Text under C/POSIX is host-testable because those collations
    /// short-circuit ahead of the syscache: ordered text under C/POSIX
    /// is `ConservativePruning`, while text `<>` is always `Unsupported`. Text under
    /// the unresolved `InvalidOid` collation is `Unsupported` (fail-safe)
    /// for both `=` and ordered classes.
    #[test]
    fn supported_predicate_text_collation_known_cases() {
        let c = pg_sys::C_COLLATION_OID;
        let posix = pg_sys::POSIX_COLLATION_OID;

        let with = |opno: u32, collid: pg_sys::Oid| PgComparisonOp {
            opno: Oid::from(opno),
            opfuncid: Oid::INVALID,
            opresulttype: Oid::INVALID,
            opcollid: Oid::INVALID,
            inputcollid: collid,
        };

        // Ordered text under C / POSIX → ConservativePruning (no syscache needed).
        for &collid in &[c, posix] {
            for &opno in &[op::TEXT_LT, op::TEXT_LE, op::TEXT_GT, op::TEXT_GE] {
                assert_eq!(
                    supported_predicate(pg_sys::TEXTOID, with(opno, collid)),
                    PredicateCapability::ConservativePruning,
                    "ordered text opno {opno} under C/POSIX must be ConservativePruning",
                );
            }
            // text `=` under C/POSIX → deterministic short-circuit → ConservativePruning.
            assert_eq!(
                supported_predicate(pg_sys::TEXTOID, with(op::TEXT_EQ, collid)),
                PredicateCapability::ConservativePruning,
            );
        }

        // text `<>` is never pushable.
        assert_eq!(
            supported_predicate(pg_sys::TEXTOID, with(op::TEXT_NE, c)),
            PredicateCapability::Unsupported,
        );

        // Unresolved collation (InvalidOid) → fail-safe Unsupported for
        // both `=` and ordered classes.
        assert_eq!(
            supported_predicate(
                pg_sys::TEXTOID,
                with(op::TEXT_EQ, pg_sys::Oid::INVALID)
            ),
            PredicateCapability::Unsupported,
        );
        assert_eq!(
            supported_predicate(
                pg_sys::TEXTOID,
                with(op::TEXT_LT, pg_sys::Oid::INVALID)
            ),
            PredicateCapability::Unsupported,
        );
    }

    /// `is_c_or_posix_collation` is true *only* for the built-in C
    /// (950) and POSIX (951) collation OIDs.
    #[test]
    fn is_c_or_posix_collation_only_for_c_and_posix() {
        assert!(is_c_or_posix_collation(pg_sys::C_COLLATION_OID));
        assert!(is_c_or_posix_collation(pg_sys::POSIX_COLLATION_OID));
        assert_eq!(u32::from(pg_sys::C_COLLATION_OID), 950);
        assert_eq!(u32::from(pg_sys::POSIX_COLLATION_OID), 951);

        // Everything else is false: InvalidOid, the default collation
        // (100), and an arbitrary user collation OID.
        assert!(!is_c_or_posix_collation(pg_sys::Oid::INVALID));
        assert!(!is_c_or_posix_collation(pg_sys::DEFAULT_COLLATION_OID));
        assert!(!is_c_or_posix_collation(Oid::from(50_000u32)));
    }

    /// `null_test_capability` returns `ExactRowFilter` for all admitted scan
    /// value types including float (null tests don't involve value comparison).
    #[test]
    fn null_test_capability_admits_all_supported_types_including_float() {
        let policy = PredicatePushdownPolicy::new();
        let supported = [
            pg_sys::INT2OID,
            pg_sys::INT4OID,
            pg_sys::INT8OID,
            pg_sys::NUMERICOID,
            pg_sys::DATEOID,
            pg_sys::TIMESTAMPOID,
            pg_sys::TIMESTAMPTZOID,
            pg_sys::TEXTOID,
            pg_sys::FLOAT4OID,
            pg_sys::FLOAT8OID,
        ];
        for &type_oid in &supported {
            assert_eq!(
                policy.null_test_capability(type_oid),
                PredicateCapability::ExactRowFilter,
                "null_test_capability must be ExactRowFilter for type {}",
                u32::from(type_oid),
            );
        }
    }

    /// Float null-test capability is independent of `FLOAT_PUSHDOWN_ENABLED`.
    #[test]
    fn null_test_capability_float_is_independent_of_comparison_toggle() {
        let policy = PredicatePushdownPolicy::new();
        // Even with FLOAT_PUSHDOWN_ENABLED = false, float null tests are safe.
        assert_eq!(
            policy.null_test_capability(pg_sys::FLOAT4OID),
            PredicateCapability::ExactRowFilter,
        );
        assert_eq!(
            policy.null_test_capability(pg_sys::FLOAT8OID),
            PredicateCapability::ExactRowFilter,
        );
    }

    /// `null_test_capability` rejects types outside the admitted set.
    #[test]
    fn null_test_capability_rejects_unsupported_types() {
        let policy = PredicatePushdownPolicy::new();
        let unsupported =
            [pg_sys::BOOLOID, pg_sys::BYTEAOID, Oid::from(9_999_999u32)];
        for &type_oid in &unsupported {
            assert_eq!(
                policy.null_test_capability(type_oid),
                PredicateCapability::Unsupported,
                "null_test_capability must be Unsupported for type {}",
                u32::from(type_oid),
            );
        }
    }
}
