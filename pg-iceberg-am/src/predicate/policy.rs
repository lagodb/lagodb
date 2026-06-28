//! Shared Iceberg capability policy and PostgreSQL operator mapping.

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
            // results), the same failure class as numeric/float comparisons.
            // This hazard is orthogonal to collation: it persists even under
            // `C` / `POSIX`. `IS NULL` / `IS NOT NULL` on `bpchar` is unaffected
            // (see `null_test_capability`): a null test inspects only the null
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
                PgBuiltInOids::DATEOID
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
    /// collation-agnostic temporal category (date / timestamp / timestamptz).
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
    /// remains safe even though float comparisons are unsupported.
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
        // Float null-tests are safe even though float comparisons are unsupported.
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

// =============================================================================
// Shared opno-table test fixture: single source of truth for the
// `(type, [eq, ne, lt, le, gt, ge])` comparison-operator verdict table, used by
// both the host `#[test]` policy suite below and the backend `#[pg_test]`
// capability matrix (`customscan/pg_test/predicate/capability_matrix.rs`).
//
// Built directly from the production `pg_operator_oid` constants so the test
// table can never silently drift from the OIDs the policy actually matches on.
// Kept crate-internal (`pub(crate)`); only the test/`pg_test` builds compile it.
// =============================================================================
#[cfg(any(test, feature = "pg_test"))]
pub(crate) mod test_opno_table {
    use super::ComparisonOpClass;
    use super::pg_operator_oid as op;
    use pgrx::pg_sys;

    // Per-type comparison opno rows in column order `[Eq, NotEq, Lt, Le, Gt, Ge]`.
    pub(crate) const INT2: [u32; 6] = [
        op::INT2_EQ,
        op::INT2_NE,
        op::INT2_LT,
        op::INT2_LE,
        op::INT2_GT,
        op::INT2_GE,
    ];
    pub(crate) const INT4: [u32; 6] = [
        op::INT4_EQ,
        op::INT4_NE,
        op::INT4_LT,
        op::INT4_LE,
        op::INT4_GT,
        op::INT4_GE,
    ];
    pub(crate) const INT8: [u32; 6] = [
        op::INT8_EQ,
        op::INT8_NE,
        op::INT8_LT,
        op::INT8_LE,
        op::INT8_GT,
        op::INT8_GE,
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
    pub(crate) const TIMESTAMPTZ: [u32; 6] = [
        op::TIMESTAMPTZ_EQ,
        op::TIMESTAMPTZ_NE,
        op::TIMESTAMPTZ_LT,
        op::TIMESTAMPTZ_LE,
        op::TIMESTAMPTZ_GT,
        op::TIMESTAMPTZ_GE,
    ];
    pub(crate) const TEXT: [u32; 6] = [
        op::TEXT_EQ,
        op::TEXT_NE,
        op::TEXT_LT,
        op::TEXT_LE,
        op::TEXT_GT,
        op::TEXT_GE,
    ];

    /// The six op-classes in the same column order as the per-type rows.
    pub(crate) const CLASS_BY_COLUMN: [ComparisonOpClass; 6] = [
        ComparisonOpClass::Eq,
        ComparisonOpClass::NotEq,
        ComparisonOpClass::Lt,
        ComparisonOpClass::Le,
        ComparisonOpClass::Gt,
        ComparisonOpClass::Ge,
    ];

    /// The full `(type, [eq, ne, lt, le, gt, ge])` opno table, mirroring
    /// `pg_operator.dat`. Order within each row is `[Eq, NotEq, Lt, Le, Gt, Ge]`.
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

// =============================================================================
// Host tests: pure policy logic that needs no PG backend.
//
// Only `op_class` (opno -> class mapping), `is_c_or_posix_collation` (constant
// comparison), and `null_test_capability` (type-allowlist match) are exercised
// here — none of them touch `pg_sys`. The value-comparison matrix
// (`capability_for` / `can_build`) references `get_collation_isdeterministic`
// in its text arm, so it cannot run in a host `#[test]` and lives in the
// backend suite (`customscan/pg_test/predicate/capability_matrix.rs`). See
// `docs/testing.md`.
// =============================================================================
#[cfg(test)]
mod tests {
    use super::test_opno_table::{CLASS_BY_COLUMN, opno_table};
    use super::*;
    use pgrx::pg_sys::Oid;

    fn op_class(opno: pg_sys::Oid) -> Option<ComparisonOpClass> {
        PredicatePushdownPolicy::new().op_class(opno)
    }

    fn is_c_or_posix_collation(oid: pg_sys::Oid) -> bool {
        PredicatePushdownPolicy::new().is_c_or_posix_collation(oid)
    }

    /// `op_class` must map every opno in the consolidated table to the
    /// expected class.
    #[test]
    fn op_class_maps_every_known_opno() {
        for (type_oid, opnos) in opno_table() {
            for (col, &opno) in opnos.iter().enumerate() {
                assert_eq!(
                    op_class(Oid::from(opno)),
                    Some(CLASS_BY_COLUMN[col]),
                    "opno {opno} (type {}, column {col}) must map to {:?}",
                    u32::from(type_oid),
                    CLASS_BY_COLUMN[col],
                );
            }
        }
    }

    /// Distinct opnos must never collide on the same table entry.
    #[test]
    fn op_class_table_has_no_duplicate_opnos() {
        let mut seen = std::collections::HashSet::new();
        for (_type_oid, opnos) in opno_table() {
            for opno in opnos {
                assert!(seen.insert(opno), "opno {opno} appears twice in the table");
            }
        }
    }

    /// `op_class` returns `None` for operators outside the map.
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

    /// `is_c_or_posix_collation` is true *only* for the built-in C (950) and
    /// POSIX (951) collation OIDs.
    #[test]
    fn is_c_or_posix_collation_only_for_c_and_posix() {
        assert!(is_c_or_posix_collation(pg_sys::C_COLLATION_OID));
        assert!(is_c_or_posix_collation(pg_sys::POSIX_COLLATION_OID));
        assert_eq!(u32::from(pg_sys::C_COLLATION_OID), 950);
        assert_eq!(u32::from(pg_sys::POSIX_COLLATION_OID), 951);

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

    /// Float null tests remain supported while float comparisons are not.
    #[test]
    fn null_test_capability_float_is_independent_of_comparison_toggle() {
        let policy = PredicatePushdownPolicy::new();
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
