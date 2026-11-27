//! Provider-neutral Iceberg predicate capability rules.

use lagodb_core::expr::PgComparisonKind;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::predicate) enum PredicateCapability {
    Exact,
    Conservative,
}

/// Logical scalar kinds understood by Iceberg predicate construction.
/// PostgreSQL OIDs and Arrow types are translated by their respective
/// adapters before entering these capability rules.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::predicate) enum PredicateValueKind {
    Boolean,
    Integer,
    Long,
    Date,
    Timestamp,
    Timestamptz,
    Decimal,
    Float,
    String,
    Binary,
    Time,
}

/// Pure Iceberg predicate capabilities shared by relation planning and
/// columnar runtime pruning.
///
/// Set membership stays out of the Arrow row filter. iceberg-lite's current
/// row filter evaluates one full-batch equality and OR per literal, giving
/// `O(batch_rows * literals)` work and intermediate Boolean arrays. `= ANY` is
/// therefore a Conservative file-pruning predicate, and PostgreSQL keeps the
/// exact qual, for which it can select hashed ScalarArrayOp execution. A set
/// prices only the files its partition values prune, as its equalities would;
/// the rows of every selected file still reach PostgreSQL, which path costing
/// follows from the filter's evaluation rather than from its pruning.
pub(in crate::predicate) struct PredicateCapabilities;

impl PredicateCapabilities {
    // TODO: reconcile Exact with the missing-column row-filter bug inherited
    // by iceberg-lite:
    // https://github.com/apache/iceberg-rust/issues/3355
    // PredicateConverter::{less_than, less_than_or_eq} use build_always_true()
    // when the field ID is absent from the Parquet file. For an optional column
    // added without an initial-default or identity partition value, projection
    // yields NULL, so SQL `<` / `<=` must reject those rows. Declaring the filter
    // Exact and removing the residual instead exposes false-positive rows.
    // Physically stored NULLs do not use this missing-field branch.
    //
    // The branch is type-independent: affected ordered kinds admitted here are
    // Integer/Long (int2/int4/int8), Decimal (numeric), String (text/varchar),
    // Binary (bytea, including fixed binary, and uuid), and Date/Timestamp/
    // Timestamptz. PostgreSQL temporal and fixed-binary comparisons already
    // keep Conservative residuals; those residuals remove these false
    // positives. Float `<` / `<=` are Exact but unaffected: their not-NaN test
    // carries a NotNull guard, which the reader evaluates to false for a
    // missing column. Boolean admits only equality/inequality; Time
    // comparisons are off.
    // This defect therefore predates the addition of binary ordered pushdown.
    //
    // Any capability correction must account for operand mirroring and native
    // negation: `k > col` becomes `col < k`, and `NOT(col > k)` becomes `col <= k`
    // (likewise `>=` becomes `<` under NOT). Checking only the original operator
    // or excluding only Binary would leave the same correctness hole.
    // Related PR https://github.com/apache/iceberg-rust/pull/3302 handles missing
    // fields with defaults/identity partition values; its scope explicitly
    // leaves the no-value NULL case unchanged. Track #3355 separately.
    pub(in crate::predicate) const fn comparison_capability(
        value_kind: PredicateValueKind,
        operator: PgComparisonKind,
    ) -> Option<PredicateCapability> {
        match value_kind {
            PredicateValueKind::Boolean => match operator {
                PgComparisonKind::Equal | PgComparisonKind::NotEqual => {
                    Some(PredicateCapability::Exact)
                }
                _ => None,
            },
            // Iceberg and Arrow order floats by IEEE totalOrder, while
            // PostgreSQL treats -0 and 0 as equal and every NaN as the
            // greatest value. `semantics::float` rewrites each comparison so
            // that it accepts exactly PostgreSQL's rows.
            PredicateValueKind::Float => Some(PredicateCapability::Exact),
            PredicateValueKind::Time => None,
            PredicateValueKind::Integer
            | PredicateValueKind::Long
            | PredicateValueKind::Date
            | PredicateValueKind::Timestamp
            | PredicateValueKind::Timestamptz
            | PredicateValueKind::Decimal
            | PredicateValueKind::String
            | PredicateValueKind::Binary => Some(PredicateCapability::Exact),
        }
    }

    pub(in crate::predicate) const fn supports_boolean_test(
        value_kind: PredicateValueKind,
    ) -> bool {
        matches!(value_kind, PredicateValueKind::Boolean)
    }

    pub(in crate::predicate) const fn supports_starts_with(
        value_kind: PredicateValueKind,
    ) -> bool {
        matches!(value_kind, PredicateValueKind::String)
    }

    pub(in crate::predicate) const fn supports_nan_test(
        value_kind: PredicateValueKind,
    ) -> bool {
        matches!(value_kind, PredicateValueKind::Float)
    }

    /// Iceberg null predicates inspect only validity, so every logical kind
    /// represented by the provider schema has exact null-test semantics.
    pub(in crate::predicate) const fn supports_null_test(
        value_kind: PredicateValueKind,
    ) -> bool {
        match value_kind {
            PredicateValueKind::Boolean
            | PredicateValueKind::Integer
            | PredicateValueKind::Long
            | PredicateValueKind::Date
            | PredicateValueKind::Timestamp
            | PredicateValueKind::Timestamptz
            | PredicateValueKind::Decimal
            | PredicateValueKind::Float
            | PredicateValueKind::String
            | PredicateValueKind::Binary
            | PredicateValueKind::Time => true,
        }
    }
}
