//! Iceberg CustomScan provider and predicate pushdown implementation.

mod predicate_classifier;
mod predicate_pushdown_policy;
pub(crate) mod predicate_translator;
mod provider_private_data;
mod provider_projection;
mod provider_scan_state;

pub mod provider;

#[cfg(feature = "pg_test")]
mod pg_test;

pub use predicate_classifier::IcebergPredicateClassifier;
pub use predicate_pushdown_policy::{
    ComparisonOpClass, PredicateCapability, PredicatePushdownPolicy,
};
pub use predicate_translator::{
    IcebergPredicateTranslator, IcebergScalar, IcebergTranslationError, ScalarKind,
};
pub use provider::{IcebergCustomScanProvider, IcebergPrivateData, IcebergScanState};

/// Whether `float4` / `float8` *comparison* predicates are eligible for
/// pushdown. Single source of truth shared by [`PredicatePushdownPolicy`]
/// (classifier) and [`IcebergPredicateTranslator`] (executor). `IS NULL` /
/// `IS NOT NULL` on float columns is unaffected (see
/// [`PredicatePushdownPolicy::null_test_capability`]): a null test inspects
/// only the null bitmap, never a value, so the NaN hazard below does not apply.
///
/// Disabled: Arrow's row-level filter uses IEEE 754 comparison semantics where
/// `NaN != NaN` and `NaN` is unordered, but PostgreSQL defines `NaN = NaN` as
/// true and sorts NaN above infinity. Under `ConservativePruning` the row
/// filter can drop rows that PG's residual qual would have kept (false
/// negatives), violating the "no false negatives" contract. Re-enable once the
/// scan path separates manifest/row-group pruning from row-level filtering, or
/// wraps float comparisons in PG-compatible NaN semantics.
///
/// TODO(float pushdown): if we ever pursue maximum-efficiency row-level
/// pushdown for floats, the pushed predicate must implement PostgreSQL's NaN
/// ordering/equality exactly (`NaN = NaN`, NaN sorts above infinity). A
/// pruning-only path may be less strict, but it still must be conservative:
/// keep the file/row group/page whenever IEEE/Arrow semantics cannot prove
/// that PostgreSQL would reject every row.
pub(crate) const FLOAT_PUSHDOWN_ENABLED: bool = false;

/// Whether `numeric` comparison predicates (`=` / `<` / `<=` / `>` / `>=`)
/// are eligible for pushdown. Single source of truth shared by
/// [`PredicatePushdownPolicy`] (classifier) and
/// [`IcebergPredicateTranslator`] (executor). `IS NULL` / `IS NOT NULL` on
/// `numeric` columns is unaffected (see [`PredicatePushdownPolicy::null_test_capability`]):
/// a null test inspects only the null bitmap, never a value, so the scale
/// hazard below does not apply.
///
/// Disabled: the only filter API this provider has (`with_filter` →
/// `iceberg-lite`'s `RecordBatchReaderBuilder::with_row_filter`) applies the
/// predicate as a *row-level* Arrow filter, not pruning-only. PostgreSQL's
/// `numeric` is arbitrary precision, while an Iceberg `decimal(P, S)` column
/// has fixed precision and scale. A pushed comparison is exact only when the
/// literal can be represented at the column's `(P, S)` with operator-aware
/// boundary handling. The current classifier sees only the PG type OID, and the
/// translator sees only the runtime `Datum`; neither has the bound Iceberg
/// decimal scale. Today `iceberg-lite` rejects decimal scale conversion during
/// predicate binding; a naive downscale/cast would move ordered-comparison
/// boundaries and could drop rows PostgreSQL would keep. Because
/// `ConservativePruning` predicates still enter the row filter, the residual
/// `plan.qual` cannot recover such rows — the missing row never reaches it —
/// so this is a silent false negative (wrong results), the same failure class
/// that disables [`FLOAT_PUSHDOWN_ENABLED`].
///
/// Re-enable once either (1) `iceberg-lite` exposes a pruning-only filter API
/// distinct from the row-level filter, or (2) the pushdown path carries bound
/// Iceberg decimal `(P, S)` into both const and param/rescan translation and
/// applies the same representability rules as the write/read decimal codecs.
///
/// TODO(decimal pushdown): for maximum-efficiency row-level pushdown, do not
/// rely on the generic Parquet/Arrow decimal row filter to reinterpret
/// PostgreSQL `numeric`. Translate the PG predicate into an equivalent Iceberg
/// `decimal(P, S)` predicate before handing it to the reader. That translation
/// must be operator-aware: exact in-grid literals may be pushed directly,
/// off-grid ordered literals need boundary rewrites (for example `x < 12.345`
/// on `decimal(_, 2)` becomes `x <= 12.34`), `=` may become `AlwaysFalse`, and
/// `<>` may become `IS NOT NULL` to preserve SQL NULL semantics. A pruning-only
/// future path can use simpler no-false-negative rules, but any row-level
/// filter that drops rows must be fully equivalent to PostgreSQL comparison
/// semantics on the bound Iceberg decimal domain.
pub(crate) const NUMERIC_COMPARISON_PUSHDOWN_ENABLED: bool = false;

/// Register the Iceberg provider once from `_PG_init`.
pub fn register() {
    pg_lakebase_core::customscan::provider::register_provider::<
        IcebergCustomScanProvider,
    >();
}
