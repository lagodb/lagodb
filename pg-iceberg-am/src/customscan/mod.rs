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
/// has a fixed scale; when the literal's scale exceeds the column scale the
/// row filter casts/rounds the literal to the column scale, which moves an
/// ordered-comparison boundary and can drop rows PostgreSQL would keep. Because
/// `ConservativePruning` predicates still enter the row filter, the residual
/// `plan.qual` cannot recover those rows — the missing row never reaches it —
/// so this is a silent false negative (wrong results), the same failure class
/// that disables [`FLOAT_PUSHDOWN_ENABLED`].
///
/// Re-enable once `iceberg-lite` exposes a pruning-only filter API distinct
/// from the row-level filter, so `ConservativePruning` predicates can drive
/// manifest/row-group/page pruning without row-level filtering. A per-value
/// scale-aware guard is intentionally avoided: it would couple the translator
/// to Iceberg decimal scale across const/param/rescan paths and still not
/// generalize to other `ConservativePruning` types.
pub(crate) const NUMERIC_COMPARISON_PUSHDOWN_ENABLED: bool = false;

/// Register the Iceberg provider once from `_PG_init`.
pub fn register() {
    pg_lakebase_core::customscan::provider::register_provider::<
        IcebergCustomScanProvider,
    >();
}
