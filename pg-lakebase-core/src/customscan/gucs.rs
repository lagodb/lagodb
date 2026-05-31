//! `pg_lakebase.customscan_mode` GUC (`off` / `auto` / `force`). Downstream
//! extensions register via [`crate::customscan::init`]. `force` only biases
//! cost on legal CustomPaths for tests; it does not relax gates or semantics.

use std::sync::OnceLock;

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting, PostgresGucEnum};

static CUSTOMSCAN_MODE: GucSetting<CustomScanMode> =
    GucSetting::<CustomScanMode>::new(CustomScanMode::Auto);

static INIT: OnceLock<()> = OnceLock::new();

/// Path-emission mode for the framework hook. See module docs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PostgresGucEnum)]
pub enum CustomScanMode {
    /// Suppress every CustomPath the framework might emit. The
    /// framework hook short-circuits before any provider is consulted
    /// and PG's default scan paths are used.
    Off,
    /// Default: emit CustomPaths and let the planner decide based on
    /// cost.
    Auto,
    /// Bias cost on CustomPaths the framework already deems legal so
    /// the planner picks them deterministically. Does not relax any
    /// path-stage gate or the pushable-clauses requirement in
    /// `create_path`.
    Force,
}

pub(crate) fn init() {
    INIT.get_or_init(|| {
        GucRegistry::define_enum_guc(
            c"pg_lakebase.customscan_mode",
            c"Path-emission mode for the pg-lakebase CustomScan framework",
            c"\"off\" suppresses every CustomPath; the framework hook short-circuits \
              before any provider is consulted and PG's default scan paths are used. \
              \"auto\" (default) emits CustomPaths and lets the planner pick based on cost. \
              \"force\" biases cost on CustomPaths the framework already deems legal so the \
              planner picks them deterministically; \"force\" is intended for regression / \
              debug only, does not relax any path-stage gate, does not push additional \
              clauses, and does not change SQL semantics.",
            &CUSTOMSCAN_MODE,
            GucContext::Userset,
            GucFlags::default(),
        );
    });
}

/// Returns `true` when the CustomScan framework should emit
/// CustomPaths at all.
///
/// The framework hook short-circuits before any provider is consulted
/// when this returns `false`. Equivalent to "the user has not set
/// `customscan_mode = off`".
#[inline]
pub(crate) fn enabled() -> bool {
    !matches!(CUSTOMSCAN_MODE.get(), CustomScanMode::Off)
}

/// Returns `true` when `pg_lakebase.customscan_mode = force` is set.
/// Used by [`crate::customscan::builder::emit_custom_path`] to
/// override the final `(startup_cost, total_cost)` so the planner
/// picks the CustomPath even when the baseline estimate would not
/// dominate SeqScan.
#[inline]
pub(crate) fn force_mode() -> bool {
    matches!(CUSTOMSCAN_MODE.get(), CustomScanMode::Force)
}
