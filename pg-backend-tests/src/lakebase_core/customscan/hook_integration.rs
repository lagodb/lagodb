//! Hook-integration tests for the generic CustomScan framework.
//!
//! Unlike `hook.rs`, these tests exercise the real `set_rel_pathlist_hook`
//! router through SQL planning, using a dummy provider registered from `_PG_init`.
//! That provider is installed into the process-global registry for the lifetime
//! of the `pg-backend-tests` extension. Keep its relation-name prefixes
//! unique to this module so unrelated tests do not accidentally match it.

use std::ffi::CStr;
use std::sync::OnceLock;

use pg_lakebase_core::customscan::codec::{PrivateDataReader, PrivateDataWriter};
use pg_lakebase_core::customscan::custom_private::CustomScanPrivate;
use pg_lakebase_core::customscan::provider::{
    BeginContext, CreateStateContext, CustomPathBuilder, CustomPathPlan,
    CustomScanError, EndContext, LakebaseCustomScanProvider, NextSlotContext,
    PathVariant, PathVariantKind, PlanTranslateContext, ReScanContext,
    RelPathContext, register_provider,
};
use pg_lakebase_core::expr::predicate::{PlanPredicate, PlanScalar};
use pg_lakebase_core::expr::split::{
    PushdownContract, PushdownCosting, QualPushdownDecision,
};
use pgrx::pg_sys;

const PROVIDER_NAME: &CStr = c"hook-integration-test-provider";
const PLAIN_REL_PREFIX: &str = "hook_plain_";
const JOIN_REL_PREFIX: &str = "hook_join_";
const INT4EQ_OPNO: u32 = 96;

pub(crate) fn install_hook_integration_provider() {
    static INIT: OnceLock<()> = OnceLock::new();

    INIT.get_or_init(|| {
        register_provider::<HookIntegrationProvider>();
        pg_lakebase_core::customscan::init();
    });
}

struct HookIntegrationPrivate;

impl CustomScanPrivate for HookIntegrationPrivate {
    fn encode(&self, _writer: &mut PrivateDataWriter) -> Result<(), CustomScanError> {
        Ok(())
    }

    fn decode(_reader: &mut PrivateDataReader<'_>) -> Result<Self, CustomScanError> {
        Ok(Self)
    }
}

struct HookIntegrationState;

struct HookIntegrationProvider;

impl LakebaseCustomScanProvider for HookIntegrationProvider {
    const NAME: &'static CStr = PROVIDER_NAME;
    type PrivateData = HookIntegrationPrivate;
    type State = HookIntegrationState;

    fn supports_relation(ctx: &RelPathContext) -> bool {
        relation_name(ctx.rel_oid()).is_some_and(|name| {
            name.starts_with(PLAIN_REL_PREFIX) || name.starts_with(JOIN_REL_PREFIX)
        })
    }

    fn classify_predicate(
        _ctx: &PlanTranslateContext,
        predicate: &PlanPredicate,
    ) -> QualPushdownDecision {
        if is_int4_eq_comparison(predicate) {
            QualPushdownDecision::Pushable {
                contract: PushdownContract::ExactRowFilter,
                costing: PushdownCosting::CostedPruning,
            }
        } else {
            QualPushdownDecision::Unsupported
        }
    }

    fn create_path(
        ctx: &RelPathContext,
        variant: &PathVariant<'_>,
        builder: CustomPathBuilder<Self>,
    ) -> Option<CustomPathPlan<Self>> {
        let rel_name = relation_name(ctx.rel_oid())?;
        if !variant.pushdown.has_pushed_predicates() {
            return None;
        }

        let wants_variant = if rel_name.starts_with(PLAIN_REL_PREFIX) {
            variant.kind == PathVariantKind::Plain
        } else if rel_name.starts_with(JOIN_REL_PREFIX) {
            variant.kind == PathVariantKind::JoinParameterized
        } else {
            false
        };

        if !wants_variant {
            return None;
        }

        Some(builder.build(HookIntegrationPrivate))
    }

    fn create_state(_ctx: CreateStateContext<Self>) -> Self::State {
        HookIntegrationState
    }

    fn begin(_ctx: BeginContext<'_, Self>) -> Result<(), CustomScanError> {
        Ok(())
    }

    fn next_slot(_ctx: NextSlotContext<'_, Self>) -> Result<bool, CustomScanError> {
        Ok(false)
    }

    fn rescan(_ctx: ReScanContext<'_, Self>) -> Result<(), CustomScanError> {
        Ok(())
    }

    fn end(_ctx: EndContext<'_, Self>) -> Result<(), CustomScanError> {
        Ok(())
    }
}

fn relation_name(rel_oid: pg_sys::Oid) -> Option<String> {
    unsafe {
        let raw = pg_sys::get_rel_name(rel_oid);
        if raw.is_null() {
            return None;
        }
        let name = CStr::from_ptr(raw).to_string_lossy().into_owned();
        pg_sys::pfree(raw.cast());
        Some(name)
    }
}

fn is_int4_eq_comparison(predicate: &PlanPredicate) -> bool {
    match predicate {
        PlanPredicate::Comparison { op, left, right } => {
            let accepted_shape = matches!(
                (left, right),
                (
                    PlanScalar::Column(_),
                    PlanScalar::Literal(_) | PlanScalar::Dynamic(_),
                ) | (
                    PlanScalar::Literal(_) | PlanScalar::Dynamic(_),
                    PlanScalar::Column(_),
                )
            );
            accepted_shape
                && predicate.scan_column_type() == Some(pg_sys::INT4OID)
                && op.opno == pg_sys::Oid::from(INT4EQ_OPNO)
        }
        _ => false,
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::Spi;
    use pgrx::pg_test;

    use super::PROVIDER_NAME;

    fn provider_name() -> &'static str {
        PROVIDER_NAME
            .to_str()
            .expect("provider name must be valid UTF-8")
    }

    fn run_batch(stmts: &[&str]) {
        Spi::connect_mut(|client| -> pgrx::spi::Result<()> {
            for stmt in stmts {
                client.update(*stmt, None, &[])?;
            }
            Ok(())
        })
        .expect("SPI batch execution failed");
    }

    fn explain_text_with_setup(setup: &[&str], query: &str) -> String {
        Spi::connect_mut(|client| -> pgrx::spi::Result<String> {
            for stmt in setup {
                client.update(*stmt, None, &[])?;
            }

            let mut lines = Vec::new();
            let sql = format!("EXPLAIN (COSTS OFF) {query}");
            let plan = client.select(sql.as_str(), None, &[])?;
            for row in plan {
                lines.push(
                    row.get::<String>(1)?
                        .expect("EXPLAIN row must contain text"),
                );
            }

            Ok(lines.join("\n"))
        })
        .expect("EXPLAIN through SPI failed")
    }

    #[pg_test]
    fn force_mode_emits_plain_custom_path() {
        run_batch(&[
            "DROP TABLE IF EXISTS hook_plain_force_t",
            "CREATE TEMP TABLE hook_plain_force_t(a int4)",
            "INSERT INTO hook_plain_force_t VALUES (1), (2), (3)",
        ]);

        let plan = explain_text_with_setup(
            &["SET LOCAL pg_lakebase.customscan_mode = 'force'"],
            "SELECT * FROM hook_plain_force_t WHERE a = 1",
        );

        assert!(
            plan.contains(provider_name()),
            "force mode must emit a CustomPath for a legal plain exact clause; got\n{plan}",
        );
    }

    #[pg_test]
    fn off_mode_suppresses_plain_custom_path() {
        run_batch(&[
            "DROP TABLE IF EXISTS hook_plain_off_t",
            "CREATE TEMP TABLE hook_plain_off_t(a int4)",
            "INSERT INTO hook_plain_off_t VALUES (1), (2), (3)",
        ]);

        let plan = explain_text_with_setup(
            &["SET LOCAL pg_lakebase.customscan_mode = 'off'"],
            "SELECT * FROM hook_plain_off_t WHERE a = 1",
        );

        assert!(
            !plan.contains(provider_name()),
            "off mode must suppress every framework-emitted CustomPath; got\n{plan}",
        );
    }

    #[pg_test]
    fn unsupported_only_plain_relation_emits_no_custom_path() {
        run_batch(&[
            "DROP TABLE IF EXISTS hook_plain_unsupported_t",
            "CREATE TEMP TABLE hook_plain_unsupported_t(a int4)",
            "INSERT INTO hook_plain_unsupported_t VALUES (1), (2), (3)",
        ]);

        let plan = explain_text_with_setup(
            &["SET LOCAL pg_lakebase.customscan_mode = 'force'"],
            "SELECT * FROM hook_plain_unsupported_t WHERE (a + 1) = 2",
        );

        assert!(
            !plan.contains(provider_name()),
            "unsupported-only quals must not produce a CustomPath even under force; got\n{plan}",
        );
    }

    #[pg_test]
    fn join_parameterized_non_empty_group_emits_custom_path() {
        run_batch(&[
            "DROP TABLE IF EXISTS hook_join_outer_emit",
            "DROP TABLE IF EXISTS hook_join_inner_emit",
            "CREATE TEMP TABLE hook_join_outer_emit(a int4)",
            "CREATE TEMP TABLE hook_join_inner_emit(a int4)",
            "INSERT INTO hook_join_outer_emit VALUES (1), (2), (3)",
            "INSERT INTO hook_join_inner_emit VALUES (1), (2), (3)",
        ]);

        let plan = explain_text_with_setup(
            &[
                "SET LOCAL pg_lakebase.customscan_mode = 'force'",
                "SET LOCAL enable_hashjoin = off",
                "SET LOCAL enable_mergejoin = off",
            ],
            "SELECT * FROM hook_join_outer_emit o JOIN hook_join_inner_emit i ON i.a = o.a",
        );

        assert!(
            plan.contains(provider_name()),
            "a non-empty join-parameterized group must be able to emit a CustomPath; got\n{plan}",
        );
    }

    #[pg_test]
    fn join_parameterized_empty_group_emits_no_custom_path() {
        run_batch(&[
            "DROP TABLE IF EXISTS hook_join_outer_skip",
            "DROP TABLE IF EXISTS hook_join_inner_skip",
            "CREATE TEMP TABLE hook_join_outer_skip(a int4)",
            "CREATE TEMP TABLE hook_join_inner_skip(a int4)",
            "INSERT INTO hook_join_outer_skip VALUES (1), (2), (3)",
            "INSERT INTO hook_join_inner_skip VALUES (1), (2), (3)",
        ]);

        let plan = explain_text_with_setup(
            &[
                "SET LOCAL pg_lakebase.customscan_mode = 'force'",
                "SET LOCAL enable_hashjoin = off",
                "SET LOCAL enable_mergejoin = off",
            ],
            "SELECT * FROM hook_join_outer_skip o JOIN hook_join_inner_skip i ON (i.a + 1) = o.a",
        );

        assert!(
            !plan.contains(provider_name()),
            "an empty join-parameterized pushdown group must not emit a CustomPath; got\n{plan}",
        );
    }
}
