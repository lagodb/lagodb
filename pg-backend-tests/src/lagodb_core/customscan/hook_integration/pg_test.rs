#[pgrx::pg_schema]
mod tests {
    use pgrx::Spi;
    use pgrx::pg_test;

    use super::super::PROVIDER_NAME;

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
        explain_text_with_options(setup, "COSTS OFF", query)
    }

    fn explain_text_with_options(
        setup: &[&str],
        options: &str,
        query: &str,
    ) -> String {
        Spi::connect_mut(|client| -> pgrx::spi::Result<String> {
            for stmt in setup {
                client.update(*stmt, None, &[])?;
            }

            let mut lines = Vec::new();
            let sql = format!("EXPLAIN ({options}) {query}");
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
            &["SET LOCAL lagodb.customscan_mode = 'force'"],
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
            &["SET LOCAL lagodb.customscan_mode = 'off'"],
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
            &["SET LOCAL lagodb.customscan_mode = 'force'"],
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
                "SET LOCAL lagodb.customscan_mode = 'force'",
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
                "SET LOCAL lagodb.customscan_mode = 'force'",
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

    #[pg_test]
    fn accepted_widening_is_persisted_as_conservative() {
        run_batch(&[
            "DROP TABLE IF EXISTS hook_widen_accept_t",
            "CREATE TEMP TABLE hook_widen_accept_t(a int4, b int4)",
            "INSERT INTO hook_widen_accept_t VALUES (1, 5), (2, 20)",
        ]);

        let plan = explain_text_with_options(
            &["SET LOCAL lagodb.customscan_mode = 'force'"],
            "VERBOSE, COSTS OFF",
            "SELECT * FROM hook_widen_accept_t WHERE (a = 1 AND b < 10) OR a = 2",
        );

        assert!(
            plan.contains(provider_name()),
            "provider acceptance of the final widened OR must emit a CustomPath; got\n{plan}",
        );
        assert!(
            plan.contains("Pushed Filter Conservative:"),
            "an Exact provider result for a widened candidate must be downgraded to Conservative; got\n{plan}",
        );
        assert!(
            !plan.contains("Pushed Filter Exact:"),
            "the widened candidate must never inherit the provider's Exact contract; got\n{plan}",
        );
        let residual = plan
            .lines()
            .find(|line| line.trim_start().starts_with("Filter:"))
            .expect("widening must retain the original OR as a residual");
        assert!(
            residual.contains("b < 10"),
            "the residual must be the original OR, not the generated candidate; got\n{plan}",
        );
    }

    #[pg_test(
        error = "customscan \"hook-integration-test-provider\" BeginCustomScan callback failed: customscan provider error: plan-data cell 0 has node tag T_String, expected T_Integer"
    )]
    fn provider_payload_wrong_tag_is_rejected_at_begin() {
        run_batch(&[
            "DROP TABLE IF EXISTS hook_codec_wrong_tag_t",
            "CREATE TEMP TABLE hook_codec_wrong_tag_t(a int4)",
            "SET LOCAL lagodb.customscan_mode = 'force'",
        ]);

        Spi::run("SELECT * FROM hook_codec_wrong_tag_t WHERE a = 1")
            .expect("malformed provider payload must raise PostgreSQL ERROR");
    }

    #[pg_test(
        error = "customscan \"hook-integration-test-provider\" BeginCustomScan callback failed: customscan custom_private codec error: plan-data has trailing cells: read 2, length 3"
    )]
    fn provider_payload_trailing_field_is_rejected_at_begin() {
        run_batch(&[
            "DROP TABLE IF EXISTS hook_codec_trailing_t",
            "CREATE TEMP TABLE hook_codec_trailing_t(a int4)",
            "SET LOCAL lagodb.customscan_mode = 'force'",
        ]);

        Spi::run("SELECT * FROM hook_codec_trailing_t WHERE a = 1").expect(
            "provider payload with a trailing field must raise PostgreSQL ERROR",
        );
    }
}
