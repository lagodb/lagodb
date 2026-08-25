//! End-to-end CustomScan coverage for the slot-first emit path.
//!
//! Drives a real Iceberg CustomScan (forced via the planner GUC) over a table
//! with varlena columns, a NULL in each varlena, and a reordered projection.
//! Slot datums are decoded into the scan node's per-tuple memory context (which
//! `ExecScan` resets each tuple cycle), so a correct read-back of every row
//! confirms the consumer sees each tuple before that reset reclaims it — the
//! end-to-end counterpart to the per-tuple-context lifetime asserted directly
//! in `pg-backend-tests`
//! (`emit_row_targets_per_tuple_context_and_does_not_grow_tts_mcxt`).

#[pgrx::pg_schema]
mod tests {
    use pgrx::Spi;
    use pgrx::pg_test;

    #[pg_test]
    fn customscan_slot_first_returns_expected_tuples() {
        Spi::connect_mut(|client| -> pgrx::spi::Result<()> {
            for stmt in [
                "CREATE TABLE cs_slot_first_e2e (id integer, label text, payload bytea) USING iceberg",
                "INSERT INTO cs_slot_first_e2e VALUES (1, 'alpha', '\\xdead'::bytea)",
                "INSERT INTO cs_slot_first_e2e VALUES (2, NULL, '\\xbeef'::bytea)",
                "INSERT INTO cs_slot_first_e2e VALUES (3, 'gamma', NULL)",
                "SET pg_lakebase.customscan_mode = 'force'",
            ] {
                client.update(stmt, None, &[])?;
            }

            // Confirm the forced plan actually uses the Iceberg CustomScan,
            // otherwise the row assertions would not exercise the slot-first path.
            let plan = client
                .select(
                    "EXPLAIN (COSTS OFF) \
                     SELECT label, id, payload FROM cs_slot_first_e2e WHERE id >= 1",
                    None,
                    &[],
                )?
                .filter_map(|row| row.get::<String>(1).ok().flatten())
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                plan.contains("Custom Scan (lagodb-iceberg)"),
                "forced plan must use the Iceberg CustomScan; got\n{plan}",
            );

            // Reordered, subset projection (label, id, payload) past the
            // declaration order (id, label, payload).
            let mut got: Vec<(Option<String>, i32, Option<Vec<u8>>)> = Vec::new();
            for row in client.select(
                "SELECT label, id, payload FROM cs_slot_first_e2e WHERE id >= 1 ORDER BY id",
                None,
                &[],
            )? {
                let label = row.get::<String>(1)?;
                let id = row.get::<i32>(2)?.expect("id column is never NULL");
                let payload = row.get::<Vec<u8>>(3)?;
                got.push((label, id, payload));
            }

            assert_eq!(
                got,
                vec![
                    (Some("alpha".to_string()), 1, Some(vec![0xde, 0xad])),
                    (None, 2, Some(vec![0xbe, 0xef])),
                    (Some("gamma".to_string()), 3, None),
                ],
            );

            Ok(())
        })
        .expect("CustomScan slot-first end-to-end query failed");
    }

    #[pg_test]
    fn customscan_executes_subplan_qual_over_projected_tuple() {
        Spi::connect_mut(|client| -> pgrx::spi::Result<()> {
            for stmt in [
                "CREATE TABLE cs_subplan_projected (id integer, label text, amount integer, tag text) USING iceberg",
                "INSERT INTO cs_subplan_projected VALUES (1, 'alpha', 10, 'a')",
                "INSERT INTO cs_subplan_projected VALUES (2, 'beta', 20, 'b')",
                "INSERT INTO cs_subplan_projected VALUES (3, 'gamma', 30, 'c')",
                "CREATE TEMP TABLE cs_subplan_keys (id integer)",
                "INSERT INTO cs_subplan_keys VALUES (1), (3)",
                "SET pg_lakebase.customscan_mode = 'force'",
            ] {
                client.update(stmt, None, &[])?;
            }

            let query = "SELECT id, label FROM cs_subplan_projected \
                         WHERE id >= 1 \
                           AND id NOT IN (SELECT id FROM cs_subplan_keys) \
                         ORDER BY id";
            let plan = client
                .select(&format!("EXPLAIN (COSTS OFF) {query}"), None, &[])?
                .filter_map(|row| row.get::<String>(1).ok().flatten())
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                plan.contains("Custom Scan (lagodb-iceberg)"),
                "SubPlan query must exercise the Iceberg CustomScan; got\n{plan}",
            );
            assert!(
                plan.contains("SubPlan"),
                "NOT IN query must retain a planned SubPlan; got\n{plan}",
            );
            assert!(
                plan.contains("Pushed Filter: (id >= 1)"),
                "independent planned filter must select the CustomPath; got\n{plan}",
            );

            let mut rows = Vec::new();
            for row in client.select(query, None, &[])? {
                rows.push((
                    row.get::<i32>(1)?.expect("id is not NULL"),
                    row.get::<String>(2)?.expect("label is not NULL"),
                ));
            }
            assert_eq!(rows, vec![(2, "beta".to_string())]);

            Ok(())
        })
        .expect("CustomScan projected SubPlan query failed");
    }

    #[pg_test]
    fn customscan_preserves_relabelled_parameter_type_for_filter_binding() {
        Spi::connect_mut(|client| -> pgrx::spi::Result<()> {
            for stmt in [
                "CREATE TABLE cs_relabel_filter (id integer) USING iceberg",
                "INSERT INTO cs_relabel_filter VALUES (1), (2), (3)",
                "SET pg_lakebase.customscan_mode = 'force'",
                "SET plan_cache_mode = force_generic_plan",
                "PREPARE cs_relabel_filter_query (oid) AS \
                 SELECT id FROM cs_relabel_filter WHERE id = $1::int4",
            ] {
                client.update(stmt, None, &[])?;
            }

            let plan = client
                .select(
                    "EXPLAIN (COSTS OFF) EXECUTE cs_relabel_filter_query('2'::oid)",
                    None,
                    &[],
                )?
                .filter_map(|row| row.get::<String>(1).ok().flatten())
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                plan.contains("Custom Scan (lagodb-iceberg)"),
                "binary-relabeled parameter must remain pushable; got\n{plan}",
            );
            assert!(
                plan.contains("Pushed Filter:")
                    && plan.contains("$1")
                    && plan.contains("integer"),
                "plan must retain the complete relabeled parameter; got\n{plan}",
            );

            let id = client
                .select("EXECUTE cs_relabel_filter_query('2'::oid)", None, &[])?
                .next()
                .expect("query must return one row")
                .get::<i32>(1)?;
            assert_eq!(id, Some(2));
            Ok(())
        })
        .expect("CustomScan relabeled parameter query failed");
    }
}
