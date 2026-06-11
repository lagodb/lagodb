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

#[cfg(any(test, feature = "pg_test"))]
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
                plan.contains("Custom Scan (pg-iceberg-am)"),
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
}
