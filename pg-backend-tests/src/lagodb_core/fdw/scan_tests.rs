#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::Spi;
    use pgrx::pg_sys;
    use pgrx::pg_test;

    use super::super::fixture::{TestRow, TestStore, TestTrace, TraceEvent};

    fn prepare_table(table: &str, server: &str) {
        let create_fdw = "DO $$ BEGIN CREATE FOREIGN DATA WRAPPER framework_test_fdw HANDLER framework_test_fdw_fdw_handler; EXCEPTION WHEN duplicate_object THEN NULL; END $$";
        let drop_server = format!("DROP SERVER IF EXISTS {server} CASCADE");
        let create_server =
            format!("CREATE SERVER {server} FOREIGN DATA WRAPPER framework_test_fdw");
        let create_table = format!(
            "CREATE FOREIGN TABLE {table} (id int4, sort_key int4, payload text) SERVER {server}"
        );
        Spi::connect_mut(|client| -> pgrx::spi::Result<()> {
            client.update(create_fdw, None, &[])?;
            client.update(&drop_server, None, &[])?;
            client.update(&create_server, None, &[])?;
            client.update(&create_table, None, &[])?;
            Ok(())
        })
        .expect("FDW test table setup failed");
    }

    fn table_oid(table: &str) -> pg_sys::Oid {
        Spi::get_one::<i64>(&format!("SELECT '{table}'::regclass::oid::int8"))
            .expect("foreign table OID lookup failed")
            .map(|oid| pg_sys::Oid::from(oid as u32))
            .expect("foreign table OID lookup returned NULL")
    }

    fn query_rows(sql: &str) -> Vec<(i32, i32, String)> {
        Spi::connect_mut(|client| -> pgrx::spi::Result<Vec<(i32, i32, String)>> {
            let rows = client.select(sql, None, &[])?;
            rows.map(|row| {
                Ok((
                    row.get::<i32>(1)?.expect("id must be non-NULL"),
                    row.get::<i32>(2)?.expect("sort_key must be non-NULL"),
                    row.get::<String>(3)?.expect("payload must be non-NULL"),
                ))
            })
            .collect()
        })
        .expect("FDW row query failed")
    }

    fn query_ids(sql: &str) -> Vec<i32> {
        Spi::connect_mut(|client| -> pgrx::spi::Result<Vec<i32>> {
            let rows = client.select(sql, None, &[])?;
            rows.map(|row| Ok(row.get::<i32>(1)?.expect("id must be non-NULL")))
                .collect()
        })
        .expect("FDW id query failed")
    }

    fn explain(sql: &str) -> String {
        let statement = format!("EXPLAIN (FORMAT JSON, COSTS OFF) {sql}");
        Spi::connect_mut(|client| -> pgrx::spi::Result<String> {
            let rows = client.select(&statement, None, &[])?;
            rows.into_iter()
                .next()
                .expect("EXPLAIN must return one row")
                .get::<pgrx::datum::JsonString>(1)
                .map(|value| value.expect("EXPLAIN JSON must be non-NULL").0)
        })
        .expect("FDW EXPLAIN failed")
    }

    #[pg_test]
    fn scan_projection_and_filter_pushdown() {
        let table = "fdw_test_scan_projection_t";
        prepare_table(table, "fdw_test_scan_projection_server");

        let all_rows = query_rows(&format!(
            "SELECT id, sort_key, payload FROM {table} ORDER BY id"
        ));
        assert_eq!(
            all_rows,
            vec![
                (1, 30, "zulu".to_owned()),
                (2, 10, "alpha".to_owned()),
                (3, 20, "mike".to_owned()),
            ]
        );

        TestTrace::clear();
        let whole_row_count = Spi::connect_mut(|client| -> pgrx::spi::Result<i64> {
            let rows = client.select(
                &format!("SELECT {table} FROM {table} WHERE id = 1"),
                None,
                &[],
            )?;
            Ok(rows.count() as i64)
        })
        .expect("whole-row FDW query failed");
        assert_eq!(whole_row_count, 1);
        assert!(TestTrace::take().iter().any(|event| {
            matches!(
                event,
                TraceEvent::ScanBegin {
                    filters,
                    projection: "relation",
                    ..
                } if filters == &vec![(1, Some(1))]
            )
        }));

        TestTrace::clear();
        let projected =
            Spi::connect_mut(|client| -> pgrx::spi::Result<Vec<String>> {
                let rows = client.select(
                    &format!("SELECT payload FROM {table} WHERE id = 2"),
                    None,
                    &[],
                )?;
                rows.map(|row| {
                    Ok(row.get::<String>(1)?.expect("payload must be non-NULL"))
                })
                .collect()
            })
            .expect("projected FDW query failed");
        assert_eq!(projected, vec!["alpha"]);
        assert!(TestTrace::take().iter().any(|event| {
            matches!(
                event,
                TraceEvent::ScanBegin {
                    planned_count: 1,
                    filters,
                    projection: "projected",
                    ..
                } if filters == &vec![(1, Some(2))]
            )
        }));

        TestTrace::clear();
        let count = Spi::connect_mut(|client| -> pgrx::spi::Result<i64> {
            let rows =
                client.select(&format!("SELECT count(*) FROM {table}"), None, &[])?;
            rows.into_iter()
                .next()
                .expect("COUNT must return one row")
                .get::<i64>(1)
                .map(|value| value.expect("COUNT must be non-NULL"))
        })
        .expect("COUNT over FDW failed");
        assert_eq!(count, 3);
        let count_trace = TestTrace::take();
        assert!(
            count_trace.iter().any(|event| {
                matches!(
                    event,
                    TraceEvent::ScanBegin {
                        planned_count: 0,
                        filters,
                        projection: "projected",
                        ..
                    } if filters.is_empty()
                )
            }),
            "COUNT trace did not use the observed projected shape: {count_trace:?}"
        );

        TestTrace::clear();
        let constant_rows =
            Spi::connect_mut(|client| -> pgrx::spi::Result<Vec<i32>> {
                let rows = client.select(
                    &format!("SELECT 1 FROM {table} LIMIT 1"),
                    None,
                    &[],
                )?;
                rows.map(|row| {
                    Ok(row.get::<i32>(1)?.expect("constant must be non-NULL"))
                })
                .collect()
            })
            .expect("constant FDW query failed");
        assert_eq!(constant_rows, vec![1]);
        let constant_trace = TestTrace::take();
        assert!(
            constant_trace.iter().any(|event| {
                matches!(
                    event,
                    TraceEvent::ScanBegin {
                        planned_count: 0,
                        filters,
                        projection: "synthetic-null",
                        ..
                    } if filters.is_empty()
                )
            }),
            "constant query did not use synthetic-null projection: {constant_trace:?}"
        );

        TestTrace::clear();
        let sort_key_ids =
            query_ids(&format!("SELECT id FROM {table} WHERE sort_key = 10"));
        assert_eq!(sort_key_ids, vec![2]);
        assert!(TestTrace::take().iter().any(|event| {
            matches!(
                event,
                TraceEvent::ScanBegin {
                    planned_count: 1,
                    filters,
                    ..
                } if filters == &vec![(2, Some(10))]
            )
        }));

        TestTrace::clear();
        let mixed_ids = query_ids(&format!(
            "SELECT id FROM {table} WHERE id = 2 AND id + 1 = 4 ORDER BY id"
        ));
        assert!(mixed_ids.is_empty());
        assert!(TestTrace::take().iter().any(|event| {
            matches!(
                event,
                TraceEvent::ScanBegin {
                    planned_count: 1,
                    filters,
                    ..
                } if filters == &vec![(1, Some(2))]
            )
        }));

        TestTrace::clear();
        let conflicting_ids = query_ids(&format!(
            "SELECT id FROM {table} WHERE id = 2 AND sort_key = 20"
        ));
        assert!(conflicting_ids.is_empty());
        assert!(TestTrace::take().iter().any(|event| {
            matches!(
                event,
                TraceEvent::ScanBegin {
                    planned_count: 2,
                    filters,
                    ..
                } if filters.len() == 2
                    && filters.contains(&(1, Some(2)))
                    && filters.contains(&(2, Some(20)))
            )
        }));

        TestTrace::clear();
        let residual_ids = query_ids(&format!(
            "SELECT id FROM {table} WHERE id + 1 = 3 ORDER BY id"
        ));
        assert_eq!(residual_ids, vec![2]);
        assert!(TestTrace::take().iter().any(|event| {
            matches!(
                event,
                TraceEvent::ScanBegin {
                    planned_count: 0,
                    filters,
                    ..
                } if filters.is_empty()
            )
        }));
    }

    #[pg_test]
    fn pathkeys_ordered_alternative_and_fallback() {
        let table = "fdw_test_pathkeys_t";
        prepare_table(table, "fdw_test_pathkeys_server");
        TestStore::replace(
            table_oid(table),
            vec![
                TestRow {
                    id: 3,
                    sort_key: 3,
                    payload: "mike".to_owned(),
                },
                TestRow {
                    id: 1,
                    sort_key: 1,
                    payload: "zulu".to_owned(),
                },
                TestRow {
                    id: 2,
                    sort_key: 2,
                    payload: "alpha".to_owned(),
                },
            ],
        );

        TestTrace::clear();
        let ordered = query_rows(&format!(
            "SELECT id, sort_key, payload FROM {table} ORDER BY sort_key DESC LIMIT 2"
        ));
        assert_eq!(
            ordered,
            vec![(3, 3, "mike".to_owned()), (2, 2, "alpha".to_owned()),]
        );
        let ordered_trace = TestTrace::take();
        assert!(ordered_trace.iter().any(|event| {
            matches!(event, TraceEvent::ScanBegin { ordered: true, .. })
        }));
        let ordered_plan = explain(&format!(
            "SELECT id, sort_key FROM {table} ORDER BY sort_key DESC LIMIT 2"
        ));
        assert!(
            !ordered_plan.contains("\"Node Type\": \"Sort\""),
            "ordered FDW path must not require a local Sort: {ordered_plan}"
        );

        TestTrace::clear();
        let equivalent_member_ids = query_ids(&format!(
            "SELECT id FROM {table} WHERE id = sort_key ORDER BY id"
        ));
        assert_eq!(equivalent_member_ids, vec![1, 2, 3]);
        let member_trace = TestTrace::take();
        let pathkey_selections = member_trace
            .iter()
            .filter_map(|event| match event {
                TraceEvent::Pathkeys {
                    candidate_count,
                    selected_attno,
                    ..
                } => Some((*candidate_count, *selected_attno)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(pathkey_selections.len() >= 2);
        assert!(
            pathkey_selections
                .iter()
                .all(|(_, selected_attno)| *selected_attno == 2)
        );
        assert!(
            pathkey_selections
                .iter()
                .any(|(candidate_count, _)| *candidate_count >= 2)
        );
        let member_plan = explain(&format!(
            "SELECT id FROM {table} WHERE id = sort_key ORDER BY id"
        ));
        assert!(
            !member_plan.contains("\"Node Type\": \"Sort\""),
            "ordered EC-member path must not require a local Sort: {member_plan}"
        );

        TestTrace::clear();
        let fallback = Spi::connect_mut(|client| -> pgrx::spi::Result<Vec<String>> {
            let rows = client.select(
                &format!("SELECT payload FROM {table} ORDER BY lower(payload)"),
                None,
                &[],
            )?;
            rows.map(|row| {
                Ok(row.get::<String>(1)?.expect("payload must be non-NULL"))
            })
            .collect()
        })
        .expect("fallback FDW query failed");
        assert_eq!(fallback, vec!["alpha", "mike", "zulu"]);
        assert!(TestTrace::take().iter().any(|event| {
            matches!(event, TraceEvent::ScanBegin { ordered: false, .. })
        }));
        let fallback_plan = explain(&format!(
            "SELECT payload FROM {table} ORDER BY lower(payload)"
        ));
        assert!(
            fallback_plan.contains("\"Node Type\": \"Sort\""),
            "provider-rejected pathkeys must leave a local Sort candidate: {fallback_plan}"
        );
    }

    #[pg_test]
    fn lateral_parameterized_ordered_rescan() {
        let table = "fdw_test_lateral_t";
        prepare_table(table, "fdw_test_lateral_server");
        TestTrace::clear();

        let rows = Spi::connect_mut(|client| -> pgrx::spi::Result<Vec<(Option<i32>, Option<i32>)>> {
            client.update("SET LOCAL enable_hashjoin = off", None, &[])?;
            client.update("SET LOCAL enable_mergejoin = off", None, &[])?;
            let rows = client.select(
                &format!(
                    "SELECT outer_values.v, inner_scan.id \
                     FROM (VALUES (1::int4), (NULL::int4), (3::int4)) AS outer_values(v) \
                     LEFT JOIN LATERAL ( \
                         SELECT id FROM {table} \
                         WHERE {table}.id = outer_values.v \
                         ORDER BY {table}.sort_key LIMIT 1 \
                     ) AS inner_scan ON true \
                     ORDER BY outer_values.v NULLS FIRST"
                ),
                None,
                &[],
            )?;
            rows.map(|row| Ok((row.get::<i32>(1)?, row.get::<i32>(2)?)))
                .collect()
        })
        .expect("LATERAL FDW query failed");
        assert_eq!(
            rows,
            vec![(None, None), (Some(1), Some(1)), (Some(3), Some(3))]
        );

        let trace = TestTrace::take();
        assert!(trace.iter().any(|event| {
            matches!(
                event,
                TraceEvent::ScanBegin {
                    ordered: true,
                    filters,
                    ..
                } if filters == &vec![(1, Some(1))]
            )
        }));
        assert!(trace.iter().any(|event| {
            matches!(
                event,
                TraceEvent::ScanRescan {
                    filters_changed: true,
                    filters,
                }
                if filters == &vec![(1, None)]
            )
        }));
        assert!(trace.iter().any(|event| {
            matches!(
                event,
                TraceEvent::ScanRescan {
                    filters_changed: true,
                    filters,
                }
                if filters == &vec![(1, Some(3))]
            )
        }));
    }

    #[pg_test]
    fn lateral_rescan_rebinds_relabelled_exec_param() {
        let table = "fdw_test_relabelled_param_t";
        prepare_table(table, "fdw_test_relabelled_param_server");
        TestTrace::clear();

        let rows =
            Spi::connect_mut(|client| -> pgrx::spi::Result<Vec<Option<i32>>> {
                client.update("SET LOCAL enable_hashjoin = off", None, &[])?;
                client.update("SET LOCAL enable_mergejoin = off", None, &[])?;
                let rows = client.select(
                    &format!(
                        "SELECT inner_scan.id \
                     FROM (VALUES (1::oid), (3::oid)) AS outer_values(v) \
                     LEFT JOIN LATERAL ( \
                         SELECT id FROM {table} \
                         WHERE {table}.id = outer_values.v::int4 \
                         ORDER BY {table}.sort_key LIMIT 1 \
                     ) AS inner_scan ON true \
                     ORDER BY outer_values.v"
                    ),
                    None,
                    &[],
                )?;
                rows.map(|row| row.get::<i32>(1)).collect()
            })
            .expect("LATERAL FDW relabelled-param query failed");
        assert_eq!(rows, vec![Some(1), Some(3)]);

        let trace = TestTrace::take();
        assert!(trace.iter().any(|event| {
            matches!(
                event,
                TraceEvent::ScanBegin { filters, .. } if filters == &vec![(1, Some(1))]
            )
        }));
        assert!(trace.iter().any(|event| {
            matches!(
                event,
                TraceEvent::ScanRescan {
                    filters_changed: true,
                    filters,
                } if filters == &vec![(1, Some(3))]
            )
        }));
    }

    #[pg_test]
    fn lateral_rescan_does_not_rebind_filters_for_unrelated_params() {
        let table = "fdw_test_unrelated_rescan_t";
        prepare_table(table, "fdw_test_unrelated_rescan_server");
        TestTrace::clear();

        let rows = Spi::connect_mut(|client| -> pgrx::spi::Result<Vec<(String, Option<i32>)>> {
            client.update("SET LOCAL enable_hashjoin = off", None, &[])?;
            client.update("SET LOCAL enable_mergejoin = off", None, &[])?;
            let rows = client.select(
                &format!(
                    "SELECT outer_values.payload, inner_scan.id \
                     FROM (VALUES ('zulu'::text), ('alpha'::text)) AS outer_values(payload) \
                     LEFT JOIN LATERAL ( \
                         SELECT id FROM {table} \
                         WHERE {table}.id = 1 \
                           AND {table}.payload = outer_values.payload \
                         OFFSET 0 LIMIT 1 \
                     ) AS inner_scan ON true \
                     ORDER BY outer_values.payload DESC"
                ),
                None,
                &[],
            )?;
            rows.map(|row| {
                Ok((
                    row.get::<String>(1)?.expect("payload must be non-NULL"),
                    row.get::<i32>(2)?,
                ))
            })
            .collect()
        })
        .expect("LATERAL unrelated-param FDW query failed");
        assert_eq!(
            rows,
            vec![("zulu".to_owned(), Some(1)), ("alpha".to_owned(), None)]
        );

        let rescans = TestTrace::take()
            .into_iter()
            .filter_map(|event| match event {
                TraceEvent::ScanRescan {
                    filters_changed,
                    filters,
                } => Some((filters_changed, filters)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            !rescans.is_empty(),
            "query must rescan the parameterized FDW"
        );
        assert!(rescans.iter().all(|(changed, filters)| {
            !changed && filters == &vec![(1, Some(1))]
        }));
    }
}
