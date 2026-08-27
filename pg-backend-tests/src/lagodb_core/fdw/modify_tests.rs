#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::Spi;
    use pgrx::pg_test;

    use super::super::fixture::{TestTrace, TraceEvent};

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
        .expect("FDW modify table setup failed");
    }

    fn query_modify_rows(sql: &str) -> Vec<(i32, String)> {
        Spi::connect_mut(|client| -> pgrx::spi::Result<Vec<(i32, String)>> {
            let rows = client.update(sql, None, &[])?;
            rows.map(|row| {
                Ok((
                    row.get::<i32>(1)?.expect("returned id must be non-NULL"),
                    row.get::<String>(2)?
                        .expect("returned payload must be non-NULL"),
                ))
            })
            .collect()
        })
        .expect("FDW modify RETURNING query failed")
    }

    fn query_item_pointer_rows(sql: &str) -> Vec<(i32, bool)> {
        Spi::connect_mut(|client| -> pgrx::spi::Result<Vec<(i32, bool)>> {
            let rows = client.update(sql, None, &[])?;
            rows.map(|row| {
                Ok((
                    row.get::<i32>(1)?.expect("returned id must be non-NULL"),
                    row.get::<bool>(2)?.expect("ctid test must be non-NULL"),
                ))
            })
            .collect()
        })
        .expect("FDW ItemPointer RETURNING query failed")
    }

    fn query_state(table: &str) -> Vec<(i32, i32, String)> {
        Spi::connect_mut(|client| -> pgrx::spi::Result<Vec<(i32, i32, String)>> {
            let rows = client.select(
                &format!("SELECT id, sort_key, payload FROM {table} ORDER BY id"),
                None,
                &[],
            )?;
            rows.map(|row| {
                Ok((
                    row.get::<i32>(1)?.expect("id must be non-NULL"),
                    row.get::<i32>(2)?.expect("sort_key must be non-NULL"),
                    row.get::<String>(3)?.expect("payload must be non-NULL"),
                ))
            })
            .collect()
        })
        .expect("FDW final-state query failed")
    }

    #[pg_test]
    fn modify_with_attribute_identity() {
        let table = "fdw_test_attr_modify_t";
        prepare_table(table, "fdw_test_attr_modify_server");
        TestTrace::clear();

        let inserted = query_modify_rows(&format!(
            "INSERT INTO {table} VALUES (4, 40, 'inserted') RETURNING id, payload"
        ));
        assert_eq!(inserted, vec![(4, "inserted".to_owned())]);

        let updated = query_modify_rows(&format!(
            "UPDATE {table} SET id = 10, sort_key = 99, payload = 'updated' \
             WHERE id = 1 RETURNING id, payload"
        ));
        assert_eq!(updated, vec![(10, "updated".to_owned())]);

        let deleted = query_modify_rows(&format!(
            "DELETE FROM {table} WHERE id = 2 RETURNING id, payload"
        ));
        assert_eq!(deleted, vec![(2, "alpha".to_owned())]);

        assert_eq!(
            query_state(table),
            vec![
                (3, 20, "mike".to_owned()),
                (4, 40, "inserted".to_owned()),
                (10, 99, "updated".to_owned()),
            ]
        );

        let trace = TestTrace::take();
        assert!(trace.iter().any(|event| {
            matches!(
                event,
                TraceEvent::Modify {
                    operation: "insert",
                    identity: "none",
                    id: 4,
                    returned_item_pointer: false,
                }
            )
        }));
        assert!(trace.iter().any(|event| {
            matches!(
                event,
                TraceEvent::Modify {
                    operation: "update",
                    identity: "attribute",
                    id: 1,
                    returned_item_pointer: false,
                }
            )
        }));
        assert!(trace.iter().any(|event| {
            matches!(
                event,
                TraceEvent::Modify {
                    operation: "delete",
                    identity: "attribute",
                    id: 2,
                    returned_item_pointer: false,
                }
            )
        }));
    }

    #[pg_test]
    fn modify_with_item_pointer_identity() {
        let table = "fdw_test_tid_modify_t";
        prepare_table(table, "fdw_test_tid_modify_server");
        TestTrace::clear();

        let inserted = query_item_pointer_rows(&format!(
            "INSERT INTO {table} VALUES (4, 40, 'inserted') \
             RETURNING id, ctid = '(1,5)'::tid"
        ));
        assert_eq!(inserted, vec![(4, true)]);

        let updated = query_item_pointer_rows(&format!(
            "UPDATE {table} SET id = 10, sort_key = 99, payload = 'updated' \
             WHERE id = 1 RETURNING id, ctid = '(1,11)'::tid"
        ));
        assert_eq!(updated, vec![(10, true)]);

        let deleted = query_item_pointer_rows(&format!(
            "DELETE FROM {table} WHERE id = 2 RETURNING id, ctid = '(1,3)'::tid"
        ));
        assert_eq!(deleted, vec![(2, true)]);

        assert_eq!(
            query_state(table),
            vec![
                (3, 20, "mike".to_owned()),
                (4, 40, "inserted".to_owned()),
                (10, 99, "updated".to_owned()),
            ]
        );

        let trace = TestTrace::take();
        assert!(trace.iter().any(|event| {
            matches!(
                event,
                TraceEvent::Modify {
                    operation: "insert",
                    identity: "none",
                    id: 4,
                    returned_item_pointer: true,
                }
            )
        }));
        assert!(trace.iter().any(|event| {
            matches!(
                event,
                TraceEvent::Modify {
                    operation: "update",
                    identity: "item-pointer",
                    id: 1,
                    returned_item_pointer: true,
                }
            )
        }));
        assert!(trace.iter().any(|event| {
            matches!(
                event,
                TraceEvent::Modify {
                    operation: "delete",
                    identity: "item-pointer",
                    id: 2,
                    returned_item_pointer: true,
                }
            )
        }));
    }
}
