//! End-to-end checks for the core negotiator's OR/NOT safety boundaries.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use lagodb_core::expr::{PushdownContract, PushdownCosting};
    use lagodb_core::fdw::ForeignPlanQualLocation;
    use pgrx::Spi;
    use pgrx::pg_test;

    use super::super::fixture::{TestTrace, TraceEvent};

    struct FilterTestTable {
        name: &'static str,
    }

    impl FilterTestTable {
        fn create(name: &'static str, server: &'static str) -> Self {
            let create_fdw = "DO $$ BEGIN CREATE FOREIGN DATA WRAPPER framework_test_fdw HANDLER framework_test_fdw_fdw_handler; EXCEPTION WHEN duplicate_object THEN NULL; END $$";
            let drop_server = format!("DROP SERVER IF EXISTS {server} CASCADE");
            let create_server = format!(
                "CREATE SERVER {server} FOREIGN DATA WRAPPER framework_test_fdw"
            );
            let create_table = format!(
                "CREATE FOREIGN TABLE {name} (id int4, sort_key int4, payload text) SERVER {server}"
            );
            Spi::connect_mut(|client| -> pgrx::spi::Result<()> {
                client.update(create_fdw, None, &[])?;
                client.update(&drop_server, None, &[])?;
                client.update(&create_server, None, &[])?;
                client.update(&create_table, None, &[])?;
                Ok(())
            })
            .expect("FDW filter-negotiation table setup failed");
            Self { name }
        }

        fn ids(&self, predicate: &str) -> Vec<i32> {
            let sql =
                format!("SELECT id FROM {} WHERE {predicate} ORDER BY id", self.name);
            Spi::connect_mut(|client| -> pgrx::spi::Result<Vec<i32>> {
                client
                    .select(&sql, None, &[])?
                    .map(|row| Ok(row.get::<i32>(1)?.expect("id must be non-NULL")))
                    .collect()
            })
            .expect("FDW filter-negotiation query failed")
        }

        fn assert_no_planned_filter(&self, predicate: &str, expected: &[i32]) {
            TestTrace::clear();
            assert_eq!(self.ids(predicate), expected);
            let trace = TestTrace::take();
            assert!(
                trace.iter().any(|event| matches!(
                    event,
                    TraceEvent::ScanBegin {
                        planned_count: 0,
                        filters,
                        ..
                    } if filters.is_empty()
                )),
                "provider must receive no planned filter for {predicate:?}: {trace:?}",
            );
            assert!(
                trace.iter().any(|event| matches!(
                    event,
                    TraceEvent::PlanBuild {
                        filters,
                        binding_count: 0,
                        residual_count: 1,
                        recheck_count: 0,
                    } if filters.is_empty()
                )),
                "build_plan must receive the finalized residual-only filter plan for {predicate:?}: {trace:?}",
            );
        }
    }

    #[pg_test]
    fn build_plan_receives_finalized_typed_filter_plan() {
        let table = FilterTestTable::create(
            "fdw_filter_plan_view_t",
            "fdw_filter_plan_view_server",
        );

        TestTrace::clear();
        assert_eq!(table.ids("id = 2 AND sort_key = 10"), vec![2]);
        let trace = TestTrace::take();

        assert!(
            trace.iter().any(|event| matches!(
                event,
                TraceEvent::PlanBuild {
                    filters,
                    binding_count: 2,
                    residual_count: 1,
                    recheck_count: 1,
                } if filters.iter().any(|filter| {
                    filter.0 == 1
                        && filter.1.start == 0
                        && filter.1.end == 1
                        && filter.2 == PushdownContract::ExactRowFilter
                        && filter.3 == PushdownCosting::CostedPruning
                        && filter.4 == ForeignPlanQualLocation::Recheck { index: 0 }
                }) && filters.iter().any(|filter| {
                    filter.0 == 2
                        && filter.1.start == 1
                        && filter.1.end == 2
                        && filter.2 == PushdownContract::ConservativePruning
                        && filter.3 == PushdownCosting::CostedPruning
                        && filter.4 == ForeignPlanQualLocation::Local { index: 0 }
                })
            )),
            "build_plan must receive the final typed predicate, binding range, and final qual split: {trace:?}",
        );
    }

    #[pg_test]
    fn rejected_boolean_candidates_remain_complete_postgresql_residuals() {
        let table = FilterTestTable::create(
            "fdw_filter_negotiation_t",
            "fdw_filter_negotiation_server",
        );

        // Both leaves are supported, but this provider rejects complete OR.
        table.assert_no_planned_filter("id = 1 OR sort_key = 20", &[1, 3]);

        // One supported branch must not escape an OR with an unsupported branch.
        table.assert_no_planned_filter("id = 2 OR id + 1 = 2", &[1, 2]);

        // Core can widen the left AND to `id = 2`, but the resulting complete
        // OR candidate must be confirmed by the provider and is rejected here.
        table.assert_no_planned_filter(
            "(id = 2 AND payload = 'nope') OR sort_key = 20",
            &[3],
        );

        // NOT never performs partial pushdown.
        table.assert_no_planned_filter("NOT (id = 2)", &[1, 3]);
    }

    #[pg_test]
    fn volatile_candidate_remains_a_postgresql_residual() {
        let table = FilterTestTable::create(
            "fdw_filter_volatile_t",
            "fdw_filter_volatile_server",
        );

        // Multiplying by zero keeps the result deterministic while retaining
        // the volatile `random()` call in this single candidate predicate.
        table.assert_no_planned_filter("id = 2 + (random() * 0)::integer", &[2]);
    }
}
