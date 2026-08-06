use std::time::{Duration, Instant};

use pgrx::prelude::*;

use crate::worker::WorkerKey;

pub(super) const WORKER_CONNECTION_POINT: &str =
    "lakebase-worker-after-database-connection";

const WORKER_TEST_SUITE_LOCK: i64 = -5_494_768_671_203_916_627;

pub(super) struct WorkerHarness {
    database_oid: i64,
    suite_lock_held: bool,
}

impl WorkerHarness {
    pub(super) fn new() -> Self {
        let database_oid = Spi::get_one::<i64>(
            "SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()",
        )
        .expect("database OID query failed")
        .expect("current database OID is null");
        Spi::run(&format!(
            "SELECT pg_catalog.pg_advisory_lock({WORKER_TEST_SUITE_LOCK})"
        ))
        .expect("failed to serialize worker tests");
        let harness = Self {
            database_oid,
            suite_lock_held: true,
        };
        Spi::run("CREATE EXTENSION IF NOT EXISTS injection_points")
            .expect("failed to install PostgreSQL injection-point controls");
        harness
    }

    pub(super) fn attach(&self, action: InjectionAction) -> InjectionAttachment {
        InjectionAttachment::new(action)
    }

    pub(super) fn wake_maintenance(&self) {
        let needs_supervisor_wake =
            crate::worker::wake_worker(self.maintenance_key());
        if needs_supervisor_wake {
            crate::worker::signal_supervisor();
        }
    }

    fn maintenance_key(&self) -> WorkerKey {
        let worker_id = Spi::get_one::<i32>(&format!(
            "SELECT worker_id FROM lakebase.worker_status WHERE database_oid = {} AND extension_name = 'pg_lakebase_runtime' AND worker_name = 'maintenance'",
            self.database_oid
        ))
        .expect("maintenance worker ID query failed")
        .expect("maintenance worker ID is missing");

        // The SQL wake function publishes its action from PostgreSQL's
        // top-level commit callback. A #[pg_test] body runs inside pgrx's
        // surrounding transaction, so that callback cannot run before this
        // body returns. Exercise the already-committed shared-state effect
        // directly; transaction publication is covered by SQL regression.
        WorkerKey::new(
            u32::try_from(self.database_oid)
                .expect("database OID must fit PostgreSQL OID"),
            worker_id,
        )
    }

    pub(super) fn worker_pid(&self) -> i32 {
        Spi::get_one::<i32>(&format!(
            "SELECT pid FROM lakebase.worker_status WHERE database_oid = {} AND extension_name = 'pg_lakebase_runtime' AND worker_name = 'maintenance'",
            self.database_oid
        ))
        .expect("worker PID query failed")
        .expect("maintenance worker PID is missing")
    }

    pub(super) const fn database_oid(&self) -> i64 {
        self.database_oid
    }

    pub(super) fn supervisor_pid(&self) -> i32 {
        Spi::get_one::<i32>(
            "SELECT pid FROM lakebase.process_status WHERE process_kind = 'supervisor'",
        )
        .expect("supervisor PID query failed")
        .expect("supervisor status is missing")
    }

    pub(super) fn coordinator_pid(&self) -> i32 {
        Spi::get_one::<i32>(&format!(
            "SELECT pid FROM lakebase.process_status WHERE process_kind = 'coordinator' AND database_oid = {}",
            self.database_oid
        ))
        .expect("coordinator PID query failed")
        .expect("coordinator status is missing")
    }

    pub(super) fn wait_for_coordinator(&self) {
        self.wait_for_query(
            "database coordinator is running",
            &format!(
                "SELECT EXISTS (SELECT FROM lakebase.process_status WHERE process_kind = 'coordinator' AND database_oid = {} AND state = 'running' AND pid IS NOT NULL)",
                self.database_oid
            ),
        );
    }

    pub(super) fn wait_for_worker(&self, predicate: &str) {
        self.wait_for_query(
            predicate,
            &format!(
                "SELECT EXISTS (SELECT FROM lakebase.worker_status WHERE database_oid = {} AND extension_name = 'pg_lakebase_runtime' AND worker_name = 'maintenance' AND ({predicate}))",
                self.database_oid
            ),
        );
    }

    pub(super) fn wait_for_worker_at_connection_point(&self) {
        self.wait_for_query(
            "maintenance worker waiting at its production connection boundary",
            &format!(
                concat!(
                    "SELECT EXISTS (SELECT FROM lakebase.worker_status AS worker ",
                    "JOIN pg_catalog.pg_stat_activity AS activity ON activity.pid = worker.pid ",
                    "WHERE worker.database_oid = {} ",
                    "AND worker.extension_name = 'pg_lakebase_runtime' ",
                    "AND worker.worker_name = 'maintenance' ",
                    "AND worker.process_state = 'running' ",
                    "AND activity.wait_event_type = 'InjectionPoint' ",
                    "AND activity.wait_event = '{}')"
                ),
                self.database_oid, WORKER_CONNECTION_POINT
            ),
        );
    }

    pub(super) fn wait_for_query(&self, description: &str, query: &str) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            Spi::run("SELECT pg_catalog.pg_stat_clear_snapshot()")
                .expect("failed to clear PostgreSQL statistics snapshot");
            if Spi::get_one::<bool>(query)
                .expect("worker status predicate failed")
                .unwrap_or(false)
            {
                return;
            }
            if Instant::now() >= deadline {
                let details = Spi::get_one::<String>(
                "SELECT jsonb_build_object('workers', coalesce((SELECT jsonb_agg(to_jsonb(status)) FROM lakebase.worker_status AS status), '[]'::jsonb), 'processes', coalesce((SELECT jsonb_agg(to_jsonb(status)) FROM lakebase.process_status AS status), '[]'::jsonb))::text",
                )
                .expect("worker diagnostic query failed")
                .unwrap_or_else(|| "{}".to_owned());
                panic!(
                    "worker predicate timed out: predicate={description}, status={details}"
                );
            }
            Spi::run("SELECT pg_catalog.pg_sleep(0.01)")
                .expect("worker predicate sleep failed");
        }
    }
}

impl Drop for WorkerHarness {
    fn drop(&mut self) {
        if self.suite_lock_held {
            let _ = Spi::run(&format!(
                "SELECT pg_catalog.pg_advisory_unlock({WORKER_TEST_SUITE_LOCK})"
            ));
            self.suite_lock_held = false;
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum InjectionAction {
    Error,
    Wait,
}

impl InjectionAction {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Wait => "wait",
        }
    }
}

pub(super) struct InjectionAttachment {
    action: InjectionAction,
    waiter_reached: bool,
    attached: bool,
}

impl InjectionAttachment {
    fn new(action: InjectionAction) -> Self {
        Spi::run(&format!(
            "SELECT injection_points_attach('{WORKER_CONNECTION_POINT}', '{}')",
            action.as_str()
        ))
        .expect("failed to attach worker injection point");
        Self {
            action,
            waiter_reached: false,
            attached: true,
        }
    }

    pub(super) fn mark_waiter_reached(&mut self) {
        assert!(matches!(self.action, InjectionAction::Wait));
        self.waiter_reached = true;
    }

    pub(super) fn release(&mut self) {
        if self.waiter_reached {
            Spi::run(&format!(
                "SELECT injection_points_wakeup('{WORKER_CONNECTION_POINT}')"
            ))
            .expect("failed to wake worker injection point");
            self.waiter_reached = false;
        }
        if self.attached {
            Spi::run(&format!(
                "SELECT injection_points_detach('{WORKER_CONNECTION_POINT}')"
            ))
            .expect("failed to detach worker injection point");
            self.attached = false;
        }
    }
}

impl Drop for InjectionAttachment {
    fn drop(&mut self) {
        if self.waiter_reached {
            let _ = Spi::run(&format!(
                "SELECT injection_points_wakeup('{WORKER_CONNECTION_POINT}')"
            ));
        }
        if self.attached {
            let _ = Spi::run(&format!(
                "SELECT injection_points_detach('{WORKER_CONNECTION_POINT}')"
            ));
        }
    }
}
