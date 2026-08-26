use pgrx::prelude::*;

use super::harness::{InjectionAction, WORKER_CONNECTION_POINT, WorkerHarness};

#[pgrx::pg_schema]
mod tests {
    use super::*;

    #[pgrx::pg_test]
    fn supervisor_restart_preserves_worker_at_production_boundary() {
        let harness = WorkerHarness::new();
        harness.wait_for_worker("NOT needs_restart AND process_state = 'stopped'");

        let mut injection = harness.attach(InjectionAction::Wait);
        harness.wake_maintenance();
        harness.wait_for_worker_at_connection_point();
        injection.mark_waiter_reached();
        let old_supervisor_pid = harness.supervisor_pid();
        let old_worker_pid = harness.worker_pid();

        let terminated = Spi::get_one::<bool>(
            "SELECT pg_catalog.pg_terminate_backend(pid) FROM lagodb.process_status WHERE process_kind = 'supervisor'",
        )
        .expect("supervisor termination query failed")
        .unwrap_or(false);
        assert!(terminated, "supervisor was not terminated");

        harness.wait_for_query(
            "restarted supervisor retained the worker at the production boundary",
            &format!(
                concat!(
                    "SELECT (SELECT pid <> {} AND state = 'running' ",
                    "FROM lagodb.process_status WHERE process_kind = 'supervisor') ",
                    "AND (SELECT pid = {} AND process_state = 'running' ",
                    "FROM lagodb.worker_status WHERE database_oid = {} ",
                    "AND extension_name = 'lagodb_base' ",
                    "AND worker_name = 'maintenance') ",
                    "AND EXISTS (SELECT FROM pg_catalog.pg_stat_activity ",
                    "WHERE pid = {} ",
                    "AND wait_event_type = 'InjectionPoint' ",
                    "AND wait_event = '{}') ",
                    "AND (SELECT count(*) = 1 FROM pg_catalog.pg_stat_activity ",
                    "WHERE backend_type = 'lagodb worker' AND datid = {})"
                ),
                old_supervisor_pid,
                old_worker_pid,
                harness.database_oid(),
                old_worker_pid,
                WORKER_CONNECTION_POINT,
                harness.database_oid()
            ),
        );

        injection.release();
        harness.wait_for_worker(
            "NOT needs_restart AND process_state = 'stopped' AND pid IS NULL",
        );
    }

    #[pgrx::pg_test]
    fn supervisor_and_coordinator_ignore_statement_cancel() {
        let harness = WorkerHarness::new();
        harness.wait_for_worker(
            "failure_count = 0 AND NOT needs_restart AND process_state = 'stopped'",
        );

        let mut injection = harness.attach(InjectionAction::Error);
        harness.wake_maintenance();
        harness.wait_for_worker(
            "failure_count = 1 AND needs_restart AND process_state = 'restarting' AND pid IS NULL",
        );
        harness.wait_for_coordinator();

        let supervisor_pid = harness.supervisor_pid();
        let coordinator_pid = harness.coordinator_pid();
        let cancelled = Spi::get_one::<bool>(&format!(
            "SELECT pg_catalog.pg_cancel_backend({supervisor_pid}) AND pg_catalog.pg_cancel_backend({coordinator_pid})",
        ))
        .expect("control-process cancellation query failed")
        .unwrap_or(false);
        assert!(cancelled, "PostgreSQL did not deliver both SIGINT requests");
        Spi::run("SELECT pg_catalog.pg_sleep(0.1)")
            .expect("control-process cancellation observation sleep failed");

        let unchanged = Spi::get_one::<bool>(&format!(
            concat!(
                "SELECT EXISTS (SELECT FROM lagodb.process_status ",
                "WHERE process_kind = 'supervisor' AND pid = {} ",
                "AND state = 'running') AND EXISTS (SELECT FROM ",
                "lagodb.process_status WHERE process_kind = 'coordinator' ",
                "AND database_oid = {} AND pid = {} AND state = 'running')"
            ),
            supervisor_pid,
            harness.database_oid(),
            coordinator_pid,
        ))
        .expect("control-process status query failed")
        .unwrap_or(false);
        assert!(unchanged, "SIGINT restarted a worker control process");

        injection.release();
        harness.wait_for_worker(
            "failure_count = 0 AND NOT needs_restart AND process_state = 'stopped' AND pid IS NULL",
        );
    }
}
