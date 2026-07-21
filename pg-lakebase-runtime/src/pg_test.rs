//! pgrx test-runner configuration and PostgreSQL-backed runtime tests.
//!
//! The runner configuration is required only by the host Rust test harness.
//! The supervisor harness and SQL-callable tests are compiled only when
//! `cargo pgrx test` enables the `pg_test` feature.

#[cfg(feature = "pg_test")]
use std::time::{Duration, Instant};

#[cfg(feature = "pg_test")]
use pgrx::prelude::*;

#[cfg(feature = "pg_test")]
use crate::runtime::RuntimeTestInjection;

#[cfg(feature = "pg_test")]
const RUNTIME_TEST_SUITE_LOCK: i64 = -5_494_768_671_203_916_627;

#[cfg(feature = "pg_test")]
struct RuntimeWorkerHarness {
    database_oid: i64,
    lock_held: bool,
    suite_lock_held: bool,
    capacity_workers_registered: bool,
}

#[cfg(feature = "pg_test")]
impl RuntimeWorkerHarness {
    fn new() -> Self {
        let database_oid = Spi::get_one::<i64>(
            "SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()",
        )
        .expect("database OID query failed")
        .expect("current database OID is null");
        Spi::run(&format!(
            "SELECT pg_catalog.pg_advisory_lock({RUNTIME_TEST_SUITE_LOCK})"
        ))
        .expect("failed to serialize runtime supervisor tests");
        Self {
            database_oid,
            lock_held: false,
            suite_lock_held: true,
            capacity_workers_registered: false,
        }
    }

    fn set_injection(&self, injection: &str) {
        RuntimeTestInjection::set(injection);
    }

    fn reset_injection(&self) {
        RuntimeTestInjection::clear();
    }

    fn acquire_worker_barrier(&mut self) {
        Spi::run(&format!(
            "SELECT pg_catalog.pg_advisory_lock({})",
            self.database_oid
        ))
        .expect("failed to acquire worker test barrier");
        self.lock_held = true;
    }

    fn release_worker_barrier(&mut self) {
        if self.lock_held {
            let unlocked = Spi::get_one::<bool>(&format!(
                "SELECT pg_catalog.pg_advisory_unlock({})",
                self.database_oid
            ))
            .expect("failed to release worker test barrier")
            .unwrap_or(false);
            assert!(unlocked, "worker test barrier was not held");
            self.lock_held = false;
        }
    }

    fn wake_maintenance(&self) {
        let extension_oid = self.extension_oid("pg_lakebase_runtime");
        assert!(crate::runtime::wake_worker(
            self.database_oid(),
            extension_oid,
            "maintenance",
        ));
        crate::runtime::signal_launcher();
    }

    fn generation(&self) -> i64 {
        Spi::get_one::<i64>(&format!(
            "SELECT generation::bigint FROM lakebase.worker_runtime_status WHERE database_oid = {} AND extension_name = 'pg_lakebase_runtime' AND worker_name = 'maintenance'",
            self.database_oid
        ))
        .expect("worker generation query failed")
        .expect("maintenance worker status is missing")
    }

    fn worker_pid(&self) -> i32 {
        Spi::get_one::<i32>(&format!(
            "SELECT pid FROM lakebase.worker_runtime_status WHERE database_oid = {} AND extension_name = 'pg_lakebase_runtime' AND worker_name = 'maintenance'",
            self.database_oid
        ))
        .expect("worker PID query failed")
        .expect("maintenance worker PID is missing")
    }

    fn launcher_pid(&self) -> i32 {
        Spi::get_one::<i32>(
            "SELECT pid FROM lakebase.process_runtime_status WHERE process_kind = 'launcher'",
        )
        .expect("launcher PID query failed")
        .expect("launcher status is missing")
    }

    fn database_oid(&self) -> u32 {
        u32::try_from(self.database_oid).expect("database OID exceeds u32")
    }

    fn extension_oid(&self, extension_name: &str) -> u32 {
        let query = format!(
            "SELECT oid::bigint FROM pg_catalog.pg_extension WHERE extname = '{}'",
            extension_name.replace('\'', "''"),
        );
        let oid = Spi::get_one::<i64>(&query)
            .expect("extension OID query failed")
            .expect("extension OID is missing");
        u32::try_from(oid).expect("extension OID exceeds u32")
    }

    fn register_capacity_workers(&mut self, extension_oid: u32, count: usize) {
        // Set this first so Drop removes a partially registered fixture if
        // registration panics partway through the loop.
        self.capacity_workers_registered = true;
        RuntimeTestInjection::register_capacity_workers(
            u32::try_from(self.database_oid).expect("database OID exceeds u32"),
            extension_oid,
            count,
        );
    }

    fn clear_capacity_workers(&mut self) {
        RuntimeTestInjection::clear_capacity_workers(
            u32::try_from(self.database_oid).expect("database OID exceeds u32"),
        );
        self.capacity_workers_registered = false;
    }

    fn wait_for(&self, predicate: &str) {
        self.wait_for_query(
            predicate,
            &format!(
                "SELECT EXISTS (SELECT FROM lakebase.worker_runtime_status WHERE database_oid = {} AND extension_name = 'pg_lakebase_runtime' AND worker_name = 'maintenance' AND ({predicate}))",
                self.database_oid
            ),
        );
    }

    fn wait_for_worker_barrier(&self) {
        self.wait_for_query(
            "maintenance worker is waiting on the advisory test barrier",
            &format!(
                "SELECT EXISTS (SELECT FROM pg_catalog.pg_locks AS held_lock JOIN lakebase.worker_runtime_status AS status ON status.pid = held_lock.pid WHERE status.database_oid = {} AND status.extension_name = 'pg_lakebase_runtime' AND status.worker_name = 'maintenance' AND held_lock.locktype = 'advisory' AND NOT held_lock.granted)",
                self.database_oid
            ),
        );
    }

    fn wait_for_reconciler_barrier(&self) {
        self.wait_for_query(
            "database reconciler is exiting on the advisory test barrier",
            &format!(
                "SELECT EXISTS (SELECT FROM pg_catalog.pg_locks AS held_lock JOIN lakebase.process_runtime_status AS status ON status.pid = held_lock.pid WHERE status.process_kind = 'database_reconciler' AND status.database_oid = {} AND status.state = 'exiting' AND held_lock.locktype = 'advisory' AND NOT held_lock.granted)",
                self.database_oid
            ),
        );
    }

    fn wait_for_injection_barrier(&self) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while !RuntimeTestInjection::barrier_reached() {
            assert!(
                Instant::now() < deadline,
                "runtime fault-injection barrier was not reached"
            );
            Spi::run("SELECT pg_catalog.pg_sleep(0.01)")
                .expect("runtime barrier sleep failed");
        }
    }

    fn request_reconcile(&self) {
        assert!(crate::runtime::request_database_reconcile(
            self.database_oid()
        ));
        crate::runtime::signal_launcher();
    }

    fn reconcile_snapshot(&self) -> Option<(u64, u64, Option<u64>)> {
        RuntimeTestInjection::reconcile_snapshot(self.database_oid())
    }

    fn wait_for_reconcile_complete(&self) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while !RuntimeTestInjection::reconcile_is_complete(self.database_oid()) {
            assert!(
                Instant::now() < deadline,
                "database reconciliation did not consume its durable intent"
            );
            Spi::run("SELECT pg_catalog.pg_sleep(0.01)")
                .expect("reconcile completion sleep failed");
        }
    }

    fn wait_for_query(&self, description: &str, query: &str) {
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
                    "SELECT jsonb_build_object('workers', coalesce((SELECT jsonb_agg(to_jsonb(status)) FROM lakebase.worker_runtime_status AS status), '[]'::jsonb), 'processes', coalesce((SELECT jsonb_agg(to_jsonb(status)) FROM lakebase.process_runtime_status AS status), '[]'::jsonb))::text",
                )
                .expect("runtime diagnostic query failed")
                .unwrap_or_else(|| "{}".to_owned());
                panic!(
                    "runtime predicate timed out: predicate={description}, status={details}"
                );
            }
            Spi::run("SELECT pg_catalog.pg_sleep(0.01)")
                .expect("runtime predicate sleep failed");
        }
    }
}

#[cfg(feature = "pg_test")]
impl Drop for RuntimeWorkerHarness {
    fn drop(&mut self) {
        RuntimeTestInjection::clear();
        if self.lock_held {
            let _ = Spi::run(&format!(
                "SELECT pg_catalog.pg_advisory_unlock({})",
                self.database_oid
            ));
        }
        if self.capacity_workers_registered {
            RuntimeTestInjection::request_capacity_worker_cleanup(
                u32::try_from(self.database_oid).expect("database OID exceeds u32"),
            );
            self.capacity_workers_registered = false;
        }
        if self.suite_lock_held {
            let _ = Spi::run(&format!(
                "SELECT pg_catalog.pg_advisory_unlock({RUNTIME_TEST_SUITE_LOCK})"
            ));
            self.suite_lock_held = false;
        }
    }
}

#[cfg(test)]
pub fn setup(_options: Vec<&str>) {}

#[cfg(test)]
pub fn postgresql_conf_options() -> Vec<&'static str> {
    vec![
        "shared_preload_libraries = 'pg_lakebase_runtime'",
        "max_worker_processes = 32",
    ]
}

#[cfg(feature = "pg_test")]
#[pgrx::pg_schema]
mod tests {
    use super::*;

    #[pgrx::pg_test]
    fn exiting_worker_retains_identity_and_capacity_until_release() {
        let mut harness = RuntimeWorkerHarness::new();
        harness.wait_for(
            "registration_state = 'registered' AND dispatch_state = 'idle' AND process_state = 'stopped'",
        );
        harness.set_injection("hold_after_exiting");
        harness.acquire_worker_barrier();
        harness.wake_maintenance();
        harness.wait_for("process_state = 'exiting' AND pid IS NOT NULL");
        harness.wait_for_worker_barrier();
        let generation = harness.generation();

        harness.wake_maintenance();
        assert_eq!(harness.generation(), generation);
        let active = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM lakebase.worker_runtime_status WHERE database_oid = {} AND process_state IN ('starting', 'running', 'exiting')",
            harness.database_oid
        ))
        .expect("active worker count failed")
        .unwrap_or(-1);
        assert_eq!(active, 1);

        harness.reset_injection();
        harness.release_worker_barrier();
        harness.wait_for(
            &format!(
                "dispatch_state = 'idle' AND process_state = 'stopped' AND generation > {generation}"
            ),
        );
    }

    #[pgrx::pg_test]
    fn exit_cleanup_failure_cannot_apply_a_normal_directive() {
        let harness = RuntimeWorkerHarness::new();
        harness.wait_for("dispatch_state = 'idle' AND process_state = 'stopped'");
        let generation = harness.generation();
        harness.set_injection("fail_exit_cleanup");
        harness.wake_maintenance();
        harness.wait_for(&format!(
            "generation > {generation} AND dispatch_state = 'delayed' AND process_state = 'stopped' AND not_before_ms IS NOT NULL"
        ));
        harness.reset_injection();
    }

    #[pgrx::pg_test]
    fn terminate_before_worker_start_reaches_physical_stop() {
        let harness = RuntimeWorkerHarness::new();
        harness.wait_for("dispatch_state = 'idle' AND process_state = 'stopped'");
        let extension_oid = harness.extension_oid("pg_lakebase_runtime");
        harness.set_injection("hold_before_start");
        harness.wake_maintenance();
        harness.wait_for("process_state = 'starting' AND pid IS NULL");
        harness.wait_for_injection_barrier();
        let generation = harness.generation();

        crate::runtime::stop_worker(
            harness.database_oid(),
            extension_oid,
            "maintenance",
        )
        .expect("terminate-before-start did not reach physical stop");
        harness.wait_for(&format!(
            "generation = {generation} AND process_state = 'stopped' AND stop_requested"
        ));

        harness.reset_injection();
        harness.request_reconcile();
        harness.wait_for_reconcile_complete();
        harness.wait_for(&format!(
            "generation > {generation} AND process_state = 'stopped' AND dispatch_state = 'idle' AND NOT stop_requested"
        ));
    }

    #[pgrx::pg_test]
    fn reconcile_intent_during_exiting_runs_a_later_generation() {
        let mut harness = RuntimeWorkerHarness::new();
        harness.wait_for("process_state = 'stopped'");
        harness.set_injection("hold_reconciler_after_completion");
        harness.acquire_worker_barrier();
        harness.request_reconcile();
        harness.wait_for_reconciler_barrier();

        let (first_desired, first_completed, active_target) = harness
            .reconcile_snapshot()
            .expect("active reconciler has no durable intent");
        let active_target = active_target.expect("reconciler target is missing");
        assert_eq!(active_target, first_desired);
        assert_ne!(first_desired, first_completed);

        harness.request_reconcile();
        let (second_desired, second_completed, still_active_target) = harness
            .reconcile_snapshot()
            .expect("new reconcile intent was not retained");
        assert!(second_desired > active_target);
        assert_eq!(second_completed, first_completed);
        assert_eq!(still_active_target, Some(active_target));

        harness.reset_injection();
        harness.release_worker_barrier();
        harness.wait_for_reconcile_complete();
    }

    #[pgrx::pg_test]
    fn exiting_workers_do_not_oversubscribe_configured_capacity() {
        let mut harness = RuntimeWorkerHarness::new();
        harness.wait_for("dispatch_state = 'idle' AND process_state = 'stopped'");
        let configured = Spi::get_one::<i32>(
            "SELECT pg_catalog.current_setting('pg_lakebase.max_active_workers')::integer",
        )
        .expect("max_active_workers query failed")
        .expect("max_active_workers is null");
        harness.set_injection("hold_after_running");
        harness.acquire_worker_barrier();
        let extension_oid = Spi::get_one::<i64>(
            "SELECT oid::bigint FROM pg_catalog.pg_extension WHERE extname = 'pg_lakebase_runtime'",
        )
        .expect("runtime extension OID query failed")
        .expect("runtime extension OID is null");
        harness.register_capacity_workers(
            u32::try_from(extension_oid).expect("extension OID exceeds u32"),
            usize::try_from(
                configured
                    .checked_add(1)
                    .expect("configured capacity exceeds i32"),
            )
            .expect("configured capacity is negative"),
        );
        harness.wait_for_query(
            "configured capacity is full and another worker remains ready",
            &format!(
                "SELECT (SELECT count(*) FROM lakebase.worker_runtime_status WHERE database_oid = {} AND worker_name LIKE 'supervisor_capacity_%' AND process_state IN ('starting', 'running', 'exiting')) = {} AND EXISTS (SELECT FROM lakebase.worker_runtime_status WHERE database_oid = {} AND worker_name LIKE 'supervisor_capacity_%' AND dispatch_state = 'ready' AND process_state = 'stopped')",
                harness.database_oid, configured, harness.database_oid
            ),
        );
        harness.wait_for_query(
            "a capacity-test worker is waiting on the advisory barrier",
            &format!(
                "SELECT EXISTS (SELECT FROM pg_catalog.pg_locks AS held_lock JOIN lakebase.worker_runtime_status AS status ON status.pid = held_lock.pid WHERE status.database_oid = {} AND status.worker_name LIKE 'supervisor_capacity_%' AND held_lock.locktype = 'advisory' AND NOT held_lock.granted)",
                harness.database_oid
            ),
        );

        harness.reset_injection();
        harness.release_worker_barrier();
        harness.wait_for_query(
            "capacity-test workers have physically stopped",
            &format!(
                "SELECT NOT EXISTS (SELECT FROM lakebase.worker_runtime_status WHERE database_oid = {} AND worker_name LIKE 'supervisor_capacity_%' AND process_state IN ('starting', 'running', 'exiting'))",
                harness.database_oid
            ),
        );
        harness.clear_capacity_workers();
    }

    #[pgrx::pg_test]
    fn launcher_restart_drains_previous_generation_before_resuming() {
        let mut harness = RuntimeWorkerHarness::new();
        harness.wait_for("dispatch_state = 'idle' AND process_state = 'stopped'");
        harness.set_injection("hold_after_running");
        harness.acquire_worker_barrier();
        harness.wake_maintenance();
        harness.wait_for("process_state = 'running' AND pid IS NOT NULL");
        harness.wait_for_worker_barrier();
        let old_launcher_pid = harness.launcher_pid();
        let old_worker_pid = harness.worker_pid();
        let old_generation = harness.generation();

        let terminated = Spi::get_one::<bool>(
            "SELECT pg_catalog.pg_terminate_backend(pid) FROM lakebase.process_runtime_status WHERE process_kind = 'launcher'",
        )
        .expect("launcher termination query failed")
        .unwrap_or(false);
        assert!(terminated, "launcher was not terminated");

        harness.reset_injection();
        harness.release_worker_barrier();
        harness.wait_for_query(
            "the restarted launcher drained the old backend and converged",
            &format!(
                "SELECT (SELECT pid <> {old_launcher_pid} AND state = 'running' AND recovery_backend_count = 0 FROM lakebase.process_runtime_status WHERE process_kind = 'launcher') AND NOT EXISTS (SELECT FROM pg_catalog.pg_stat_activity WHERE pid = {old_worker_pid}) AND EXISTS (SELECT FROM lakebase.worker_runtime_status WHERE database_oid = {} AND extension_name = 'pg_lakebase_runtime' AND worker_name = 'maintenance' AND recovery_state = 'ready' AND generation > {old_generation} AND process_state = 'stopped')",
                harness.database_oid
            ),
        );
    }
}
