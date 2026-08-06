use std::collections::HashSet;
use std::time::Duration;

use pg_lakebase_core::extension_worker::WorkerSchedule;

use crate::worker::CAPACITY_RETRY;
use crate::worker::scheduler::Scheduler;
use crate::worker::state::{
    ProcessState, RegistrationState, RestartPolicy, Slot, WorkerKey,
};

const NOW_MS: i64 = 1_000_000;
const RUN_AFTER: Duration = Duration::from_secs(60);

fn restart_policy() -> RestartPolicy {
    RestartPolicy::new(
        Duration::from_secs(5),
        Duration::from_secs(60),
        Duration::from_secs(30),
    )
}

fn registered_slot(worker_id: i32) -> Slot {
    let mut slot = Slot::new(WorkerKey::new(1, worker_id));
    slot.initialize_registration(2, "transactional-stop-test");
    slot
}

#[test]
fn transactional_stop_preserves_running_worker_schedule_in_both_exit_orders() {
    let expected_deadline = NOW_MS + i64::try_from(RUN_AFTER.as_millis()).unwrap();

    let mut exit_first = registered_slot(1);
    exit_first.prepare_start();
    assert!(exit_first.mark_running(101, NOW_MS));
    exit_first.failure_count = 3;
    exit_first.complete_run(WorkerSchedule::RunAfter(RUN_AFTER), NOW_MS);
    assert!(exit_first.prepare_transactional_stop());
    assert!(exit_first.is_stop_requested());
    assert!(exit_first.needs_restart());
    assert_eq!(exit_first.restart_after_ms, expected_deadline);

    assert!(
        exit_first
            .confirm_stopped(NOW_MS + 1, &restart_policy(), 1)
            .is_some()
    );
    assert!(exit_first.is_stop_requested());
    assert!(exit_first.needs_restart());
    assert_eq!(exit_first.failure_count, 3);
    assert_eq!(exit_first.restart_after_ms, expected_deadline);
    exit_first.reconcile_present();
    assert!(!exit_first.is_stop_requested());
    assert!(exit_first.needs_restart());
    assert_eq!(exit_first.process(), ProcessState::Restarting);
    assert_eq!(exit_first.restart_after_ms, expected_deadline);

    let mut reconcile_first = registered_slot(2);
    reconcile_first.prepare_start();
    assert!(reconcile_first.mark_running(102, NOW_MS));
    reconcile_first.complete_run(WorkerSchedule::RunAfter(RUN_AFTER), NOW_MS);
    assert!(reconcile_first.prepare_transactional_stop());
    reconcile_first.reconcile_present();
    assert!(reconcile_first.is_stop_requested());
    assert_eq!(reconcile_first.restart_after_ms, expected_deadline);

    assert!(
        reconcile_first
            .confirm_stopped(NOW_MS + 1, &restart_policy(), 1)
            .is_some()
    );
    assert!(reconcile_first.is_stop_requested());
    reconcile_first.reconcile_present();
    assert!(!reconcile_first.is_stop_requested());
    assert!(reconcile_first.needs_restart());
    assert_eq!(reconcile_first.process(), ProcessState::Restarting);
    assert_eq!(reconcile_first.restart_after_ms, expected_deadline);
}

#[test]
fn transactional_stop_preserves_starting_worker_in_both_exit_orders() {
    let mut exit_first = registered_slot(3);
    exit_first.prepare_start();
    exit_first.failure_count = 2;
    assert!(exit_first.prepare_transactional_stop());
    assert!(
        exit_first
            .confirm_stopped(NOW_MS, &restart_policy(), 0)
            .is_some()
    );
    assert_eq!(exit_first.failure_count, 0);
    assert!(exit_first.is_stop_requested());
    assert!(exit_first.needs_restart());
    exit_first.reconcile_present();
    assert!(!exit_first.is_stop_requested());
    assert_eq!(exit_first.restart_delay(NOW_MS), Some(Duration::ZERO));

    let mut reconcile_first = registered_slot(4);
    reconcile_first.prepare_start();
    assert!(reconcile_first.prepare_transactional_stop());
    reconcile_first.reconcile_present();
    assert!(reconcile_first.is_stop_requested());
    assert!(
        reconcile_first
            .confirm_stopped(NOW_MS, &restart_policy(), 0)
            .is_some()
    );
    assert!(reconcile_first.is_stop_requested());
    reconcile_first.reconcile_present();
    assert!(!reconcile_first.is_stop_requested());
    assert_eq!(reconcile_first.restart_delay(NOW_MS), Some(Duration::ZERO));
}

#[test]
fn transactional_stop_registration_failure_waits_for_reconciliation() {
    let mut stopped = registered_slot(5);
    stopped.prepare_start();
    assert!(stopped.prepare_transactional_stop());
    assert!(stopped.registration_failed(NOW_MS, CAPACITY_RETRY));
    assert!(stopped.is_stop_requested());
    assert!(stopped.needs_restart());
    assert_eq!(stopped.restart_after_ms, 0);
    stopped.reconcile_present();
    assert!(!stopped.is_stop_requested());
    assert_eq!(stopped.restart_delay(NOW_MS), Some(Duration::ZERO));

    let mut capacity_failure = registered_slot(6);
    assert!(capacity_failure.registration_failed(NOW_MS, CAPACITY_RETRY));
    assert_eq!(
        capacity_failure.restart_after_ms,
        NOW_MS + i64::try_from(CAPACITY_RETRY.as_millis()).unwrap(),
    );
}

#[test]
fn confirmed_removal_clears_schedule_and_clean_exit_resets_failures() {
    let mut slot = registered_slot(7);
    slot.prepare_start();
    assert!(slot.mark_running(107, NOW_MS));
    slot.failure_count = 4;
    slot.complete_run(WorkerSchedule::RunAfter(RUN_AFTER), NOW_MS);
    assert!(slot.prepare_transactional_stop());
    slot.mark_removing();
    assert!(
        slot.confirm_stopped(NOW_MS + 1, &restart_policy(), 0)
            .is_some()
    );

    assert_eq!(slot.registration(), RegistrationState::Removing);
    assert_eq!(slot.process(), ProcessState::Stopped);
    assert_eq!(slot.failure_count, 0);
    assert!(!slot.is_stop_requested());
    assert!(!slot.needs_restart());
    assert_eq!(slot.restart_after_ms, 0);
}

#[test]
fn idle_completion_remains_running_until_the_exit_callback() {
    let mut slot = registered_slot(8);
    slot.prepare_start();
    assert!(slot.mark_running(108, NOW_MS));
    slot.complete_run(WorkerSchedule::Idle, NOW_MS);
    assert_eq!(slot.process(), ProcessState::Running);
    assert!(
        slot.confirm_stopped(NOW_MS + 1, &restart_policy(), 0)
            .is_some()
    );
    assert_eq!(slot.process(), ProcessState::Stopped);
}

#[test]
fn scheduler_rejects_databases_outside_the_candidate_snapshot() {
    let mut scheduler = Scheduler::new();
    scheduler.enqueue(10);
    assert_eq!(scheduler.len(), 0);

    let candidates = HashSet::from([10]);
    assert_eq!(scheduler.reconcile_live(&candidates), vec![10]);
    scheduler.enqueue(10);
    assert_eq!(scheduler.pop_front(), Some(10));

    scheduler.reconcile_live(&HashSet::new());
    scheduler.enqueue(10);
    assert_eq!(scheduler.len(), 0);
}
