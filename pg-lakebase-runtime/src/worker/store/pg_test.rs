use pgrx::pg_sys;

use crate::worker::INVALID_OID;
use crate::worker::state::{CoordinatorStopDisposition, ProcessState, SharedState};

use super::{COORDINATOR_TABLE, SHARED_STATE, Store};

struct CoordinatorFixtures {
    database_oids: Vec<u32>,
}

impl CoordinatorFixtures {
    fn new() -> Self {
        Self {
            database_oids: Vec::new(),
        }
    }

    fn assert_coordination_routing(&mut self) {
        let mut state = SHARED_STATE.exclusive();
        let original_rescan_all = state.rescan_all;
        state.rescan_all = 0;

        let missing = self.unused_database_oid();
        assert!(
            Store::request_coordination_locked(&mut state, missing),
            "a missing coordinator must fall back to the supervisor",
        );
        assert_eq!(state.rescan_all, 1, "fallback must request a full scan");
        assert!(
            COORDINATOR_TABLE.find(missing).is_none(),
            "a notification must not create a coordinator entry",
        );
        state.rescan_all = 0;

        let stopped = self.install_locked(&mut state, ProcessState::Stopped);
        assert!(
            Store::request_coordination_locked(&mut state, stopped),
            "a stopped coordinator must fall back to the supervisor",
        );
        assert_eq!(state.rescan_all, 1, "fallback must request a full scan");
        state.rescan_all = 0;

        let starting = self.install_locked(&mut state, ProcessState::Starting);
        assert!(
            Store::request_coordination_locked(&mut state, starting),
            "a coordinator without a published ProcNumber must fall back to the supervisor",
        );
        assert_eq!(state.rescan_all, 1, "fallback must request a full scan");
        state.rescan_all = 0;

        let running = self.install_locked(&mut state, ProcessState::Running);
        assert!(
            !Store::request_coordination_locked(&mut state, running),
            "a live coordinator must consume the notification without a supervisor wake",
        );
        assert_eq!(
            state.rescan_all, 0,
            "direct coordinator notification must not request a full scan",
        );

        for database_oid in [stopped, starting, running] {
            assert!(
                COORDINATOR_TABLE
                    .find(database_oid)
                    .expect("coordination test slot disappeared")
                    .needs_restart(),
                "coordination request was not persisted for database {database_oid}",
            );
        }
        self.remove_locked(&mut state, [stopped, starting, running]);
        state.rescan_all = original_rescan_all;
    }

    fn assert_exit_dispositions(&mut self) {
        let mut state = SHARED_STATE.exclusive();
        let pending = self.install_locked(&mut state, ProcessState::Running);
        assert!(!Store::request_coordination_locked(&mut state, pending));
        assert_eq!(
            COORDINATOR_TABLE
                .with_mut(pending, |slot| slot.confirm_stopped(0))
                .flatten(),
            Some(CoordinatorStopDisposition::HandoffNow),
            "the exit callback must hand a pending request to the supervisor",
        );

        let completed = self.install_locked(&mut state, ProcessState::Running);
        assert_eq!(
            COORDINATOR_TABLE
                .with_mut(completed, |slot| slot.confirm_stopped(0))
                .flatten(),
            Some(CoordinatorStopDisposition::Settled),
            "normal coordinator completion must not publish another request",
        );

        let crashed = self.install_locked(&mut state, ProcessState::Running);
        assert_eq!(
            COORDINATOR_TABLE
                .with_mut(crashed, |slot| slot.confirm_stopped(1))
                .flatten(),
            Some(CoordinatorStopDisposition::Failed),
            "a failed coordinator must retain work for its exit notification",
        );
        let crashed_slot = COORDINATOR_TABLE
            .find(crashed)
            .expect("crashed coordinator transition disappeared");
        assert!(
            crashed_slot.needs_restart()
                && crashed_slot.process() == ProcessState::Stopped,
            "a failed coordinator must remain visible to the supervisor scan",
        );
        self.remove_locked(&mut state, [pending, completed, crashed]);
    }

    fn unused_database_oid(&self) -> u32 {
        (1..u32::MAX)
            .rev()
            .find(|database_oid| {
                *database_oid != INVALID_OID
                    && COORDINATOR_TABLE.find(*database_oid).is_none()
            })
            .expect("no unused coordinator test key is available")
    }

    fn install_locked(
        &mut self,
        _state: &mut SharedState,
        process: ProcessState,
    ) -> u32 {
        let database_oid = self.unused_database_oid();
        let mut slot = COORDINATOR_TABLE.get_or_insert(database_oid);
        match process {
            ProcessState::Stopped => {}
            ProcessState::Starting => slot.reserve(),
            ProcessState::Running => {
                slot.reserve();
                // SAFETY: this runs in a PostgreSQL backend with a published
                // PID and ProcNumber. Cleanup happens under the same lock.
                assert!(slot.mark_running(unsafe { pg_sys::MyProcPid }, unsafe {
                    pg_sys::MyProcNumber
                }));
            }
            _ => panic!("unsupported test coordinator state: {process:?}"),
        }
        assert!(COORDINATOR_TABLE.replace(slot));
        self.database_oids.push(database_oid);
        database_oid
    }

    fn remove_locked<const N: usize>(
        &mut self,
        _state: &mut SharedState,
        database_oids: [u32; N],
    ) {
        for database_oid in database_oids {
            COORDINATOR_TABLE.remove(database_oid);
            self.database_oids
                .retain(|candidate| *candidate != database_oid);
        }
    }
}

impl Drop for CoordinatorFixtures {
    fn drop(&mut self) {
        if self.database_oids.is_empty() {
            return;
        }
        let _state = SHARED_STATE.exclusive();
        for database_oid in self.database_oids.drain(..) {
            COORDINATOR_TABLE.remove(database_oid);
        }
    }
}

#[pgrx::pg_test(schema = "tests")]
fn coordination_notifications_route_to_one_control_process() {
    CoordinatorFixtures::new().assert_coordination_routing();
}

#[pgrx::pg_test(schema = "tests")]
fn coordinator_exit_routes_handoff_and_failure_by_disposition() {
    CoordinatorFixtures::new().assert_exit_dispositions();
}
