use std::time::Duration;

use lagodb_core::extension_worker::WorkerSchedule;
use pgrx::PGRXSharedMemory;

use super::{
    INVALID_OID, MAX_WORKER_NAME_BYTES, ProcessState, RegistrationState,
    RestartPolicy,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkerStopDisposition {
    Settled,
    Reconcile,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct Slot {
    pub(crate) database_oid: u32,
    pub(crate) worker_id: i32,
    pub(crate) extension_oid: u32,
    pub(crate) pid: i32,
    pub(crate) restart_after_ms: i64,
    pub(crate) start_time_ms: i64,
    pub(crate) failure_count: i32,
    pub(crate) worker_name_len: u16,
    pub(crate) registration_state: RegistrationState,
    pub(crate) process_state: ProcessState,
    pub(crate) wake_requested: u8,
    stop_requested: u8,
    needs_restart: u8,
    pub(crate) _padding: [u8; 1],
    pub(crate) worker_name: [u8; MAX_WORKER_NAME_BYTES],
}

// SAFETY: Slot is repr(C), Copy, and contains only scalar values, repr(u8)
// enums, and a fixed byte array. Its identity is the leading key fields.
unsafe impl PGRXSharedMemory for Slot {}

#[derive(Clone, Copy)]
pub(crate) struct Identity {
    pub(crate) database_oid: u32,
    pub(crate) worker_id: i32,
    pub(crate) extension_oid: u32,
    worker_name_len: u16,
    worker_name: [u8; MAX_WORKER_NAME_BYTES],
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(C)]
pub(crate) struct WorkerKey {
    pub(crate) database_oid: u32,
    pub(crate) worker_id: i32,
}

impl WorkerKey {
    pub(crate) const fn new(database_oid: u32, worker_id: i32) -> Self {
        Self {
            database_oid,
            worker_id,
        }
    }
}

impl Identity {
    pub(crate) fn worker_name(&self) -> &str {
        let len = usize::from(self.worker_name_len);
        // SAFETY: initialize_registration copies this prefix from a Rust `&str`.
        unsafe { std::str::from_utf8_unchecked(&self.worker_name[..len]) }
    }
}

impl Slot {
    pub(crate) const EMPTY: Self = Self {
        database_oid: INVALID_OID,
        worker_id: 0,
        extension_oid: INVALID_OID,
        pid: 0,
        restart_after_ms: 0,
        start_time_ms: 0,
        failure_count: 0,
        worker_name_len: 0,
        registration_state: RegistrationState::Empty,
        process_state: ProcessState::Stopped,
        wake_requested: 0,
        stop_requested: 0,
        needs_restart: 0,
        _padding: [0; 1],
        worker_name: [0; MAX_WORKER_NAME_BYTES],
    };

    pub(crate) const fn new(key: WorkerKey) -> Self {
        Self {
            database_oid: key.database_oid,
            worker_id: key.worker_id,
            ..Self::EMPTY
        }
    }

    pub(crate) const fn key(&self) -> WorkerKey {
        WorkerKey::new(self.database_oid, self.worker_id)
    }

    pub(crate) const fn registration(&self) -> RegistrationState {
        self.registration_state
    }

    pub(crate) const fn process(&self) -> ProcessState {
        self.process_state
    }

    pub(crate) const fn is_stop_requested(&self) -> bool {
        self.stop_requested != 0
    }

    pub(crate) const fn needs_restart(&self) -> bool {
        self.needs_restart != 0
    }

    pub(crate) fn has_active_process(&self) -> bool {
        self.process() == ProcessState::Starting || self.pid > 0
    }

    pub(crate) fn restart_delay(&self, now_ms: i64) -> Option<Duration> {
        let registration = self.registration();
        let process = self.process();
        if registration != RegistrationState::Registered
            || process == ProcessState::Starting
            || self.pid > 0
            || self.stop_requested != 0
            || self.needs_restart == 0
        {
            return None;
        }
        let delay_ms = self.restart_after_ms.saturating_sub(now_ms).max(0);
        Some(Duration::from_millis(
            u64::try_from(delay_ms).expect("nonnegative restart delay exceeds u64"),
        ))
    }

    pub(crate) const fn identity(&self) -> Identity {
        Identity {
            database_oid: self.database_oid,
            worker_id: self.worker_id,
            extension_oid: self.extension_oid,
            worker_name_len: self.worker_name_len,
            worker_name: self.worker_name,
        }
    }

    pub(crate) fn initialize_registration(
        &mut self,
        extension_oid: u32,
        worker_name: &str,
    ) {
        let bytes = worker_name.as_bytes();
        let len = u16::try_from(bytes.len())
            .expect("worker catalog name length exceeds u16");
        let key = self.key();
        *self = Self {
            database_oid: key.database_oid,
            worker_id: key.worker_id,
            extension_oid,
            registration_state: RegistrationState::Registered,
            process_state: ProcessState::NotStarted,
            needs_restart: 1,
            ..Self::EMPTY
        };
        self.worker_name[..bytes.len()].copy_from_slice(bytes);
        self.worker_name_len = len;
    }

    pub(crate) fn request_wakeup(&mut self) {
        self.wake_requested = 1;
        if self.process() != ProcessState::Starting
            && self.pid == 0
            && self.registration() == RegistrationState::Registered
            && self.stop_requested == 0
            && !(self.failure_count > 0 && self.restart_after_ms > 0)
        {
            self.needs_restart = 1;
            self.restart_after_ms = 0;
            self.wake_requested = 0;
        }
    }

    pub(crate) fn reconcile_present(&mut self) {
        self.registration_state = RegistrationState::Registered;
        if self.stop_requested != 0 {
            if self.has_active_process() {
                // SIGTERM cannot be withdrawn. Keep the stop gate closed until
                // the old invocation has physically exited; its exit callback
                // will request another catalog reconciliation.
                return;
            }
            self.stop_requested = 0;
            if self.wake_requested != 0 {
                self.request_wakeup();
            } else if self.needs_restart != 0 {
                // A rolled-back transactional stop restores the immediate or
                // RunAfter schedule preserved when termination was requested.
                self.schedule_existing_restart();
            } else {
                self.process_state = ProcessState::Stopped;
            }
        } else if self.wake_requested != 0 {
            self.request_wakeup();
        }
    }

    pub(crate) fn mark_removing(&mut self) {
        self.registration_state = RegistrationState::Removing;
        self.request_stop();
    }

    pub(crate) fn prepare_start(&mut self) {
        self.process_state = ProcessState::Starting;
        self.needs_restart = 0;
        self.wake_requested = 0;
        self.restart_after_ms = 0;
        self.start_time_ms = 0;
    }

    pub(crate) fn mark_running(&mut self, pid: i32, start_time_ms: i64) -> bool {
        let current = self.process();
        if current != ProcessState::Starting
            || self.stop_requested != 0
            || self.wake_requested != 0
        {
            return false;
        }
        self.process_state = ProcessState::Running;
        self.pid = pid;
        self.needs_restart = 0;
        self.start_time_ms = start_time_ms;
        true
    }

    pub(crate) fn complete_run(&mut self, schedule: WorkerSchedule, now_ms: i64) {
        match schedule {
            WorkerSchedule::Idle => {}
            WorkerSchedule::RunImmediately => {
                self.needs_restart = 1;
                self.restart_after_ms = 0;
                self.process_state = ProcessState::Restarting;
            }
            WorkerSchedule::RunAfter(delay) => {
                let delay_ms = i64::try_from(delay.as_millis()).unwrap_or(i64::MAX);
                self.needs_restart = 1;
                self.restart_after_ms = now_ms.saturating_add(delay_ms);
                self.process_state = ProcessState::Restarting;
            }
        }
    }

    pub(crate) fn request_stop(&mut self) {
        self.stop_requested = 1;
        let current = self.process();
        if current == ProcessState::Running {
            self.process_state = ProcessState::Restarting;
        }
    }

    pub(crate) fn prepare_transactional_stop(&mut self) -> bool {
        let process = self.process();
        if process != ProcessState::Starting && self.pid == 0 {
            return false;
        }
        self.needs_restart = 1;
        self.request_stop();
        true
    }

    pub(crate) fn confirm_stopped(
        &mut self,
        now_ms: i64,
        restart_policy: &RestartPolicy,
        exit_code: i32,
    ) -> Option<WorkerStopDisposition> {
        let process = self.process();
        if process != ProcessState::Starting && self.pid == 0 {
            return None;
        }
        let explicit_stop = self.stop_requested != 0;
        let restart_requested = self.needs_restart != 0;
        let stopped_normally = exit_code == 0;
        let start_time_ms = self.start_time_ms;
        let needs_reconcile = explicit_stop
            || self.registration() != RegistrationState::Registered
            || restart_requested
            || self.wake_requested != 0
            || !stopped_normally;

        self.pid = 0;
        self.start_time_ms = 0;
        if stopped_normally {
            // Every clean invocation breaks a crash loop, including one
            // terminated as part of a transactional catalog change.
            self.failure_count = 0;
        }

        if self.registration() != RegistrationState::Registered {
            self.process_state = ProcessState::Stopped;
            self.stop_requested = 0;
            self.needs_restart = 0;
            self.restart_after_ms = 0;
            self.wake_requested = 0;
        } else if explicit_stop {
            // The transaction outcome is not known here. Preserve both the
            // stop gate and the pre-stop schedule until a catalog snapshot
            // confirms whether the registration survived.
            self.schedule_existing_restart();
        } else if stopped_normally {
            if self.wake_requested != 0 {
                self.process_state = ProcessState::Restarting;
                self.needs_restart = 1;
                self.restart_after_ms = 0;
                self.wake_requested = 0;
            } else if restart_requested {
                self.schedule_existing_restart();
            } else {
                self.process_state = ProcessState::Stopped;
                self.needs_restart = 0;
                self.restart_after_ms = 0;
            }
        } else if restart_requested {
            self.schedule_existing_restart();
        } else {
            self.process_state = ProcessState::Restarting;
            self.failure_count = restart_policy.failure_count_after_crash(
                self.failure_count,
                start_time_ms,
                now_ms,
            );
            self.needs_restart = 1;
            let backoff = restart_policy.crash_backoff(self.failure_count);
            self.restart_after_ms = now_ms.saturating_add(
                i64::try_from(backoff.as_millis()).unwrap_or(i64::MAX),
            );
        }
        Some(if needs_reconcile {
            WorkerStopDisposition::Reconcile
        } else {
            WorkerStopDisposition::Settled
        })
    }

    pub(crate) fn registration_failed(
        &mut self,
        now_ms: i64,
        retry_delay: Duration,
    ) -> bool {
        let pending = self.restart_delay(now_ms) == Some(Duration::ZERO);
        if !self.has_active_process() && !pending {
            return false;
        }
        self.pid = 0;
        if self.stop_requested != 0 {
            // Registration/startup failure can race a transactional stop.
            // Reconciliation, not this callback, decides whether to restore
            // or remove the preserved restart request.
            self.schedule_existing_restart();
        } else {
            self.process_state = ProcessState::NotStarted;
            self.restart_after_ms = now_ms.saturating_add(
                i64::try_from(retry_delay.as_millis()).unwrap_or(i64::MAX),
            );
            self.needs_restart = 1;
        }
        self.start_time_ms = 0;
        true
    }

    fn schedule_existing_restart(&mut self) {
        self.process_state = ProcessState::Restarting;
        self.needs_restart = 1;
    }
}
