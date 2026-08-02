use std::time::Duration;

use pg_lakebase_core::extension_worker::WorkerDirective;
use pgrx::pg_sys;

use super::{
    DispatchState, INVALID_OID, MAX_WORKER_NAME_BYTES, ProcessState,
    RegistrationState, RuntimeStateDecodeError, RuntimeStateKind,
    RuntimeStateTransitionError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum PendingDirective {
    None = 0,
    Idle = 1,
    RunImmediately = 2,
    RunAfter = 3,
}

impl PendingDirective {
    fn decode(raw: u8) -> Result<Self, RuntimeStateDecodeError> {
        match raw {
            0 => Ok(Self::None),
            1 => Ok(Self::Idle),
            2 => Ok(Self::RunImmediately),
            3 => Ok(Self::RunAfter),
            _ => Err(RuntimeStateDecodeError::new(
                RuntimeStateKind::PendingDirective,
                raw,
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WorkerCompletion {
    pub(crate) stopped_normally: bool,
    pub(crate) explicit_stop: bool,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct WorkerSlot {
    pub(crate) database_oid: u32,
    pub(crate) extension_oid: u32,
    pub(crate) generation: u32,
    pub(crate) pid: i32,
    pub(crate) proc_number: i32,
    pub(crate) not_before_ms: i64,
    pub(crate) startup_deadline_ms: i64,
    pub(crate) pending_delay_ms: i64,
    pub(crate) worker_name_len: u16,
    pub(crate) registration_state: u8,
    pub(crate) dispatch_state: u8,
    pub(crate) process_state: u8,
    pub(crate) wake_requested: u8,
    pub(crate) stop_requested: u8,
    pub(crate) pending_directive: u8,
    pub(crate) pending_directive_valid: u8,
    pub(crate) exit_callback_seen: u8,
    pub(crate) exit_code: i32,
    pub(crate) _padding: [u8; 4],
    pub(crate) worker_name: [u8; MAX_WORKER_NAME_BYTES],
}

#[derive(Clone, Copy)]
pub(crate) struct WorkerIdentity {
    pub(crate) database_oid: u32,
    pub(crate) extension_oid: u32,
    worker_name_len: u16,
    worker_name: [u8; MAX_WORKER_NAME_BYTES],
}

impl WorkerIdentity {
    pub(crate) fn worker_name(&self) -> &str {
        let len = usize::from(self.worker_name_len).min(MAX_WORKER_NAME_BYTES);
        std::str::from_utf8(&self.worker_name[..len]).unwrap_or("<invalid utf8>")
    }
}

impl WorkerSlot {
    pub(crate) const EMPTY: Self = Self {
        database_oid: INVALID_OID,
        extension_oid: INVALID_OID,
        generation: 0,
        pid: 0,
        proc_number: pg_sys::INVALID_PROC_NUMBER,
        not_before_ms: 0,
        startup_deadline_ms: 0,
        pending_delay_ms: 0,
        worker_name_len: 0,
        registration_state: RegistrationState::Empty as u8,
        dispatch_state: DispatchState::Idle as u8,
        process_state: ProcessState::Stopped as u8,
        wake_requested: 0,
        stop_requested: 0,
        pending_directive: PendingDirective::None as u8,
        pending_directive_valid: 0,
        exit_callback_seen: 0,
        exit_code: 0,
        _padding: [0; 4],
        worker_name: [0; MAX_WORKER_NAME_BYTES],
    };

    pub(crate) fn registration(
        &self,
    ) -> Result<RegistrationState, RuntimeStateDecodeError> {
        RegistrationState::decode(self.registration_state)
    }

    pub(crate) fn dispatch(&self) -> Result<DispatchState, RuntimeStateDecodeError> {
        DispatchState::decode(self.dispatch_state)
    }

    pub(crate) fn process(&self) -> Result<ProcessState, RuntimeStateDecodeError> {
        ProcessState::decode(self.process_state)
    }

    pub(crate) fn validate(&self) -> Result<(), RuntimeStateDecodeError> {
        self.registration()?;
        self.dispatch()?;
        self.process()?;
        PendingDirective::decode(self.pending_directive)?;
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.registration() == Ok(RegistrationState::Empty)
    }

    pub(crate) fn worker_name(&self) -> &[u8] {
        let len = usize::from(self.worker_name_len).min(MAX_WORKER_NAME_BYTES);
        &self.worker_name[..len]
    }

    pub(crate) fn worker_name_str(&self) -> &str {
        std::str::from_utf8(self.worker_name()).unwrap_or("<invalid utf8>")
    }

    pub(crate) const fn identity(&self) -> WorkerIdentity {
        WorkerIdentity {
            database_oid: self.database_oid,
            extension_oid: self.extension_oid,
            worker_name_len: self.worker_name_len,
            worker_name: self.worker_name,
        }
    }

    pub(crate) fn matches_worker(
        &self,
        database_oid: u32,
        extension_oid: u32,
        worker_name: &str,
    ) -> bool {
        self.database_oid == database_oid
            && self.extension_oid == extension_oid
            && self.worker_name() == worker_name.as_bytes()
            && !self.is_empty()
    }

    pub(crate) fn initialize_registration(
        &mut self,
        database_oid: u32,
        extension_oid: u32,
        worker_name: &str,
        generation: u32,
        state: RegistrationState,
    ) -> bool {
        let bytes = worker_name.as_bytes();
        let Ok(len) = u16::try_from(bytes.len()) else {
            return false;
        };
        if bytes.is_empty() || bytes.len() > MAX_WORKER_NAME_BYTES {
            return false;
        }
        *self = Self {
            database_oid,
            extension_oid,
            generation,
            registration_state: state as u8,
            ..Self::EMPTY
        };
        self.worker_name[..bytes.len()].copy_from_slice(bytes);
        self.worker_name_len = len;
        true
    }

    pub(crate) fn begin_registration_replacement(
        &mut self,
    ) -> Result<(), RuntimeStateTransitionError> {
        let process = self.process().map_err(|_| {
            RuntimeStateTransitionError::worker("corrupt", "pending_commit")
        })?;
        let registration = self.registration().map_err(|_| {
            RuntimeStateTransitionError::worker("corrupt", "pending_commit")
        })?;
        if process != ProcessState::Stopped
            || !matches!(registration, RegistrationState::Registered)
        {
            return Err(RuntimeStateTransitionError::worker(
                registration.as_str(),
                RegistrationState::PendingCommit.as_str(),
            ));
        }
        self.registration_state = RegistrationState::PendingCommit as u8;
        self.generation = self.generation.wrapping_add(1).max(1);
        self.reset_invocation();
        Ok(())
    }

    pub(crate) fn finish_registration(&mut self) {
        self.registration_state = RegistrationState::Registered as u8;
        self.stop_requested = 0;
        self.publish_wakeup();
    }

    pub(crate) fn publish_wakeup(&mut self) {
        self.wake_requested = 1;
        if self.process() == Ok(ProcessState::Stopped)
            && self.registration() == Ok(RegistrationState::Registered)
            && self.stop_requested == 0
        {
            self.dispatch_state = DispatchState::Ready as u8;
            self.not_before_ms = 0;
        }
    }

    pub(crate) fn promote_due(&mut self, now_ms: i64) {
        if self.dispatch() == Ok(DispatchState::Delayed)
            && self.not_before_ms <= now_ms
            && self.stop_requested == 0
        {
            self.dispatch_state = DispatchState::Ready as u8;
            self.not_before_ms = 0;
        }
    }

    pub(crate) fn reconcile_present(&mut self) {
        if self.registration() == Ok(RegistrationState::PendingCommit) {
            return;
        }
        self.registration_state = RegistrationState::Registered as u8;
        if self.stop_requested != 0 && self.process() == Ok(ProcessState::Stopped) {
            self.stop_requested = 0;
            self.publish_wakeup();
        } else if self.wake_requested != 0 {
            self.publish_wakeup();
        }
    }

    pub(crate) fn mark_removing(
        &mut self,
    ) -> Result<(), RuntimeStateTransitionError> {
        self.registration_state = RegistrationState::Removing as u8;
        self.request_stop()
    }

    pub(crate) fn prepare_start(
        &mut self,
        startup_deadline_ms: i64,
    ) -> Result<u32, RuntimeStateTransitionError> {
        let registration = self.registration().map_err(|_| {
            RuntimeStateTransitionError::worker("corrupt", "starting")
        })?;
        let dispatch = self.dispatch().map_err(|_| {
            RuntimeStateTransitionError::worker("corrupt", "starting")
        })?;
        let process = self.process().map_err(|_| {
            RuntimeStateTransitionError::worker("corrupt", "starting")
        })?;
        if registration != RegistrationState::Registered
            || dispatch != DispatchState::Ready
            || process != ProcessState::Stopped
            || self.stop_requested != 0
        {
            return Err(RuntimeStateTransitionError::worker(
                process.as_str(),
                ProcessState::Starting.as_str(),
            ));
        }
        self.generation = self.generation.wrapping_add(1).max(1);
        self.process_state = ProcessState::Starting as u8;
        self.dispatch_state = DispatchState::Idle as u8;
        self.wake_requested = 0;
        self.startup_deadline_ms = startup_deadline_ms;
        self.reset_completion_evidence();
        Ok(self.generation)
    }

    pub(crate) fn token_matches(&self, generation: u32) -> bool {
        self.generation == generation
    }

    pub(crate) fn publish_running(
        &mut self,
        generation: u32,
        pid: i32,
        proc_number: i32,
    ) -> Result<bool, RuntimeStateTransitionError> {
        if !self.token_matches(generation) {
            return Ok(false);
        }
        let current = self
            .process()
            .map_err(|_| RuntimeStateTransitionError::worker("corrupt", "running"))?;
        if current != ProcessState::Starting || self.stop_requested != 0 {
            return Ok(false);
        }
        self.process_state = ProcessState::Running as u8;
        self.pid = pid;
        self.proc_number = proc_number;
        self.startup_deadline_ms = 0;
        Ok(true)
    }

    pub(crate) fn publish_directive(
        &mut self,
        generation: u32,
        directive: WorkerDirective,
    ) -> Result<bool, RuntimeStateTransitionError> {
        if !self.token_matches(generation) {
            return Ok(false);
        }
        let current = self
            .process()
            .map_err(|_| RuntimeStateTransitionError::worker("corrupt", "exiting"))?;
        if !matches!(current, ProcessState::Running | ProcessState::Exiting) {
            return Err(RuntimeStateTransitionError::worker(
                current.as_str(),
                ProcessState::Exiting.as_str(),
            ));
        }
        let (pending, delay_ms) = match directive {
            WorkerDirective::Idle => (PendingDirective::Idle, 0),
            WorkerDirective::RunImmediately => (PendingDirective::RunImmediately, 0),
            WorkerDirective::RunAfter(delay) => (
                PendingDirective::RunAfter,
                i64::try_from(delay.as_millis()).unwrap_or(i64::MAX),
            ),
        };
        self.pending_directive = pending as u8;
        self.pending_delay_ms = delay_ms;
        self.pending_directive_valid = 1;
        self.process_state = ProcessState::Exiting as u8;
        Ok(true)
    }

    pub(crate) fn record_exit_callback(
        &mut self,
        generation: u32,
        code: i32,
    ) -> bool {
        if !self.token_matches(generation)
            || !self.process().is_ok_and(ProcessState::is_active)
        {
            return false;
        }
        self.exit_callback_seen = 1;
        self.exit_code = code;
        true
    }

    pub(crate) fn request_stop(&mut self) -> Result<(), RuntimeStateTransitionError> {
        self.stop_requested = 1;
        self.dispatch_state = DispatchState::Idle as u8;
        self.not_before_ms = 0;
        let current = self
            .process()
            .map_err(|_| RuntimeStateTransitionError::worker("corrupt", "exiting"))?;
        if matches!(current, ProcessState::Starting | ProcessState::Running) {
            self.process_state = ProcessState::Exiting as u8;
        }
        Ok(())
    }

    pub(crate) fn mark_start_timed_out(&mut self, now_ms: i64) -> bool {
        if self.process() != Ok(ProcessState::Starting)
            || self.startup_deadline_ms == 0
            || self.startup_deadline_ms > now_ms
        {
            return false;
        }
        self.process_state = ProcessState::Exiting as u8;
        self.startup_deadline_ms = 0;
        true
    }

    pub(crate) fn confirm_stopped(
        &mut self,
        generation: u32,
        now_ms: i64,
        crash_backoff: Duration,
    ) -> Result<Option<WorkerCompletion>, RuntimeStateDecodeError> {
        if !self.token_matches(generation) {
            return Ok(None);
        }
        let process = self.process()?;
        if !process.is_active() {
            return Ok(None);
        }
        let explicit_stop = self.stop_requested != 0;
        let stopped_normally = self.pending_directive_valid != 0
            && self.exit_callback_seen != 0
            && self.exit_code == 0;

        self.process_state = ProcessState::Stopped as u8;
        self.pid = 0;
        self.proc_number = pg_sys::INVALID_PROC_NUMBER;
        self.startup_deadline_ms = 0;

        if explicit_stop || self.registration()? != RegistrationState::Registered {
            self.dispatch_state = DispatchState::Idle as u8;
            self.not_before_ms = 0;
        } else if self.wake_requested != 0 {
            self.dispatch_state = DispatchState::Ready as u8;
            self.not_before_ms = 0;
            self.wake_requested = 0;
        } else if stopped_normally {
            match PendingDirective::decode(self.pending_directive)? {
                PendingDirective::RunImmediately => {
                    self.dispatch_state = DispatchState::Ready as u8;
                    self.not_before_ms = 0;
                }
                PendingDirective::RunAfter => {
                    self.dispatch_state = DispatchState::Delayed as u8;
                    self.not_before_ms = now_ms.saturating_add(self.pending_delay_ms);
                }
                PendingDirective::Idle | PendingDirective::None => {
                    self.dispatch_state = DispatchState::Idle as u8;
                    self.not_before_ms = 0;
                }
            }
        } else {
            self.dispatch_state = DispatchState::Delayed as u8;
            self.not_before_ms = now_ms.saturating_add(
                i64::try_from(crash_backoff.as_millis()).unwrap_or(i64::MAX),
            );
        }
        self.reset_completion_evidence();
        Ok(Some(WorkerCompletion {
            stopped_normally,
            explicit_stop,
        }))
    }

    pub(crate) fn registration_failed(
        &mut self,
        generation: u32,
        retry_at_ms: i64,
    ) -> bool {
        if !self.token_matches(generation)
            || !self.process().is_ok_and(ProcessState::is_active)
        {
            return false;
        }
        self.process_state = ProcessState::Stopped as u8;
        self.pid = 0;
        self.proc_number = pg_sys::INVALID_PROC_NUMBER;
        if self.stop_requested != 0 {
            self.dispatch_state = DispatchState::Idle as u8;
            self.not_before_ms = 0;
        } else {
            self.dispatch_state = DispatchState::Delayed as u8;
            self.not_before_ms = retry_at_ms;
        }
        self.startup_deadline_ms = 0;
        self.reset_completion_evidence();
        true
    }

    pub(crate) fn reset_after_lost_owner(&mut self) {
        if self.process().is_ok_and(ProcessState::is_active) {
            self.generation = self.generation.wrapping_add(1).max(1);
        }
        self.process_state = ProcessState::Stopped as u8;
        self.pid = 0;
        self.proc_number = pg_sys::INVALID_PROC_NUMBER;
        self.startup_deadline_ms = 0;
        self.reset_completion_evidence();
        self.dispatch_state = DispatchState::Idle as u8;
        self.not_before_ms = 0;
        if self.registration() == Ok(RegistrationState::Registered)
            && self.stop_requested == 0
        {
            // Recovery may only turn this intent into Ready after committed
            // catalog reconciliation has confirmed that the registration exists.
            self.wake_requested = 1;
        }
    }

    fn reset_invocation(&mut self) {
        self.pid = 0;
        self.proc_number = pg_sys::INVALID_PROC_NUMBER;
        self.not_before_ms = 0;
        self.startup_deadline_ms = 0;
        self.dispatch_state = DispatchState::Idle as u8;
        self.process_state = ProcessState::Stopped as u8;
        self.wake_requested = 0;
        self.stop_requested = 0;
        self.reset_completion_evidence();
    }

    fn reset_completion_evidence(&mut self) {
        self.pending_directive = PendingDirective::None as u8;
        self.pending_directive_valid = 0;
        self.pending_delay_ms = 0;
        self.exit_callback_seen = 0;
        self.exit_code = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registered_worker() -> WorkerSlot {
        let mut worker = WorkerSlot::EMPTY;
        assert!(worker.initialize_registration(
            42,
            8,
            "worker",
            1,
            RegistrationState::Registered,
        ));
        worker.publish_wakeup();
        worker
    }

    fn running_worker() -> WorkerSlot {
        let mut worker = registered_worker();
        let generation = worker.prepare_start(1_000).unwrap();
        assert!(worker.publish_running(generation, 123, 9).unwrap());
        worker
    }

    #[test]
    fn stopped_ready_start_consumes_dispatch_and_retains_capacity() {
        let mut worker = registered_worker();
        assert_eq!(worker.process(), Ok(ProcessState::Stopped));
        assert_eq!(worker.dispatch(), Ok(DispatchState::Ready));

        let generation = worker.prepare_start(1_000).unwrap();
        assert_eq!(generation, 2);
        assert_eq!(worker.process(), Ok(ProcessState::Starting));
        assert_eq!(worker.dispatch(), Ok(DispatchState::Idle));
        assert!(worker.process().unwrap().is_active());
    }

    #[test]
    fn registration_failure_clears_physical_identity() {
        let mut worker = registered_worker();
        let generation = worker.prepare_start(1_000).unwrap();
        worker.pid = 123;
        worker.proc_number = 9;

        assert!(worker.registration_failed(generation, 2_000));

        assert_eq!(worker.process(), Ok(ProcessState::Stopped));
        assert_eq!(worker.pid, 0);
        assert_eq!(worker.proc_number, pg_sys::INVALID_PROC_NUMBER);
    }

    #[test]
    fn normal_directive_is_applied_only_after_stop_confirmation() {
        let mut worker = running_worker();
        let generation = worker.generation;
        worker
            .publish_directive(generation, WorkerDirective::Idle)
            .unwrap();
        worker.record_exit_callback(generation, 0);

        assert_eq!(worker.process(), Ok(ProcessState::Exiting));
        assert_eq!(worker.pid, 123);
        assert!(worker.prepare_start(2_000).is_err());

        let completion = worker
            .confirm_stopped(generation, 500, Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert!(completion.stopped_normally);
        assert_eq!(worker.process(), Ok(ProcessState::Stopped));
        assert_eq!(worker.dispatch(), Ok(DispatchState::Idle));
        assert_eq!(worker.pid, 0);
    }

    #[test]
    fn run_after_is_delayed_only_after_stop_confirmation() {
        let mut worker = running_worker();
        let generation = worker.generation;
        worker
            .publish_directive(
                generation,
                WorkerDirective::RunAfter(Duration::from_secs(2)),
            )
            .unwrap();
        worker.record_exit_callback(generation, 0);
        assert_eq!(worker.dispatch(), Ok(DispatchState::Idle));

        worker
            .confirm_stopped(generation, 500, Duration::from_secs(5))
            .unwrap();
        assert_eq!(worker.dispatch(), Ok(DispatchState::Delayed));
        assert_eq!(worker.not_before_ms, 2_500);
    }

    #[test]
    fn wakeup_during_running_or_exiting_dominates_idle() {
        for wake_before_directive in [true, false] {
            let mut worker = running_worker();
            let generation = worker.generation;
            if wake_before_directive {
                worker.publish_wakeup();
            }
            worker
                .publish_directive(generation, WorkerDirective::Idle)
                .unwrap();
            if !wake_before_directive {
                worker.publish_wakeup();
            }
            worker.record_exit_callback(generation, 0);
            worker
                .confirm_stopped(generation, 500, Duration::from_secs(5))
                .unwrap();
            assert_eq!(worker.dispatch(), Ok(DispatchState::Ready));
        }
    }

    #[test]
    fn explicit_stop_dominates_directive_and_crash_retry() {
        let mut worker = running_worker();
        let generation = worker.generation;
        worker.request_stop().unwrap();
        worker
            .publish_directive(generation, WorkerDirective::RunImmediately)
            .unwrap();
        worker.record_exit_callback(generation, 1);
        let completion = worker
            .confirm_stopped(generation, 500, Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert!(completion.explicit_stop);
        assert_eq!(worker.dispatch(), Ok(DispatchState::Idle));
    }

    #[test]
    fn missing_or_nonzero_exit_callback_is_a_crash() {
        for exit_code in [None, Some(1)] {
            let mut worker = running_worker();
            let generation = worker.generation;
            worker
                .publish_directive(generation, WorkerDirective::Idle)
                .unwrap();
            if let Some(code) = exit_code {
                worker.record_exit_callback(generation, code);
            }
            let completion = worker
                .confirm_stopped(generation, 500, Duration::from_secs(5))
                .unwrap()
                .unwrap();
            assert!(!completion.stopped_normally);
            assert_eq!(worker.dispatch(), Ok(DispatchState::Delayed));
            assert_eq!(worker.not_before_ms, 5_500);
        }
    }

    #[test]
    fn exit_without_a_directive_is_a_crash_even_with_zero_exit_code() {
        let mut worker = running_worker();
        let generation = worker.generation;
        worker.record_exit_callback(generation, 0);

        let completion = worker
            .confirm_stopped(generation, 500, Duration::from_secs(5))
            .unwrap()
            .unwrap();

        assert!(!completion.stopped_normally);
        assert_eq!(worker.dispatch(), Ok(DispatchState::Delayed));
        assert_eq!(worker.not_before_ms, 5_500);
    }

    #[test]
    fn launcher_recovery_preserves_stop_until_catalog_reconciliation() {
        let mut worker = running_worker();
        worker.request_stop().unwrap();

        worker.reset_after_lost_owner();

        assert_eq!(worker.process(), Ok(ProcessState::Stopped));
        assert_eq!(worker.dispatch(), Ok(DispatchState::Idle));
        assert_eq!(worker.stop_requested, 1);
        assert_eq!(worker.wake_requested, 0);

        worker.reconcile_present();

        assert_eq!(worker.stop_requested, 0);
        assert_eq!(worker.dispatch(), Ok(DispatchState::Ready));
    }

    #[test]
    fn catalog_reconciliation_preserves_stop_for_starting_worker() {
        let mut worker = registered_worker();
        worker.prepare_start(1_000).unwrap();
        worker.request_stop().unwrap();

        worker.reconcile_present();

        assert_eq!(worker.process(), Ok(ProcessState::Exiting));
        assert_eq!(worker.stop_requested, 1);
        assert_eq!(worker.dispatch(), Ok(DispatchState::Idle));
    }

    #[test]
    fn launcher_recovery_waits_for_catalog_before_restoring_dispatch() {
        let mut worker = running_worker();

        worker.reset_after_lost_owner();

        assert_eq!(worker.process(), Ok(ProcessState::Stopped));
        assert_eq!(worker.dispatch(), Ok(DispatchState::Idle));
        assert_eq!(worker.wake_requested, 1);

        worker.reconcile_present();

        assert_eq!(worker.dispatch(), Ok(DispatchState::Ready));
    }

    #[test]
    fn late_old_generation_child_is_fenced_after_launcher_recovery() {
        let mut worker = running_worker();
        let old_generation = worker.generation;
        worker.reset_after_lost_owner();

        assert!(worker.generation > old_generation);
        assert!(!worker.publish_running(old_generation, 456, 10).unwrap());
        assert!(
            !worker
                .publish_directive(old_generation, WorkerDirective::RunImmediately,)
                .unwrap()
        );
        assert_eq!(worker.process(), Ok(ProcessState::Stopped));
        assert_eq!(worker.dispatch(), Ok(DispatchState::Idle));
    }

    #[test]
    fn stale_generation_cannot_update_reused_slot() {
        let mut worker = running_worker();
        assert!(!worker.record_exit_callback(worker.generation - 1, 0));
        assert!(
            !worker
                .publish_directive(
                    worker.generation - 1,
                    WorkerDirective::RunImmediately,
                )
                .unwrap()
        );
    }

    #[test]
    fn slot_replacement_requires_physical_stop() {
        for state in [
            ProcessState::Starting,
            ProcessState::Running,
            ProcessState::Exiting,
        ] {
            let mut worker = registered_worker();
            worker.process_state = state as u8;
            assert!(worker.begin_registration_replacement().is_err());
        }
        let mut worker = registered_worker();
        assert!(worker.begin_registration_replacement().is_ok());
    }
}
