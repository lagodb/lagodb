use pgrx::pg_sys;

use crate::state::{
    INVALID_OID, ProcessState, RuntimeStateDecodeError, RuntimeStateTransitionError,
};

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct DatabaseReconcileState {
    pub(crate) database_oid: u32,
    pub(crate) _padding: u32,
    pub(crate) desired_generation: u64,
    pub(crate) completed_generation: u64,
}

impl DatabaseReconcileState {
    pub(crate) const EMPTY: Self = Self {
        database_oid: INVALID_OID,
        _padding: 0,
        desired_generation: 0,
        completed_generation: 0,
    };

    pub(crate) fn request(&mut self, database_oid: u32) {
        if self.database_oid == INVALID_OID {
            self.database_oid = database_oid;
        }
        debug_assert_eq!(self.database_oid, database_oid);
        self.desired_generation = self.desired_generation.wrapping_add(1);
    }

    pub(crate) const fn is_pending(&self) -> bool {
        self.database_oid != INVALID_OID
            && self.desired_generation != self.completed_generation
    }

    pub(crate) fn complete(&mut self, target_generation: u64) {
        self.completed_generation = target_generation;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReconcilerCompletion {
    pub(crate) completed_target: Option<u64>,
    pub(crate) retry: bool,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct ReconcilerSlot {
    pub(crate) database_oid: u32,
    pub(crate) generation: u32,
    pub(crate) target_generation: u64,
    pub(crate) startup_deadline_ms: i64,
    pub(crate) pid: i32,
    pub(crate) proc_number: i32,
    pub(crate) process_state: u8,
    pub(crate) stop_requested: u8,
    pub(crate) completion_valid: u8,
    pub(crate) retry_requested: u8,
    pub(crate) exit_callback_seen: u8,
    pub(crate) _padding: [u8; 3],
    pub(crate) exit_code: i32,
}

impl ReconcilerSlot {
    pub(crate) const EMPTY: Self = Self {
        database_oid: INVALID_OID,
        generation: 0,
        target_generation: 0,
        startup_deadline_ms: 0,
        pid: 0,
        proc_number: pg_sys::INVALID_PROC_NUMBER,
        process_state: ProcessState::Stopped as u8,
        stop_requested: 0,
        completion_valid: 0,
        retry_requested: 0,
        exit_callback_seen: 0,
        _padding: [0; 3],
        exit_code: 0,
    };

    pub(crate) fn process(&self) -> Result<ProcessState, RuntimeStateDecodeError> {
        ProcessState::decode(self.process_state)
    }

    pub(crate) fn reserve(
        database_oid: u32,
        generation: u32,
        target_generation: u64,
        startup_deadline_ms: i64,
    ) -> Self {
        Self {
            database_oid,
            generation,
            target_generation,
            startup_deadline_ms,
            process_state: ProcessState::Starting as u8,
            ..Self::EMPTY
        }
    }

    pub(crate) fn token_matches(&self, generation: u32) -> bool {
        self.generation == generation
    }

    pub(crate) fn publish_running(
        &mut self,
        generation: u32,
        pid: i32,
        proc_number: i32,
    ) -> Result<Option<u32>, RuntimeStateTransitionError> {
        if !self.token_matches(generation) {
            return Ok(None);
        }
        let current = self.process().map_err(|_| {
            RuntimeStateTransitionError::reconciler("corrupt", "running")
        })?;
        if current != ProcessState::Starting || self.stop_requested != 0 {
            return Ok(None);
        }
        self.process_state = ProcessState::Running as u8;
        self.pid = pid;
        self.proc_number = proc_number;
        self.startup_deadline_ms = 0;
        Ok(Some(self.database_oid))
    }

    pub(crate) fn publish_completion(
        &mut self,
        generation: u32,
        retry: bool,
    ) -> Result<bool, RuntimeStateTransitionError> {
        if !self.token_matches(generation) {
            return Ok(false);
        }
        let current = self.process().map_err(|_| {
            RuntimeStateTransitionError::reconciler("corrupt", "exiting")
        })?;
        if !matches!(current, ProcessState::Running | ProcessState::Exiting) {
            return Err(RuntimeStateTransitionError::reconciler(
                current.as_str(),
                ProcessState::Exiting.as_str(),
            ));
        }
        self.process_state = ProcessState::Exiting as u8;
        self.completion_valid = 1;
        self.retry_requested = u8::from(retry);
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
        let current = self.process().map_err(|_| {
            RuntimeStateTransitionError::reconciler("corrupt", "exiting")
        })?;
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
    ) -> Option<ReconcilerCompletion> {
        if !self.token_matches(generation)
            || !self.process().is_ok_and(ProcessState::is_active)
        {
            return None;
        }
        let (completed_target, retry) = if self.stop_requested != 0 {
            (None, false)
        } else if self.completion_valid != 0
            && self.exit_callback_seen != 0
            && self.exit_code == 0
        {
            if self.retry_requested != 0 {
                (None, true)
            } else {
                (Some(self.target_generation), false)
            }
        } else {
            (None, true)
        };
        self.process_state = ProcessState::Stopped as u8;
        self.pid = 0;
        self.proc_number = pg_sys::INVALID_PROC_NUMBER;
        self.startup_deadline_ms = 0;
        Some(ReconcilerCompletion {
            completed_target,
            retry,
        })
    }

    pub(crate) fn reset_after_lost_owner(&mut self) {
        let generation = if self.process().is_ok_and(ProcessState::is_active) {
            self.generation.wrapping_add(1).max(1)
        } else {
            self.generation
        };
        *self = Self {
            generation,
            ..Self::EMPTY
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intent_arriving_during_an_active_run_remains_pending() {
        let mut intent = DatabaseReconcileState::EMPTY;
        intent.request(42);
        let active_target = intent.desired_generation;

        intent.request(42);
        intent.complete(active_target);

        assert!(intent.is_pending());
        assert_ne!(intent.desired_generation, intent.completed_generation);
    }

    #[test]
    fn reconciler_only_publishes_the_generation_it_reserved() {
        let mut reconciler = ReconcilerSlot::reserve(42, 1, 11, 1_000);
        assert_eq!(reconciler.publish_running(1, 123, 9).unwrap(), Some(42));
        assert!(reconciler.publish_completion(1, false).unwrap());
        assert!(reconciler.record_exit_callback(1, 0));

        let completion = reconciler.confirm_stopped(1).unwrap();
        assert_eq!(completion.completed_target, Some(11));
        assert!(!completion.retry);
    }

    #[test]
    fn failed_reconciler_does_not_acknowledge_its_target() {
        let mut reconciler = ReconcilerSlot::reserve(42, 1, 11, 1_000);
        assert_eq!(reconciler.publish_running(1, 123, 9).unwrap(), Some(42));
        assert!(reconciler.publish_completion(1, true).unwrap());
        assert!(reconciler.record_exit_callback(1, 0));

        let completion = reconciler.confirm_stopped(1).unwrap();
        assert_eq!(completion.completed_target, None);
        assert!(completion.retry);
    }

    #[test]
    fn recovery_reset_fences_an_active_reconciler_by_generation() {
        let mut reconciler = ReconcilerSlot::reserve(42, 7, 11, 1_000);
        let old_generation = reconciler.generation;

        reconciler.reset_after_lost_owner();

        assert!(reconciler.generation > old_generation);
        assert_eq!(reconciler.database_oid, INVALID_OID);
        assert_eq!(reconciler.process(), Ok(ProcessState::Stopped));
        assert_eq!(reconciler.publish_running(old_generation, 123, 9), Ok(None));
    }
}
