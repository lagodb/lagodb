use std::path::PathBuf;

use pgrx::{PGRXSharedMemory, PgLwLock};

use super::reconciler::ReconcileReport;

const STORAGE_MAGIC: u64 = 0x5047_4c42_5354_4f52;
const MAX_ERROR_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum StorageProcessState {
    Stopped = 0,
    Starting = 1,
    Running = 2,
    Stopping = 3,
    Failed = 4,
}

impl StorageProcessState {
    fn from_raw(raw: u8) -> Self {
        match raw {
            1 => Self::Starting,
            2 => Self::Running,
            3 => Self::Stopping,
            4 => Self::Failed,
            _ => Self::Stopped,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Stopped => "stopped",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct StorageSharedState {
    magic: u64,
    struct_size: u32,
    pid: i32,
    state: u8,
    reload_requested: u8,
    force_default_chain_reload: u8,
    _padding: [u8; 1],
    last_start_ms: i64,
    last_stop_ms: i64,
    last_reload_at_ms: i64,
    last_reload_added: u32,
    last_reload_removed: u32,
    last_reload_replaced: u32,
    last_reload_unchanged: u32,
    desired_volume_count: u32,
    loaded_volume_count: u32,
    stale_volume_count: u32,
    unavailable_volume_count: u32,
    last_error_at_ms: i64,
    last_error_len: u16,
    _error_padding: [u8; 6],
    last_error: [u8; MAX_ERROR_BYTES],
}

impl Default for StorageSharedState {
    fn default() -> Self {
        Self {
            magic: STORAGE_MAGIC,
            struct_size: u32::try_from(std::mem::size_of::<Self>())
                .expect("storage shared state exceeds u32"),
            pid: 0,
            state: StorageProcessState::Stopped as u8,
            reload_requested: 0,
            force_default_chain_reload: 0,
            _padding: [0; 1],
            last_start_ms: 0,
            last_stop_ms: 0,
            last_reload_at_ms: 0,
            last_reload_added: 0,
            last_reload_removed: 0,
            last_reload_replaced: 0,
            last_reload_unchanged: 0,
            desired_volume_count: 0,
            loaded_volume_count: 0,
            stale_volume_count: 0,
            unavailable_volume_count: 0,
            last_error_at_ms: 0,
            last_error_len: 0,
            _error_padding: [0; 6],
            last_error: [0; MAX_ERROR_BYTES],
        }
    }
}

impl StorageSharedState {
    fn validate_layout(&self) -> bool {
        self.magic == STORAGE_MAGIC
            && usize::try_from(self.struct_size).ok()
                == Some(std::mem::size_of::<Self>())
    }

    fn reset_if_invalid(&mut self) {
        if !self.validate_layout() {
            *self = Self::default();
        }
    }

    fn set_state(&mut self, state: StorageProcessState) {
        self.state = state as u8;
    }

    fn set_error(&mut self, message: &str) {
        self.set_error_at(message, timestamp_ms());
    }

    fn clear_error(&mut self) {
        self.last_error_len = 0;
        self.last_error_at_ms = 0;
    }

    fn set_error_at(&mut self, message: &str, error_at_ms: i64) {
        self.last_error = [0; MAX_ERROR_BYTES];
        let message = truncate_to_char_boundary(message, MAX_ERROR_BYTES);
        let bytes = message.as_bytes();
        let len = bytes.len();
        self.last_error[..len].copy_from_slice(&bytes[..len]);
        self.last_error_len =
            u16::try_from(len).expect("MAX_ERROR_BYTES fits in u16");
        self.last_error_at_ms = error_at_ms;
    }

    fn finish_process(&mut self, pid: i32, code: i32, exited_at_ms: i64) {
        if pid <= 0 || self.pid != pid {
            return;
        }

        let previous = StorageProcessState::from_raw(self.state);
        self.pid = 0;
        match previous {
            StorageProcessState::Failed => {}
            StorageProcessState::Stopping if code == 0 => {
                self.last_stop_ms = exited_at_ms;
                self.set_state(StorageProcessState::Stopped);
            }
            _ => {
                self.set_state(StorageProcessState::Failed);
                self.set_error_at(
                    &format!(
                        "storage worker exited unexpectedly from {} state with code {code}",
                        previous.as_str()
                    ),
                    exited_at_ms,
                );
            }
        }
    }
}

// SAFETY: StorageSharedState is repr(C), Copy, contains only fixed-size scalar
// fields and arrays, and contains no references or process-local pointers.
unsafe impl PGRXSharedMemory for StorageSharedState {}

pub(crate) static STORAGE_STATE: PgLwLock<StorageSharedState> =
    unsafe { PgLwLock::new(c"pg_lakebase_runtime storage runtime") };

pub(super) struct StorageStatusStore;

impl StorageStatusStore {
    pub(super) const fn new() -> Self {
        Self
    }

    pub(super) fn mark_starting(&self, pid: i32) {
        let mut state = STORAGE_STATE.exclusive();
        state.reset_if_invalid();
        state.pid = pid;
        state.reload_requested = 0;
        state.force_default_chain_reload = 0;
        state.last_start_ms = timestamp_ms();
        state.set_state(StorageProcessState::Starting);
        state.last_error_len = 0;
        state.last_error_at_ms = 0;
    }

    pub(super) fn mark_running(&self) {
        let mut state = STORAGE_STATE.exclusive();
        state.reset_if_invalid();
        state.set_state(StorageProcessState::Running);
    }

    pub(super) fn mark_reload(&self, report: &ReconcileReport) {
        let mut state = STORAGE_STATE.exclusive();
        state.reset_if_invalid();
        state.last_reload_at_ms = timestamp_ms();
        state.last_reload_added = count_to_u32(report.added);
        state.last_reload_removed = count_to_u32(report.removed);
        state.last_reload_replaced = count_to_u32(report.replaced);
        state.last_reload_unchanged = count_to_u32(report.unchanged);
        state.desired_volume_count = count_to_u32(report.desired);
        state.loaded_volume_count = count_to_u32(report.loaded);
        state.stale_volume_count = count_to_u32(report.stale);
        state.unavailable_volume_count = count_to_u32(report.unavailable);
        if let Some(failure) = report.failures.first() {
            state.set_error(&format!(
                "storage volume store {} is {}: {}",
                failure.store_id,
                failure.state.as_str(),
                failure.message,
            ));
        } else if report.stale == 0 && report.unavailable == 0 {
            state.clear_error();
        }
    }

    pub(super) fn record_error(&self, message: &str) {
        let mut state = STORAGE_STATE.exclusive();
        state.reset_if_invalid();
        state.set_error(message);
    }

    pub(super) fn mark_failed(&self, message: &str) {
        let mut state = STORAGE_STATE.exclusive();
        state.reset_if_invalid();
        state.set_state(StorageProcessState::Failed);
        state.set_error(message);
    }

    pub(super) fn mark_stopping(&self) {
        let mut state = STORAGE_STATE.exclusive();
        state.reset_if_invalid();
        state.set_state(StorageProcessState::Stopping);
    }

    pub(super) fn finish_process(&self, pid: i32, code: i32) {
        let mut state = STORAGE_STATE.exclusive();
        state.reset_if_invalid();
        state.finish_process(pid, code, timestamp_ms());
    }

    pub(super) fn take_reload_request(&self) -> Option<bool> {
        let mut state = STORAGE_STATE.exclusive();
        state.reset_if_invalid();
        let requested = state.reload_requested != 0;
        let force_default_chain = state.force_default_chain_reload != 0;
        state.reload_requested = 0;
        state.force_default_chain_reload = 0;
        requested.then_some(force_default_chain)
    }
}

pub(crate) fn request_reload(force_default_chain: bool) {
    let pid = {
        let mut state = STORAGE_STATE.exclusive();
        state.reset_if_invalid();
        state.reload_requested = 1;
        if force_default_chain {
            state.force_default_chain_reload = 1;
        }
        state.pid
    };
    if pid <= 0 {
        return;
    }
    // SAFETY: BackendPidGetProc returns either null or a shared PGPROC whose
    // procLatch has postmaster lifetime. A PID race can at worst produce an
    // extra wakeup; the config file remains the source of truth.
    unsafe {
        let process = pgrx::pg_sys::BackendPidGetProc(pid);
        if !process.is_null() {
            pgrx::pg_sys::SetLatch(&raw mut (*process).procLatch);
        }
    }
}

pub(crate) struct StorageRuntimeStatus {
    pub(crate) enabled: bool,
    pub(crate) pid: Option<i32>,
    pub(crate) state: &'static str,
    pub(crate) socket_path: String,
    pub(crate) cache_dir: String,
    pub(crate) last_start_ms: Option<i64>,
    pub(crate) last_stop_ms: Option<i64>,
    pub(crate) last_reload_at_ms: Option<i64>,
    pub(crate) last_reload_added: i64,
    pub(crate) last_reload_removed: i64,
    pub(crate) last_reload_replaced: i64,
    pub(crate) last_reload_unchanged: i64,
    pub(crate) desired_volume_count: i64,
    pub(crate) loaded_volume_count: i64,
    pub(crate) stale_volume_count: i64,
    pub(crate) unavailable_volume_count: i64,
    pub(crate) last_error_at_ms: Option<i64>,
    pub(crate) last_error: Option<String>,
}

pub(crate) fn snapshot(
    enabled: bool,
    socket_path: PathBuf,
    cache_dir: PathBuf,
) -> StorageRuntimeStatus {
    let state = *STORAGE_STATE.share();
    let state = if state.validate_layout() {
        state
    } else {
        StorageSharedState::default()
    };
    let process_state = if enabled {
        StorageProcessState::from_raw(state.state).as_str()
    } else {
        "disabled"
    };

    StorageRuntimeStatus {
        enabled,
        pid: (enabled && state.pid > 0).then_some(state.pid),
        state: process_state,
        socket_path: socket_path.display().to_string(),
        cache_dir: cache_dir.display().to_string(),
        last_start_ms: positive_timestamp(state.last_start_ms),
        last_stop_ms: positive_timestamp(state.last_stop_ms),
        last_reload_at_ms: positive_timestamp(state.last_reload_at_ms),
        last_reload_added: i64::from(state.last_reload_added),
        last_reload_removed: i64::from(state.last_reload_removed),
        last_reload_replaced: i64::from(state.last_reload_replaced),
        last_reload_unchanged: i64::from(state.last_reload_unchanged),
        desired_volume_count: i64::from(state.desired_volume_count),
        loaded_volume_count: i64::from(state.loaded_volume_count),
        stale_volume_count: i64::from(state.stale_volume_count),
        unavailable_volume_count: i64::from(state.unavailable_volume_count),
        last_error_at_ms: positive_timestamp(state.last_error_at_ms),
        last_error: last_error(&state),
    }
}

fn last_error(state: &StorageSharedState) -> Option<String> {
    let len = usize::from(state.last_error_len).min(MAX_ERROR_BYTES);
    if len == 0 {
        return None;
    }
    Some(String::from_utf8_lossy(&state.last_error[..len]).into_owned())
}

fn truncate_to_char_boundary(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn positive_timestamp(timestamp: i64) -> Option<i64> {
    (timestamp > 0).then_some(timestamp)
}

fn count_to_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn timestamp_ms() -> i64 {
    unsafe { pgrx::pg_sys::GetCurrentTimestamp() / 1_000 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_truncation_preserves_utf8_boundaries() {
        let value = format!("{}é", "a".repeat(MAX_ERROR_BYTES - 1));
        let truncated = truncate_to_char_boundary(&value, MAX_ERROR_BYTES);

        assert_eq!(truncated.len(), MAX_ERROR_BYTES - 1);
        assert!(truncated.is_char_boundary(truncated.len()));
    }

    #[test]
    fn process_exit_preserves_recorded_failure_and_clears_pid() {
        let mut state = StorageSharedState {
            pid: 42,
            state: StorageProcessState::Failed as u8,
            ..StorageSharedState::default()
        };
        state.set_error_at("specific startup failure", 10);

        state.finish_process(42, 1, 20);

        assert_eq!(state.pid, 0);
        assert_eq!(
            StorageProcessState::from_raw(state.state),
            StorageProcessState::Failed
        );
        assert_eq!(
            last_error(&state).as_deref(),
            Some("specific startup failure")
        );
        assert_eq!(state.last_error_at_ms, 10);
        assert_eq!(state.last_stop_ms, 0);
    }

    #[test]
    fn process_exit_records_unexpected_starting_failure() {
        let mut state = StorageSharedState {
            pid: 42,
            state: StorageProcessState::Starting as u8,
            ..StorageSharedState::default()
        };

        state.finish_process(42, 1, 20);

        assert_eq!(state.pid, 0);
        assert_eq!(
            StorageProcessState::from_raw(state.state),
            StorageProcessState::Failed
        );
        assert_eq!(
            last_error(&state).as_deref(),
            Some(
                "storage worker exited unexpectedly from starting state with code 1"
            )
        );
        assert_eq!(state.last_error_at_ms, 20);
        assert_eq!(state.last_stop_ms, 0);
    }

    #[test]
    fn process_exit_marks_cooperative_shutdown_stopped() {
        let mut state = StorageSharedState {
            pid: 42,
            state: StorageProcessState::Stopping as u8,
            ..StorageSharedState::default()
        };

        state.finish_process(42, 0, 20);

        assert_eq!(state.pid, 0);
        assert_eq!(
            StorageProcessState::from_raw(state.state),
            StorageProcessState::Stopped
        );
        assert_eq!(state.last_stop_ms, 20);
        assert_eq!(last_error(&state), None);
    }

    #[test]
    fn process_exit_rejects_stale_pid() {
        let mut state = StorageSharedState {
            pid: 84,
            state: StorageProcessState::Running as u8,
            ..StorageSharedState::default()
        };

        state.finish_process(42, 1, 20);

        assert_eq!(state.pid, 84);
        assert_eq!(
            StorageProcessState::from_raw(state.state),
            StorageProcessState::Running
        );
        assert_eq!(state.last_error_at_ms, 0);
        assert_eq!(state.last_stop_ms, 0);
    }
}
