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
    _padding: [u8; 3],
    last_start_ms: i64,
    last_stop_ms: i64,
    last_reconcile_at_ms: i64,
    last_reconcile_added: u32,
    last_reconcile_removed: u32,
    last_reconcile_replaced: u32,
    last_reconcile_unchanged: u32,
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
            _padding: [0; 3],
            last_start_ms: 0,
            last_stop_ms: 0,
            last_reconcile_at_ms: 0,
            last_reconcile_added: 0,
            last_reconcile_removed: 0,
            last_reconcile_replaced: 0,
            last_reconcile_unchanged: 0,
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
        self.last_error = [0; MAX_ERROR_BYTES];
        let bytes = message.as_bytes();
        let len = bytes.len().min(MAX_ERROR_BYTES);
        self.last_error[..len].copy_from_slice(&bytes[..len]);
        self.last_error_len =
            u16::try_from(len).expect("MAX_ERROR_BYTES fits in u16");
        self.last_error_at_ms = timestamp_ms();
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

    pub(super) fn mark_reconcile(&self, report: &ReconcileReport) {
        let mut state = STORAGE_STATE.exclusive();
        state.reset_if_invalid();
        state.last_reconcile_at_ms = timestamp_ms();
        state.last_reconcile_added = count_to_u32(report.added);
        state.last_reconcile_removed = count_to_u32(report.removed);
        state.last_reconcile_replaced = count_to_u32(report.replaced);
        state.last_reconcile_unchanged = count_to_u32(report.unchanged);
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

    pub(super) fn mark_stopped(&self) {
        let mut state = STORAGE_STATE.exclusive();
        state.reset_if_invalid();
        state.pid = 0;
        state.last_stop_ms = timestamp_ms();
        state.set_state(StorageProcessState::Stopped);
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
    pub(crate) last_reconcile_at_ms: Option<i64>,
    pub(crate) last_reconcile_added: i64,
    pub(crate) last_reconcile_removed: i64,
    pub(crate) last_reconcile_replaced: i64,
    pub(crate) last_reconcile_unchanged: i64,
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
        last_reconcile_at_ms: positive_timestamp(state.last_reconcile_at_ms),
        last_reconcile_added: i64::from(state.last_reconcile_added),
        last_reconcile_removed: i64::from(state.last_reconcile_removed),
        last_reconcile_replaced: i64::from(state.last_reconcile_replaced),
        last_reconcile_unchanged: i64::from(state.last_reconcile_unchanged),
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

fn positive_timestamp(timestamp: i64) -> Option<i64> {
    (timestamp > 0).then_some(timestamp)
}

fn count_to_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn timestamp_ms() -> i64 {
    unsafe { pgrx::pg_sys::GetCurrentTimestamp() / 1_000 }
}
