use super::store::Store;

pub(crate) struct WorkerStatus {
    pub(crate) database_oid: u32,
    pub(crate) worker_id: i32,
    pub(crate) extension_oid: u32,
    pub(crate) worker_name: String,
    pub(crate) registration_state: &'static str,
    pub(crate) process_state: &'static str,
    pub(crate) pid: Option<i32>,
    pub(crate) needs_restart: bool,
    pub(crate) restart_after_ms: Option<i64>,
    pub(crate) failure_count: i32,
    pub(crate) stop_requested: bool,
}

pub(crate) struct ProcessStatus {
    pub(crate) process_kind: &'static str,
    pub(crate) database_oid: Option<u32>,
    pub(crate) state: &'static str,
    pub(crate) pid: Option<i32>,
    pub(crate) needs_restart: Option<bool>,
}

pub(super) fn worker_status() -> Vec<WorkerStatus> {
    Store::new().worker_status()
}

pub(super) fn process_status() -> Vec<ProcessStatus> {
    Store::new().process_status()
}
