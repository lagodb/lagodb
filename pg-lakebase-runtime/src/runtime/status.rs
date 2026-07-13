use super::store::RuntimeStore;

pub(crate) struct WorkerStatus {
    pub(crate) database_oid: u32,
    pub(crate) extension_oid: u32,
    pub(crate) worker_name: String,
    pub(crate) state: &'static str,
    pub(crate) pid: Option<i32>,
    pub(crate) restart_at_ms: Option<i64>,
}

pub(crate) struct ProcessStatus {
    pub(crate) process_kind: &'static str,
    pub(crate) database_oid: Option<u32>,
    pub(crate) state: &'static str,
    pub(crate) pid: Option<i32>,
}

pub(super) fn worker_status() -> Vec<WorkerStatus> {
    RuntimeStore::new().worker_status()
}

pub(super) fn process_status() -> Vec<ProcessStatus> {
    RuntimeStore::new().process_status()
}
