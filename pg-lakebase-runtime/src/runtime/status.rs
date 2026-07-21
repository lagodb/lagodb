use super::store::RuntimeStore;

pub(crate) struct WorkerStatus {
    pub(crate) database_oid: u32,
    pub(crate) extension_oid: u32,
    pub(crate) worker_name: String,
    pub(crate) registration_state: &'static str,
    pub(crate) dispatch_state: &'static str,
    pub(crate) process_state: &'static str,
    pub(crate) pid: Option<i32>,
    pub(crate) generation: u32,
    pub(crate) not_before_ms: Option<i64>,
    pub(crate) stop_requested: bool,
    pub(crate) launcher_epoch: u64,
    pub(crate) recovery_state: &'static str,
}

pub(crate) struct ProcessStatus {
    pub(crate) process_kind: &'static str,
    pub(crate) database_oid: Option<u32>,
    pub(crate) state: &'static str,
    pub(crate) pid: Option<i32>,
    pub(crate) recovery_backend_count: Option<u32>,
}

pub(super) fn worker_status() -> Vec<WorkerStatus> {
    RuntimeStore::new().worker_status()
}

pub(super) fn process_status() -> Vec<ProcessStatus> {
    RuntimeStore::new().process_status()
}
