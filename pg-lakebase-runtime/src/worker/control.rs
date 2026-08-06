use super::store::Store;

pub(super) struct StopController {
    store: Store,
}

impl StopController {
    pub(super) const fn new() -> Self {
        Self {
            store: Store::new(),
        }
    }

    pub(super) fn prepare_database_drop(&self, database_oid: u32) {
        self.store.prepare_drop_database(database_oid);
        self.store.signal_supervisor();
    }

    pub(super) fn prepare_extension_drop(
        &self,
        database_oid: u32,
        extension_oid: u32,
    ) {
        self.store
            .prepare_drop_extension(database_oid, extension_oid);
        self.store.signal_supervisor();
    }

    pub(super) fn stop_worker(&self, database_oid: u32, worker_id: i32) {
        let needs_supervisor_wake =
            self.store.request_stop_worker(database_oid, worker_id);
        if needs_supervisor_wake {
            self.store.signal_supervisor();
        }
    }
}
