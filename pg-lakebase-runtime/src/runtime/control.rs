use std::time::{Duration, Instant};

use crate::error::{LakebaseError, LakebaseResult};
use crate::gucs;

use super::bgworker::{check_for_interrupts, interruptible_sleep};
use super::store::{RuntimeStore, StopTarget};

#[derive(Clone, Copy)]
enum StopTimeoutKind {
    Database,
    Extension,
    Worker,
}

impl StopTimeoutKind {
    fn error(self, details: String) -> LakebaseError {
        match self {
            Self::Database => LakebaseError::StopDatabaseTimeout { details },
            Self::Extension => LakebaseError::StopExtensionTimeout { details },
            Self::Worker => LakebaseError::StopWorkerTimeout { details },
        }
    }
}

pub(super) struct StopController {
    store: RuntimeStore,
}

impl StopController {
    pub(super) const fn new() -> Self {
        Self {
            store: RuntimeStore::new(),
        }
    }

    pub(super) fn stop_database(&self, database_oid: u32) -> LakebaseResult<()> {
        self.store.request_stop_database(database_oid)?;
        self.store.signal_launcher();
        self.wait_until(
            || self.store.database_is_stopped(database_oid),
            StopTarget::Database(database_oid),
            StopTimeoutKind::Database,
        )
    }

    pub(super) fn stop_extension(
        &self,
        database_oid: u32,
        extension_oid: u32,
    ) -> LakebaseResult<()> {
        self.store
            .request_stop_extension(database_oid, extension_oid)?;
        self.store.signal_launcher();
        self.wait_until(
            || self.store.extension_is_stopped(database_oid, extension_oid),
            StopTarget::Extension {
                database_oid,
                extension_oid,
            },
            StopTimeoutKind::Extension,
        )
    }

    pub(super) fn stop_worker(
        &self,
        database_oid: u32,
        extension_oid: u32,
        worker_name: &str,
    ) -> LakebaseResult<()> {
        let requested = self.store.request_stop_worker(
            database_oid,
            extension_oid,
            worker_name,
        )?;
        if requested {
            self.store.signal_launcher();
        }
        self.wait_until(
            || {
                self.store
                    .worker_is_stopped(database_oid, extension_oid, worker_name)
            },
            StopTarget::Worker {
                database_oid,
                extension_oid,
                worker_name,
            },
            StopTimeoutKind::Worker,
        )
    }

    fn wait_until(
        &self,
        stopped: impl Fn() -> bool,
        target: StopTarget<'_>,
        timeout_kind: StopTimeoutKind,
    ) -> LakebaseResult<()> {
        // DDL callers may hold the database lifecycle advisory lock. Each
        // predicate/diagnostic takes the runtime LWLock only for a bounded shared
        // state read, and no runtime lock is held while sleeping.
        let deadline = Instant::now() + gucs::stop_timeout();
        loop {
            if stopped() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(timeout_kind.error(self.store.stop_diagnostics(target)));
            }
            check_for_interrupts();
            interruptible_sleep(Duration::from_millis(10));
        }
    }
}
