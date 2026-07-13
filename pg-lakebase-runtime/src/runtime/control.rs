use std::time::{Duration, Instant};

use crate::error::{LakebaseError, LakebaseResult};
use crate::gucs;

use super::bgworker::{check_for_interrupts, interruptible_sleep};
use super::store::RuntimeStore;

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
        let deadline = Instant::now() + gucs::stop_timeout();
        loop {
            let pids = self.store.stop_database_step(database_oid)?;
            for pid in pids {
                unsafe { libc::kill(pid, libc::SIGTERM) };
            }
            if !self.store.database_has_running_processes(database_oid) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(LakebaseError::StopDatabaseTimeout);
            }
            check_for_interrupts();
            interruptible_sleep(Duration::from_millis(10));
        }
    }

    pub(super) fn stop_extension(
        &self,
        database_oid: u32,
        extension_oid: u32,
    ) -> LakebaseResult<()> {
        let deadline = Instant::now() + gucs::stop_timeout();
        loop {
            let pids = self
                .store
                .stop_extension_step(database_oid, extension_oid)?;
            for pid in pids {
                unsafe { libc::kill(pid, libc::SIGTERM) };
            }
            if !self
                .store
                .extension_has_running_workers(database_oid, extension_oid)
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(LakebaseError::StopExtensionTimeout);
            }
            check_for_interrupts();
            interruptible_sleep(Duration::from_millis(10));
        }
    }

    pub(super) fn pause_reconciliation(
        &self,
        database_oid: u32,
    ) -> LakebaseResult<()> {
        let deadline = Instant::now() + gucs::stop_timeout();
        loop {
            let Some(pid) = self.store.pause_reconciliation_step(database_oid)?
            else {
                return Ok(());
            };
            // SAFETY: PID is published by the generation-fenced reconciler slot.
            unsafe { libc::kill(pid, libc::SIGTERM) };
            if Instant::now() >= deadline {
                return Err(LakebaseError::StopReconcilerTimeout);
            }
            check_for_interrupts();
            interruptible_sleep(Duration::from_millis(10));
        }
    }

    pub(super) fn stop_worker(
        &self,
        database_oid: u32,
        extension_oid: u32,
        worker_name: &str,
    ) -> LakebaseResult<()> {
        let deadline = Instant::now() + gucs::stop_timeout();
        loop {
            let Some(pid) = self.store.stop_worker_step(
                database_oid,
                extension_oid,
                worker_name,
            )?
            else {
                return Ok(());
            };
            // SAFETY: the PID was read from a generation-fenced live worker slot.
            // ESRCH is harmless because the exit callback will clear the slot.
            unsafe { libc::kill(pid, libc::SIGTERM) };
            if Instant::now() >= deadline {
                return Err(LakebaseError::StopWorkerTimeout);
            }
            check_for_interrupts();
            interruptible_sleep(Duration::from_millis(10));
        }
    }
}
