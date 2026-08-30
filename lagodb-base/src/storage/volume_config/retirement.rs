use std::cell::RefCell;
use std::io::ErrorKind;
use std::rc::Rc;

use crate::worker::ensure_preloaded;
use lagodb_core::diag::{PgReportError, SqlStateError, report_warning};
use lagodb_core::options::{TablespaceCacheError, get_tablespace};
use lagodb_core::storage::volume::StorageVolumeId;
use lagodb_core::transaction::{self, TransactionResource, TransactionResult};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use super::super::{gucs, state};
use super::control::StorageVolumeControl;
use super::domain::{StorageVolumeError, StorageVolumeName};
use super::lifecycle::UnixMillis;
use super::store::StorageVolumeConfigStore;

#[derive(Debug, Error)]
pub(crate) enum StorageVolumeRetirementError {
    #[error(transparent)]
    Volume(#[from] StorageVolumeError),
    #[error(transparent)]
    Tablespace(#[from] TablespaceCacheError),
    #[error(
        "storage volume {volume_id} is still bound to tablespace OID {tablespace_oid}"
    )]
    NotOrphan {
        volume_id: StorageVolumeId,
        tablespace_oid: u32,
    },
}

impl SqlStateError for StorageVolumeRetirementError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::Volume(error) => error.sql_error_code(),
            Self::Tablespace(error) => error.sql_error_code(),
            Self::NotOrphan { .. } => PgSqlErrorCode::ERRCODE_OBJECT_IN_USE,
        }
    }
}

/// Receive runtime-owned OAT_DROP events for tablespaces.
pub(crate) fn on_object_access(
    access: pg_sys::ObjectAccessType::Type,
    class_id: pg_sys::Oid,
    object_id: pg_sys::Oid,
    sub_id: i32,
) -> Result<(), StorageVolumeRetirementError> {
    if access != pg_sys::ObjectAccessType::OAT_DROP
        || class_id != pg_sys::TableSpaceRelationId
        || sub_id != 0
    {
        return Ok(());
    }
    if ensure_preloaded().is_err() {
        return Ok(());
    }

    let control = StorageVolumeControl::current();
    let volume_id = match control.find_bound_tablespace(object_id) {
        Ok(volume_id) => volume_id,
        Err(StorageVolumeError::ConfigIo { source, .. })
            if source.kind() == ErrorKind::NotFound =>
        {
            // No config can contain a managed binding before the worker has
            // initialized the file. Native tablespace drops remain independent
            // of that startup window.
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    let Some(volume_id) = volume_id else {
        return Ok(());
    };
    PendingVolumeRetirementBatch::stage(PendingVolumeRetirement {
        volume_id,
        tablespace_oid: object_id.to_u32(),
        retirement_grace_ms: gucs::storage_volume_retirement_grace_period_ms(),
        nest_level: unsafe { pg_sys::GetCurrentTransactionNestLevel() },
    });
    Ok(())
}

pub(crate) fn repair(
    name: &StorageVolumeName,
) -> Result<bool, StorageVolumeRetirementError> {
    let control = StorageVolumeControl::current();
    let snapshot = control.snapshot()?;
    let volume = snapshot.find(name)?;
    let Some(tablespace_oid) = volume.lifecycle.bound_tablespace_oid() else {
        return Err(StorageVolumeError::NotBound.into());
    };
    if get_tablespace(pg_sys::Oid::from(tablespace_oid))?
        .is_some_and(|binding| binding.volume_id() == volume.id)
    {
        return Err(StorageVolumeRetirementError::NotOrphan {
            volume_id: volume.id,
            tablespace_oid,
        });
    }

    let changed = control.repair(
        name,
        volume.id,
        tablespace_oid,
        UnixMillis::now()?,
        gucs::storage_volume_retirement_grace_period_ms(),
    )?;
    Ok(changed)
}

#[derive(Clone, Copy, Debug)]
struct PendingVolumeRetirement {
    volume_id: StorageVolumeId,
    tablespace_oid: u32,
    retirement_grace_ms: u64,
    nest_level: i32,
}

#[derive(Debug, Default)]
struct PendingVolumeRetirementBatch {
    pending: RefCell<Vec<PendingVolumeRetirement>>,
}

thread_local! {
    static CURRENT: RefCell<Option<Rc<PendingVolumeRetirementBatch>>> =
        const { RefCell::new(None) };
}

impl PendingVolumeRetirementBatch {
    fn stage(pending: PendingVolumeRetirement) {
        let batch = Self::current();
        batch.pending.borrow_mut().push(pending);
    }

    fn current() -> Rc<Self> {
        CURRENT.with(|slot| {
            let mut slot = slot.borrow_mut();
            if let Some(batch) = slot.as_ref() {
                return Rc::clone(batch);
            }
            let batch = Rc::new(Self::default());
            transaction::register_resource(
                Rc::clone(&batch) as Rc<dyn TransactionResource>
            );
            *slot = Some(Rc::clone(&batch));
            batch
        })
    }

    fn commit(&self) -> Result<(), StorageVolumeError> {
        let pending = std::mem::take(&mut *self.pending.borrow_mut());
        if pending.is_empty() {
            return Ok(());
        }
        let store = StorageVolumeConfigStore::for_current_data_directory();
        let result = store.update(|snapshot| {
            let marked_at_ms = UnixMillis::now()?;
            let mut changed = false;
            for pending in &pending {
                changed |= snapshot.retire(
                    pending.volume_id,
                    pending.tablespace_oid,
                    marked_at_ms,
                    pending.retirement_grace_ms,
                )?;
            }
            Ok(((), changed))
        });
        match result {
            Ok((_, changed)) => {
                if changed {
                    StorageVolumeControl::request_reload(false);
                }
                Ok(())
            }
            Err(error) => {
                if error.was_published() {
                    StorageVolumeControl::request_reload(false);
                }
                Err(error)
            }
        }
    }
}

impl TransactionResource for PendingVolumeRetirementBatch {
    fn nest_level(&self) -> i32 {
        // Entries carry their own savepoint level; keeping the resource at the
        // top level lets one batch survive sibling savepoint operations.
        1
    }

    fn set_nest_level(&self, _level: i32) {}

    fn on_commit(&self) {
        if let Err(error) = self.commit() {
            let message = format!(
                "storage volume retirement post-commit update failed: {}",
                error.diagnostic_message(),
            );
            state::StorageStatusStore::new().record_error(&message);
            report_warning(message);
        }
        CURRENT.with(|slot| *slot.borrow_mut() = None);
    }

    fn on_abort(&self) {
        self.pending.borrow_mut().clear();
        CURRENT.with(|slot| *slot.borrow_mut() = None);
    }

    fn on_pre_prepare(&self) -> TransactionResult<()> {
        if self.pending.borrow().is_empty() {
            return Ok(());
        }
        Err(PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            "cannot PREPARE a transaction with a pending storage volume retirement",
        ))
    }

    fn on_commit_sub(&self, current_nest_level: i32) {
        for pending in self.pending.borrow_mut().iter_mut() {
            if pending.nest_level >= current_nest_level {
                pending.nest_level = current_nest_level - 1;
            }
        }
    }

    fn on_abort_sub(&self, current_nest_level: i32) {
        self.pending
            .borrow_mut()
            .retain(|pending| pending.nest_level < current_nest_level);
    }
}
