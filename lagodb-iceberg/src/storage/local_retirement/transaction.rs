//! Savepoint-aware retirement collection transferred at top-level commit.

use std::cell::RefCell;
use std::mem;
use std::rc::Rc;

use lagodb_core::diag::PgReportError;
use lagodb_core::resource::CommittedCleanup;
use lagodb_core::transaction::{
    TransactionResource, TransactionResult, register_resource,
};
use lagodb_core::wal::flush_wal;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use super::LocalTableRetirement;

#[derive(Debug)]
struct TrackedRetirement {
    nest_level: i32,
    retirement: LocalTableRetirement,
}

thread_local! {
    static CURRENT: RefCell<Option<Rc<LocalTableRetirements>>> =
        const { RefCell::new(None) };
}

#[derive(Debug)]
pub(super) struct LocalTableRetirements {
    entries: RefCell<Vec<TrackedRetirement>>,
}

impl LocalTableRetirements {
    pub(super) fn register(retirement: LocalTableRetirement) {
        // SAFETY: retirement is registered while PostgreSQL owns an active
        // transaction. Only the copied nesting level is retained.
        let nest_level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };
        CURRENT.with(|current| {
            let mut current = current.borrow_mut();
            let resource =
                current.get_or_insert_with(|| {
                    let resource = Rc::new(Self {
                        entries: RefCell::new(Vec::new()),
                    });
                    register_resource(
                        Rc::clone(&resource) as Rc<dyn TransactionResource>
                    );
                    resource
                });
            resource.entries.borrow_mut().push(TrackedRetirement {
                nest_level,
                retirement,
            });
        });
    }
}

impl TransactionResource for LocalTableRetirements {
    fn nest_level(&self) -> i32 {
        // The collection survives child aborts; each entry owns its savepoint.
        1
    }

    fn set_nest_level(&self, _level: i32) {}

    fn on_pre_prepare(&self) -> TransactionResult<()> {
        if self.entries.borrow().is_empty() {
            return Ok(());
        }
        // Preserve pending-delete's policy: backend-local retirement cannot
        // survive PREPARE because it has no durable two-phase representation.
        Err(PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            "cannot prepare a transaction with pending external-object cleanup",
        ))
    }

    fn on_commit(&self) {
        let entries = mem::take(&mut *self.entries.borrow_mut());
        CURRENT.with(|current| *current.borrow_mut() = None);
        if !entries.is_empty() {
            // Capture at COMMIT, not PRE_COMMIT: PG's ON COMMIT DROP runs
            // after PRE_COMMIT callbacks and can still register retirements.
            let batch = LocalTableRetirementBatch { entries };
            CommittedCleanup::defer(move || batch.execute());
        }
    }

    fn on_prepare(&self) {
        // PRE_PREPARE accepted only an empty collection. Release its backend
        // reference; core drops the registration without executing retirement.
        CURRENT.with(|current| *current.borrow_mut() = None);
    }

    fn on_abort(&self) {
        // Abort cancels retirement; creation resources own new-generation cleanup.
        self.entries.borrow_mut().clear();
        CURRENT.with(|current| *current.borrow_mut() = None);
    }

    fn on_commit_sub(&self, current_nest_level: i32) {
        for entry in self.entries.borrow_mut().iter_mut() {
            if entry.nest_level >= current_nest_level {
                entry.nest_level = current_nest_level - 1;
            }
        }
    }

    fn on_abort_sub(&self, current_nest_level: i32) {
        self.entries
            .borrow_mut()
            .retain(|entry| entry.nest_level < current_nest_level);
    }
}

struct LocalTableRetirementBatch {
    entries: Vec<TrackedRetirement>,
}

impl LocalTableRetirementBatch {
    fn execute(&self) {
        let mut last_lsn = None;
        for entry in &self.entries {
            if let Some(lsn) = entry.retirement.log_delete_wal() {
                last_lsn = Some(lsn);
            }
        }
        if let Some(lsn) = last_lsn {
            // The last record covers every earlier retirement record and any
            // preceding async commit. Flush before removing directories and
            // before handing reservations to PG's checkpoint unlink queue,
            // so its next eligible REDO point excludes both deletion records.
            flush_wal(lsn);
        }
        for entry in &self.entries {
            entry.retirement.delete_storage();
        }
    }
}
