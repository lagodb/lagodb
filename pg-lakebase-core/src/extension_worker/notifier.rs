use std::cell::{Cell, RefCell};
use std::fmt;
use std::rc::Rc;

use crate::diag::PgReportError;
use crate::transaction::{self, TransactionResource, TransactionResult};
use pgrx::{pg_sys, prelude::*};

/// Stable catalog identity of a worker owned by an extension.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerIdentity {
    extension_name: &'static str,
    worker_name: &'static str,
}

impl WorkerIdentity {
    pub const fn new(
        extension_name: &'static str,
        worker_name: &'static str,
    ) -> Self {
        Self {
            extension_name,
            worker_name,
        }
    }
}

/// Transaction-aware notification client for the `pg_lakebase` runtime.
#[derive(Clone, Copy, Debug)]
pub struct WorkerNotifier {
    identity: WorkerIdentity,
}

impl WorkerNotifier {
    pub const fn new(identity: WorkerIdentity) -> Self {
        Self { identity }
    }

    /// Request a wakeup after the current top-level transaction commits.
    pub fn notify_after_commit(self) -> Result<(), pgrx::spi::Error> {
        let deduper = NotificationDeduper::current();
        if deduper.contains(self.identity) {
            return Ok(());
        }
        Spi::run_with_args(
            "SELECT lakebase.request_worker_wakeup($1, $2)",
            &[
                pgrx::datum::DatumWithOid::from(self.identity.extension_name),
                pgrx::datum::DatumWithOid::from(self.identity.worker_name),
            ],
        )?;
        deduper.record(self.identity);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct PendingNotification {
    identity: WorkerIdentity,
    nest_level: i32,
}

struct NotificationDeduper {
    notifications: RefCell<Vec<PendingNotification>>,
    nest_level: Cell<i32>,
}

impl fmt::Debug for NotificationDeduper {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NotificationDeduper")
            .field("notifications", &self.notifications.borrow().len())
            .field("nest_level", &self.nest_level.get())
            .finish()
    }
}

thread_local! {
    static NOTIFICATION_DEDUPER: RefCell<Option<Rc<NotificationDeduper>>> =
        const { RefCell::new(None) };
}

impl NotificationDeduper {
    fn current() -> Rc<Self> {
        NOTIFICATION_DEDUPER.with(|current| {
            if let Some(resource) = current.borrow().as_ref() {
                return Rc::clone(resource);
            }

            let resource = Rc::new(Self {
                notifications: RefCell::new(Vec::new()),
                nest_level: Cell::new(unsafe {
                    pg_sys::GetCurrentTransactionNestLevel()
                }),
            });
            transaction::register_resource(
                Rc::clone(&resource) as Rc<dyn TransactionResource>
            );
            *current.borrow_mut() = Some(Rc::clone(&resource));
            resource
        })
    }

    fn contains(&self, identity: WorkerIdentity) -> bool {
        self.notifications
            .borrow()
            .iter()
            .any(|notification| notification.identity == identity)
    }

    fn record(&self, identity: WorkerIdentity) {
        self.notifications.borrow_mut().push(PendingNotification {
            identity,
            nest_level: unsafe { pg_sys::GetCurrentTransactionNestLevel() },
        });
    }

    fn clear_current() {
        NOTIFICATION_DEDUPER.with(|current| {
            *current.borrow_mut() = None;
        });
    }
}

impl TransactionResource for NotificationDeduper {
    fn on_pre_prepare(&self) -> TransactionResult<()> {
        if self.notifications.borrow().is_empty() {
            return Ok(());
        }
        Err(PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            "cannot PREPARE a transaction with pending Lakebase worker notifications",
        ))
    }

    fn on_commit(&self) {
        Self::clear_current();
    }

    fn on_abort(&self) {
        Self::clear_current();
    }

    fn on_commit_sub(&self, current_nest_level: i32) {
        let parent_level = current_nest_level.saturating_sub(1);
        for notification in self
            .notifications
            .borrow_mut()
            .iter_mut()
            .filter(|notification| notification.nest_level >= current_nest_level)
        {
            notification.nest_level = parent_level;
        }
    }

    fn on_abort_sub(&self, current_nest_level: i32) {
        self.notifications
            .borrow_mut()
            .retain(|notification| notification.nest_level < current_nest_level);
        if self.nest_level.get() >= current_nest_level {
            Self::clear_current();
        }
    }

    fn nest_level(&self) -> i32 {
        self.nest_level.get()
    }

    fn set_nest_level(&self, level: i32) {
        self.nest_level.set(level);
    }
}
