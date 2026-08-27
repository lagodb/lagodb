//! Authorization boundary for PostgreSQL column `OAT_DROP` events.
//!
//! Supported `ALTER TABLE DROP COLUMN` statements authorize their exact
//! `(relation, attribute)` pairs before PostgreSQL executes the catalog change.
//! The object-access hook consumes those pairs. Any other Iceberg column drop
//! is dependency-driven or otherwise outside the supported DDL path and is
//! rejected. PG passes the same `dropflags` for ordinary ALTER-driven and many
//! dependency-driven drops, so `OAT_DROP` cannot identify the source itself.

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use lagodb_core::hooks::HookError;
use lagodb_core::transaction::{self, TransactionResource};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

type ColumnDropKey = (pg_sys::Oid, i32);

#[derive(Debug)]
struct AuthorizationFrame {
    // Frames are LIFO utility-command scopes; nest_level is independently the
    // PostgreSQL savepoint level used when ERROR skips the utility post-hook.
    nest_level: i32,
    allowed: HashSet<ColumnDropKey>,
}

#[derive(Debug, Default)]
struct ColumnDropAuthorizationResource {
    frames: RefCell<Vec<AuthorizationFrame>>,
}

thread_local! {
    static CURRENT: RefCell<Option<Rc<ColumnDropAuthorizationResource>>> =
        const { RefCell::new(None) };
}

pub(crate) struct ControlledColumnDrops;

impl ControlledColumnDrops {
    pub(crate) fn authorize(keys: impl IntoIterator<Item = ColumnDropKey>) {
        let allowed: HashSet<_> = keys.into_iter().collect();
        if allowed.is_empty() {
            return;
        }

        let resource = Self::current();
        resource.frames.borrow_mut().push(AuthorizationFrame {
            nest_level: unsafe { pg_sys::GetCurrentTransactionNestLevel() },
            allowed,
        });
    }

    pub(crate) fn consume(relid: pg_sys::Oid, attnum: i32) -> bool {
        CURRENT.with(|slot| {
            let slot = slot.borrow();
            let Some(resource) = slot.as_ref() else {
                return false;
            };
            let mut frames = resource.frames.borrow_mut();
            // A boolean "inside DROP COLUMN" or a transaction-wide set would
            // let nested utility commands consume an outer statement's grant.
            frames
                .last_mut()
                .is_some_and(|frame| frame.allowed.remove(&(relid, attnum)))
        })
    }

    pub(crate) fn finish() -> Result<(), HookError> {
        CURRENT.with(|slot| {
            let slot = slot.borrow();
            let Some(resource) = slot.as_ref() else {
                return Err(Self::incomplete_authorization_error(0));
            };
            let Some(frame) = resource.frames.borrow_mut().pop() else {
                return Err(Self::incomplete_authorization_error(0));
            };
            if frame.allowed.is_empty() {
                Ok(())
            } else {
                Err(Self::incomplete_authorization_error(frame.allowed.len()))
            }
        })
    }

    fn current() -> Rc<ColumnDropAuthorizationResource> {
        CURRENT.with(|slot| {
            let mut slot = slot.borrow_mut();
            if let Some(resource) = slot.as_ref() {
                return Rc::clone(resource);
            }
            let resource = Rc::new(ColumnDropAuthorizationResource::default());
            transaction::register_resource(
                Rc::clone(&resource) as Rc<dyn TransactionResource>
            );
            *slot = Some(Rc::clone(&resource));
            resource
        })
    }

    fn incomplete_authorization_error(remaining: usize) -> HookError {
        HookError::with_code(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!(
                "Iceberg ALTER TABLE DROP COLUMN did not observe all expected PostgreSQL column-drop callbacks ({remaining} remaining)"
            ),
        )
    }
}

impl TransactionResource for ColumnDropAuthorizationResource {
    fn nest_level(&self) -> i32 {
        // The resource is transaction-scoped. Individual authorization frames
        // carry their own savepoint level.
        1
    }

    fn set_nest_level(&self, _level: i32) {}

    fn on_commit(&self) {
        CURRENT.with(|slot| *slot.borrow_mut() = None);
    }

    fn on_abort(&self) {
        CURRENT.with(|slot| *slot.borrow_mut() = None);
    }

    fn on_commit_sub(&self, current_nest_level: i32) {
        for frame in self.frames.borrow_mut().iter_mut() {
            if frame.nest_level >= current_nest_level {
                frame.nest_level = current_nest_level - 1;
            }
        }
    }

    fn on_abort_sub(&self, current_nest_level: i32) {
        self.frames
            .borrow_mut()
            .retain(|frame| frame.nest_level < current_nest_level);
    }
}
