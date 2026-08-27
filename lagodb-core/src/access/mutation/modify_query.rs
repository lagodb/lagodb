//! Executor-query ownership for access-method Modify state.
//!
//! PostgreSQL can initialize several ModifyTable nodes in one `EState` (for
//! example data-modifying CTEs).  The routing table below lets those nodes share
//! one typed AM query state without making the AM aware of PostgreSQL executor
//! pointers.  It owns only `Weak` references; initialized Modify nodes retain
//! the strong handles and therefore define the actual lifetime.

use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::ptr::NonNull;
use std::rc::{Rc, Weak};

use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::api::{AmResult, ModifyQueryShared, ModifyQueryState, TableAccessMethod};
use crate::diag::PgReportError;

struct ActiveModifyQuery {
    estate: NonNull<pg_sys::EState>,
    access_method: TypeId,
    state: Weak<dyn Any>,
}

thread_local! {
    static ACTIVE_MODIFY_QUERIES: RefCell<Vec<ActiveModifyQuery>> =
        const { RefCell::new(Vec::new()) };
}

fn internal_error(message: impl Into<String>) -> PgReportError {
    PgReportError::from_message(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, message)
}

/// Acquire the typed AM state shared by all ModifyTable nodes in `estate`.
///
/// Dead weak entries are removed on every acquisition, so an `EState` address
/// reused by a later executor cannot observe an earlier query's state.
pub(crate) fn acquire<A: TableAccessMethod>(
    estate: *mut pg_sys::EState,
) -> AmResult<ModifyQueryState<A::ModifyQueryState>> {
    let estate = NonNull::new(estate)
        .ok_or_else(|| internal_error("Modify query has a NULL executor state"))?;
    let access_method = TypeId::of::<A>();

    let existing = ACTIVE_MODIFY_QUERIES.with_borrow_mut(|queries| {
        queries.retain(|query| query.state.strong_count() > 0);
        queries
            .iter()
            .find(|query| {
                query.estate == estate && query.access_method == access_method
            })
            .and_then(|query| query.state.upgrade())
    });

    if let Some(existing) = existing {
        let state = existing
            .downcast::<ModifyQueryShared<A::ModifyQueryState>>()
            .map_err(|_| {
                internal_error("Modify query state has the wrong AM type")
            })?;
        return Ok(ModifyQueryState::from_shared(state));
    }

    let state = Rc::new(ModifyQueryShared::<A::ModifyQueryState>::new()?);
    let erased: Rc<dyn Any> = state.clone();
    ACTIVE_MODIFY_QUERIES.with_borrow_mut(|queries| {
        queries.push(ActiveModifyQuery {
            estate,
            access_method,
            state: Rc::downgrade(&erased),
        });
    });
    Ok(ModifyQueryState::from_shared(state))
}
