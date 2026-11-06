//! Shared, single-backend owner of one executor query's modify state.

use std::cell::RefCell;
use std::rc::Rc;

use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use super::AmModifyQueryState;
use super::trigger_rows::TriggerQueryState;
use crate::diag::PgReportError;
use crate::handles::ItemPointer;
use crate::table_am::{AmResult, TableAccessMethod};

/// Typed, single-backend handle to AM state shared by every LagoDB
/// ModifyTable node in one PostgreSQL executor query.
pub(crate) struct ModifyQueryShared<Q: AmModifyQueryState> {
    provider: RefCell<Q>,
    trigger_rows: RefCell<TriggerQueryState>,
}

impl<Q: AmModifyQueryState> ModifyQueryShared<Q> {
    pub(crate) fn new() -> AmResult<Self> {
        Ok(Self {
            provider: RefCell::new(Q::new()?),
            trigger_rows: RefCell::new(TriggerQueryState::default()),
        })
    }
}

pub struct ModifyQueryState<Q: AmModifyQueryState> {
    inner: Rc<ModifyQueryShared<Q>>,
}

impl<Q: AmModifyQueryState> Clone for ModifyQueryState<Q> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl<Q: AmModifyQueryState> ModifyQueryState<Q> {
    /// Create an independent query-state owner.
    ///
    /// Normal ModifyTable execution acquires this handle from core's `EState`
    /// registry so sibling nodes share it. Independent write paths such as
    /// COPY can use this constructor because they have no target scan identity
    /// to share.
    pub fn new() -> AmResult<Self> {
        Ok(Self {
            inner: Rc::new(ModifyQueryShared::new()?),
        })
    }

    pub(crate) fn from_shared(inner: Rc<ModifyQueryShared<Q>>) -> Self {
        Self { inner }
    }

    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
    }

    /// Mutably borrow the AM query state for one short, non-reentrant operation.
    pub fn update<R>(
        &self,
        operation: impl FnOnce(&mut Q) -> AmResult<R>,
    ) -> AmResult<R> {
        let mut state = self.inner.provider.try_borrow_mut().map_err(|_| {
            PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "Modify query state is already borrowed",
            )
        })?;
        operation(&mut state)
    }

    /// Abandon core-owned rows when this executor query is being discarded.
    /// This is independent of whether provider writes have already finished.
    pub(crate) fn abandon_trigger_rows(&self) {
        self.inner.trigger_rows.borrow_mut().abandon();
    }

    /// Preserve one complete relation-shaped row for a queued AFTER ROW
    /// trigger in the core-owned query state.
    ///
    /// # Safety
    ///
    /// PostgreSQL must keep `tuple_desc` live for the query and `slot` live for
    /// this call.
    pub(crate) unsafe fn preserve_trigger_row<A: TableAccessMethod>(
        &self,
        relation_oid: pg_sys::Oid,
        tuple_desc: pg_sys::TupleDesc,
        slot: *mut pg_sys::TupleTableSlot,
    ) -> AmResult<ItemPointer> {
        let mut trigger_rows =
            self.inner.trigger_rows.try_borrow_mut().map_err(|_| {
                PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    "Modify query trigger-row state is already borrowed",
                )
            })?;
        unsafe { trigger_rows.preserve::<A>(relation_oid, tuple_desc, slot) }
    }
}
