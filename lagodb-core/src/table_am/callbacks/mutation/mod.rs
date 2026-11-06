//! PostgreSQL Custom ModifyTable execution state and the explicit COPY
//! execution adapter.
//!
//! ## Backend-local state
//!
//! PostgreSQL runs executor callbacks on one backend thread, so the mutation
//! subsystem uses backend-local registries instead of process-global locks.
//! This module owns `ACTIVE_MODIFY_QUERIES`, which routes an `EState` and
//! access-method type to weak query state; dead entries are removed on
//! acquisition. The trigger-row registries (`ACTIVE_STORES` and
//! `NEXT_TRIGGER_ROW_TOKEN`) are shared by ModifyTable query state and COPY
//! sessions and live in `table_am::modify::trigger_rows`.
//!
//! Modification target reads use native SeqScan sessions or provider CustomScans.
//! Their explicit [`ModifyScanBinding`](crate::table_am::ModifyScanBinding)
//! carries scan identity registration into the owning ModifyTable state; no
//! thread-local scan handshake is involved.

mod callbacks;
mod copy_adapter;
mod copy_trigger_rows;
mod modify_query;
mod session;
mod trigger_policy;

pub(crate) use modify_query::acquire as acquire_modify_query_state;
pub(crate) use trigger_policy::RelationTriggerPolicy;

use pgrx::pg_sys;

pub fn register(routine: &mut pg_sys::TableAmRoutine) {
    routine.tuple_insert = Some(callbacks::tuple_insert);
    routine.tuple_insert_speculative = Some(callbacks::tuple_insert_speculative);
    routine.tuple_complete_speculative = Some(callbacks::tuple_complete_speculative);
    routine.multi_insert = Some(callbacks::multi_insert);
    routine.tuple_delete = Some(callbacks::tuple_delete);
    routine.tuple_update = Some(callbacks::tuple_update);
    routine.tuple_lock = Some(callbacks::tuple_lock);
    routine.finish_bulk_insert = Some(callbacks::finish_bulk_insert);
}
