//! PostgreSQL Custom ModifyTable execution state and the independent COPY FROM
//! explicit COPY execution lifecycle.
//!
//! ## Backend-local state
//!
//! PostgreSQL runs executor callbacks on one backend thread, so the mutation
//! subsystem uses backend-local registries instead of process-global locks:
//!
//! - `ACTIVE_MODIFY_QUERIES` routes an `EState` and access-method type to weak
//!   query state; dead entries are removed on acquisition.
//! - `ACTIVE_STORES` routes synthetic trigger-row identities to weak,
//!   owner-held tuplestores; handles unregister on destruction.
//! - `NEXT_TRIGGER_ROW_TOKEN` allocates a backend-wide monotonic identity
//!   namespace for preserved trigger rows.
//! - `COPY_FRAMES` and `NEXT_COPY_ID` retain nested command IDs for FDW
//!   providers that explicitly request them, with ResourceOwner cleanup.
//!   AM sessions belong to the driver owner and are never looked up here.
//!
//! Modification target reads use native SeqScan sessions or provider CustomScans.
//! Their explicit [`ModifyScanBinding`] carries scan identity registration into
//! the owning ModifyTable state; no thread-local scan handshake is involved.

mod binding;
mod callbacks;
mod command;
mod copy_adapter;
mod copy_trigger_rows;
mod lifecycle;
mod modify_query;
mod session;
mod trigger_policy;
pub(crate) mod trigger_rows;

pub use binding::ModifyScanBinding;
pub(crate) use command::current_copy_command_id;
pub use lifecycle::init_lifecycle_hooks;
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
