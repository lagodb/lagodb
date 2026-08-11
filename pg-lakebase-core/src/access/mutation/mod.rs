//! PG17 Custom ModifyTable execution state and the independent COPY FROM
//! table-AM callback lifecycle.
//!
//! ## Backend-local state
//!
//! PostgreSQL runs executor callbacks on one backend thread, so the mutation
//! subsystem uses backend-local registries instead of process-global locks:
//!
//! - `ACTIVE_MODIFY_QUERIES` routes an `EState` and access-method type to weak
//!   query state; dead entries are removed on acquisition.
//! - `ACTIVE_STORES` routes synthetic trigger-row identities to weak,
//!   query-owned tuplestores; dead entries are removed on lookup.
//! - `NEXT_TRIGGER_ROW_TOKEN` allocates a backend-wide monotonic identity
//!   namespace for preserved trigger rows.
//! - `COPY_FRAMES` and `NEXT_COPY_ID` own nested COPY FROM sessions and pair
//!   them with ResourceOwner cleanup.
//!
//! Modify-purpose reads are always provider CustomScans. Their explicit
//! [`ModifyScanBinding`] carries scan identity registration into the owning
//! ModifyTable state; no thread-local scan handshake is involved.

mod binding;
mod callbacks;
mod erased_session;
mod lifecycle;
mod modify_query;
mod session;
pub(crate) mod trigger_rows;

pub use binding::ModifyScanBinding;
pub use lifecycle::{CopyFromLifecycleGuard, begin_copy_from_lifecycle};
pub(crate) use modify_query::acquire as acquire_modify_query_state;

use crate::api::TableAccessMethod;
use pgrx::pg_sys;

pub fn register<A: TableAccessMethod>(routine: &mut pg_sys::TableAmRoutine) {
    routine.tuple_insert = Some(callbacks::tuple_insert::<A>);
    routine.tuple_insert_speculative = Some(callbacks::tuple_insert_speculative);
    routine.tuple_complete_speculative = Some(callbacks::tuple_complete_speculative);
    routine.multi_insert = Some(callbacks::multi_insert::<A>);
    routine.tuple_delete = Some(callbacks::tuple_delete);
    routine.tuple_update = Some(callbacks::tuple_update);
    routine.tuple_lock = Some(callbacks::tuple_lock);
    routine.finish_bulk_insert = Some(callbacks::finish_bulk_insert::<A>);
}
