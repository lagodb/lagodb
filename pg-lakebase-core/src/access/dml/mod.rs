//! DML (INSERT/UPDATE/DELETE/MERGE) FFI integration for the table-AM API.
//!
//! PostgreSQL's [`pg_sys::TableAmRoutine`] is the bottom edge of DML
//! execution.  It tells the AM "insert this tuple" or "update this tuple", but
//! it does not provide statement/frame setup and teardown callbacks.  The
//! surrounding modules add that missing lifecycle:
//!
//! - executor/utility hooks establish the current managed DML frame,
//! - these table-AM callbacks convert raw PostgreSQL arguments into Rust
//!   handles and dispatch to the current frame's session,
//! - the session manager maps those callbacks to AM `begin_modify`,
//!   `end_modify`, and `abort_modify` semantics.
//!
//! Layered roughly bottom-up:
//!
//! - [`erased_session`]: type-erasure adapter that lets the session manager store
//!   one `Box<dyn ...>` regardless of the concrete `AmDmlSession` implementation.
//! - [`session`]: frame-scoped DML session storage and lifecycle.
//! - [`modifytable_wrapper`]: wraps ModifyTable `ExecProcNodeReal` to discover
//!   frame boundaries and drive the session lifecycle from them.
//! - [`callbacks`]: the `extern "C-unwind"` shims wired into PostgreSQL's
//!   [`pg_sys::TableAmRoutine`] vtable.
//! - [`lifecycle`]: executor and utility hook wiring that creates DML frame
//!   boundaries around those callbacks.

mod callbacks;
mod erased_session;
mod lifecycle;
mod modifytable_wrapper;
mod session;

pub use lifecycle::init_lifecycle_hooks;

use crate::api::TableAccessMethod;
use pgrx::pg_sys;

pub fn register<A: TableAccessMethod>(routine: &mut pg_sys::TableAmRoutine) {
    routine.tuple_insert = Some(callbacks::tuple_insert::<A>);
    routine.tuple_insert_speculative = Some(callbacks::tuple_insert_speculative::<A>);
    routine.tuple_complete_speculative =
        Some(callbacks::tuple_complete_speculative::<A>);
    routine.multi_insert = Some(callbacks::multi_insert::<A>);
    routine.tuple_delete = Some(callbacks::tuple_delete::<A>);
    routine.tuple_update = Some(callbacks::tuple_update::<A>);
    routine.tuple_lock = Some(callbacks::tuple_lock::<A>);
    routine.finish_bulk_insert = Some(callbacks::finish_bulk_insert::<A>);
}
