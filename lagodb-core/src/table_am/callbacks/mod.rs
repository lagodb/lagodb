//! PostgreSQL `TableAmRoutine` callbacks over the table-AM contract.
//!
//! Every function installed in the routine is an `extern "C-unwind"` FFI
//! boundary that adapts PostgreSQL's raw callback to the provider traits in
//! [`crate::table_am`]:
//! - `scan`: sequential, TID-range, bitmap, sample, and parallel scans
//! - `relation`: relation-level operations
//! - `index`: index fetch and build
//! - `mutation`: tuple mutation callbacks (INSERT/UPDATE/DELETE) and COPY
//! - `ddl`: storage DDL operations

mod common;
mod ddl;
mod index;
mod mutation;
mod relation;
mod scan;

use pgrx::{AllocatedByPostgres, PgMemoryContexts, pg_sys};

pub(crate) use mutation::{RelationTriggerPolicy, acquire_modify_query_state};
pub(crate) use scan::TableAmScanDesc;
pub use scan::virtual_slot_callbacks_with_tid;

use super::{TableAccessMethod, TableAmRoutine};

/// Allocate `A`'s routine in `TopMemoryContext` with every callback installed.
pub(super) fn build_routine<A>() -> TableAmRoutine
where
    A: TableAccessMethod,
{
    unsafe {
        let mut am_routine = PgMemoryContexts::TopMemoryContext.switch_to(|_| {
            TableAmRoutine::<AllocatedByPostgres>::alloc_node(
                pg_sys::NodeTag::T_TableAmRoutine,
            )
        });

        scan::register::<A>(&mut am_routine);
        relation::register::<A>(&mut am_routine);
        index::register::<A>(&mut am_routine);
        mutation::register(&mut am_routine);
        ddl::register::<A>(&mut am_routine);

        TableAmRoutine::from_pg(am_routine.into_pg())
    }
}
