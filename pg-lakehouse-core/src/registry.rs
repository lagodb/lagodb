use crate::api::TableAccessMethod;
use crate::TableAmRoutine;
use pgrx::{pg_sys, PgMemoryContexts};
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::AllocatedByPostgres;
use std::sync::Once;

pub fn make_table_am_routine<E, T>() -> TableAmRoutine
where
    E: Into<ErrorReport>,
    T: TableAccessMethod<E>,
{
    unsafe {
        static mut CACHED_ROUTINE: *mut pg_sys::TableAmRoutine = std::ptr::null_mut();
        static INIT: Once = Once::new();

        INIT.call_once(|| {
            let mut am_routine = PgMemoryContexts::TopMemoryContext.switch_to(|_| {
                TableAmRoutine::<AllocatedByPostgres>::alloc_node(pg_sys::NodeTag::T_TableAmRoutine)
            });

            crate::access::scan::register::<E, T::ScanState>(&mut am_routine);
            crate::access::relation::register::<E, T::RelationState>(&mut am_routine);
            crate::access::index::register::<E, T::IndexState>(&mut am_routine);
            crate::access::dml::register::<E, T::ModifyState>(&mut am_routine);
            crate::access::ddl::register::<E, T::DdlState>(&mut am_routine);

            CACHED_ROUTINE = am_routine.into_pg();
        });

        TableAmRoutine::from_pg(CACHED_ROUTINE)
    }
}
