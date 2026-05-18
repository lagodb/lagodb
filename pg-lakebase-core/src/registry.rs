use crate::TableAmRoutine;
use crate::api::TableAccessMethod;
use pgrx::AllocatedByPostgres;
use pgrx::{PgMemoryContexts, pg_sys};

pub fn build_table_am_routine<A>() -> TableAmRoutine
where
    A: TableAccessMethod,
{
    unsafe {
        let mut am_routine = PgMemoryContexts::TopMemoryContext.switch_to(|_| {
            TableAmRoutine::<AllocatedByPostgres>::alloc_node(
                pg_sys::NodeTag::T_TableAmRoutine,
            )
        });

        crate::access::scan::register::<A>(&mut am_routine);
        crate::access::relation::register::<A>(&mut am_routine);
        crate::access::index::register::<A>(&mut am_routine);
        crate::access::dml::register::<A>(&mut am_routine);
        crate::access::ddl::register::<A>(&mut am_routine);

        TableAmRoutine::from_pg(am_routine.into_pg())
    }
}
