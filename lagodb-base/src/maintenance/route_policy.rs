//! Provider-neutral routing policy for table maintenance commands.

use pgrx::pg_sys;

#[derive(Clone, Copy)]
pub(crate) struct MaintenanceRoutePolicy {
    options: pg_sys::bits32,
}

impl MaintenanceRoutePolicy {
    pub(crate) const fn new(options: pg_sys::bits32) -> Self {
        Self { options }
    }

    pub(crate) const fn routes_vacuum_to_provider(
        self,
        partitioned_table: bool,
    ) -> bool {
        self.options & pg_sys::VACOPT_VACUUM != 0
            && self.options & pg_sys::VACOPT_PROCESS_MAIN != 0
            && (self.options & pg_sys::VACOPT_FULL != 0 || partitioned_table)
    }

    pub(crate) const fn routes_analyze_to_provider(
        self,
        partitioned_table: bool,
    ) -> bool {
        self.options & pg_sys::VACOPT_ANALYZE != 0 && partitioned_table
    }
}
