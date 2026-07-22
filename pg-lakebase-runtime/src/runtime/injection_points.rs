use pg_lakebase_core::injection_point::InjectionPoint;

/// Runtime-owned injection points at stable process-lifecycle boundaries.
pub(super) struct RuntimeInjectionPoints;

impl RuntimeInjectionPoints {
    /// The dynamic worker has connected to its target database but has not yet
    /// loaded or invoked the registered extension entrypoint.
    pub(super) const WORKER_AFTER_DATABASE_CONNECTION: InjectionPoint =
        InjectionPoint::new(c"lakebase-runtime-worker-after-database-connection");
}
