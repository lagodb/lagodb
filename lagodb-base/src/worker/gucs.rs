//! GUCs for the worker supervisor and the worker restart policy.

use std::time::Duration;

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

use super::state::RestartPolicy;

static SUPERVISOR_NAPTIME_MS: GucSetting<i32> = GucSetting::<i32>::new(1_000);
static RECONCILE_INTERVAL_MS: GucSetting<i32> = GucSetting::<i32>::new(30_000);
static WORKER_RESTART_BACKOFF_INITIAL_MS: GucSetting<i32> =
    GucSetting::<i32>::new(5_000);
static WORKER_RESTART_BACKOFF_MAX_MS: GucSetting<i32> =
    GucSetting::<i32>::new(60_000);
static WORKER_RESTART_HEALTHY_MS: GucSetting<i32> = GucSetting::<i32>::new(60_000);

/// Supervisor timing, read again after every configuration reload.
#[derive(Clone, Copy)]
pub(super) struct SupervisorSettings {
    pub(super) naptime: Duration,
    pub(super) reconcile_interval: Duration,
}

impl SupervisorSettings {
    pub(super) fn current() -> Self {
        Self {
            naptime: Duration::from_millis(SUPERVISOR_NAPTIME_MS.get() as u64),
            reconcile_interval: Duration::from_millis(
                RECONCILE_INTERVAL_MS.get() as u64
            ),
        }
    }
}

impl RestartPolicy {
    /// The policy configured when a worker's exit is confirmed.
    pub(super) fn configured() -> Self {
        Self::new(
            Duration::from_millis(WORKER_RESTART_BACKOFF_INITIAL_MS.get() as u64),
            Duration::from_millis(WORKER_RESTART_BACKOFF_MAX_MS.get() as u64),
            Duration::from_millis(WORKER_RESTART_HEALTHY_MS.get() as u64),
        )
    }
}

pub(super) fn define() {
    GucRegistry::define_int_guc(
        c"lagodb.worker_supervisor_naptime_ms",
        c"Idle polling interval for the LagoDB supervisor",
        c"Value is measured in milliseconds.",
        &SUPERVISOR_NAPTIME_MS,
        10,
        60_000,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"lagodb.worker_reconcile_interval_ms",
        c"Full reconciliation safety interval for LagoDB workers",
        c"Value is measured in milliseconds.",
        &RECONCILE_INTERVAL_MS,
        100,
        3_600_000,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"lagodb.worker_restart_backoff_initial_ms",
        c"Initial delay before restarting a failed LagoDB worker",
        c"Value is measured in milliseconds.",
        &WORKER_RESTART_BACKOFF_INITIAL_MS,
        0,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"lagodb.worker_restart_backoff_max_ms",
        c"Maximum delay before restarting a repeatedly failing LagoDB worker",
        c"Value is measured in milliseconds.",
        &WORKER_RESTART_BACKOFF_MAX_MS,
        0,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"lagodb.worker_restart_healthy_ms",
        c"Worker uptime after which a failure resets the restart backoff",
        c"Value is measured in milliseconds.",
        &WORKER_RESTART_HEALTHY_MS,
        0,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::default(),
    );
}
