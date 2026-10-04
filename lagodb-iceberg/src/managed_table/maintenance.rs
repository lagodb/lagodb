mod cleanup;
mod commit_attempt;
pub mod error;
mod injection_points;
mod notification;
mod operations;
mod planner;
mod reachability;
mod types;
mod worker;
mod writer;

pub(crate) use types::{PreparedVacuum, record_metric};

pub(super) use operations::{IcebergTableMaintenance, MaintenanceExecution};

pub(crate) use cleanup::VacuumCleanup;
pub(crate) use commit_attempt::{
    VacuumAttemptOutcome, VacuumAttemptResult, VacuumCommitAttempt,
};
pub(crate) use notification::AutomaticMaintenanceNotifier;
pub(crate) use reachability::{
    IcebergReachabilityPlanner, ReachabilityDeletionCandidates,
};
