//! Maintenance wire records exchanged between the runtime and table providers.
//!
//! These are exact-build `#[repr(C)]` records only. Conversion to and from the
//! provider-facing maintenance types lives in [`crate::table_maintenance`], so
//! this ABI layer does not depend on the domain layer.

use pgrx::pg_sys;

use super::CallbackErrorReport;

pub const FORMAT_NAME_CAPACITY: usize = 32;

/// Wire encoding of PostgreSQL's VACUUM intensity.
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceMode {
    Routine = 0,
    Full = 1,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RuntimeMaintenanceConfig {
    pub enabled: u8,
    pub _padding: [u8; 3],
    pub actor_threads: i32,
    pub batch_items: i32,
    pub retry_base_ms: i32,
    pub retry_max_ms: i32,
    pub retry_max_attempts: i32,
    pub request_timeout_ms: i32,
    pub shutdown_timeout_ms: i32,
    pub vacuum_max_input_objects: i32,
    pub vacuum_max_input_mb: i32,
    pub vacuum_max_group_objects: i32,
    pub vacuum_max_group_mb: i32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct MaintenanceRequest {
    pub relation: pg_sys::Relation,
    pub mode: MaintenanceMode,
    pub option_flags: u32,
    pub max_input_objects: u64,
    pub max_input_bytes: u64,
    pub max_group_objects: u64,
    pub max_group_bytes: u64,
    pub command_time_ms: i64,
}

impl MaintenanceRequest {
    /// `option_flags` bit for `VACUUM (VERBOSE)`.
    pub const OPTION_VERBOSE: u32 = 1 << 0;
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct MaintenanceReport {
    pub groups_rewritten: u64,
    pub input_objects: u64,
    pub input_bytes: u64,
    pub output_objects: u64,
    pub output_bytes: u64,
    pub snapshots_expired: u64,
    pub manifests_rewritten: u64,
    pub objects_scheduled_for_deletion: u64,
    pub cas_retries: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct MaintenanceStats {
    pub format: [u8; FORMAT_NAME_CAPACITY],
    pub history_points: u64,
    pub current_content_objects: u64,
    pub current_content_bytes: u64,
    pub retained_content_objects: u64,
    pub retained_content_bytes: u64,
    pub current_data_objects: u64,
    pub current_data_bytes: u64,
    pub retained_data_objects: u64,
    pub retained_data_bytes: u64,
}

/// Return CALLBACK_OK with an initialized report, or CALLBACK_FAILED with a
/// synchronous PostgreSQL-owned error payload. This callback must not report
/// to PostgreSQL or let a Rust panic cross the DSO boundary.
pub type ExecuteMaintenanceCallback = unsafe extern "C-unwind" fn(
    request: *const MaintenanceRequest,
    report: *mut MaintenanceReport,
    error: *mut CallbackErrorReport,
) -> u32;
/// Uses the same output/error contract as ExecuteMaintenanceCallback.
pub type InspectMaintenanceCallback = unsafe extern "C-unwind" fn(
    relation: pg_sys::Relation,
    stats: *mut MaintenanceStats,
    error: *mut CallbackErrorReport,
) -> u32;
