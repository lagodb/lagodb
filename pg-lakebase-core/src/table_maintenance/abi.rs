//! Versioned PostgreSQL rendezvous ABI for runtime-owned maintenance routing.

use std::ffi::{CStr, c_char, c_void};
use std::sync::OnceLock;

use pgrx::pg_sys;

use super::{
    TableMaintenanceBudget, TableMaintenanceCommandTime, TableMaintenanceMode,
    TableMaintenanceOptions, TableMaintenanceReport, TableMaintenanceStats,
};

pub const RUNTIME_API_VERSION: u32 = 1;
pub const RUNTIME_API_RENDEZVOUS: &CStr = c"pg_lakebase.runtime_api.v1";
pub const MAINTENANCE_PROVIDER_VERSION: u32 = 1;
pub const FORMAT_NAME_CAPACITY: usize = 32;

pub const REGISTER_OK: u32 = 0;
pub const REGISTER_INVALID_DESCRIPTOR: u32 = 1;
pub const REGISTER_DUPLICATE_NAME: u32 = 2;
pub const REGISTER_DUPLICATE_ACCESS_METHOD: u32 = 3;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RuntimeMaintenanceConfigV1 {
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

const OPTION_VERBOSE: u32 = 1 << 0;
const OPTION_ANALYZE: u32 = 1 << 1;
const OPTION_SKIP_LOCKED: u32 = 1 << 2;
const OPTION_PROCESS_MAIN: u32 = 1 << 3;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct MaintenanceRequestV1 {
    pub relation: pg_sys::Relation,
    pub mode: u32,
    pub option_flags: u32,
    pub max_input_objects: u64,
    pub max_input_bytes: u64,
    pub max_group_objects: u64,
    pub max_group_bytes: u64,
    pub command_time_ms: i64,
}

impl MaintenanceRequestV1 {
    pub fn new(
        relation: pg_sys::Relation,
        mode: TableMaintenanceMode,
        options: TableMaintenanceOptions,
        budget: TableMaintenanceBudget,
        command_time: TableMaintenanceCommandTime,
    ) -> Self {
        let mut option_flags = 0;
        option_flags |= u32::from(options.verbose) * OPTION_VERBOSE;
        option_flags |= u32::from(options.analyze) * OPTION_ANALYZE;
        option_flags |= u32::from(options.skip_locked) * OPTION_SKIP_LOCKED;
        option_flags |= u32::from(options.process_main) * OPTION_PROCESS_MAIN;
        Self {
            relation,
            mode: match mode {
                TableMaintenanceMode::Routine => 0,
                TableMaintenanceMode::Full => 1,
            },
            option_flags,
            max_input_objects: budget.max_input_objects,
            max_input_bytes: budget.max_input_bytes,
            max_group_objects: budget.max_group_objects,
            max_group_bytes: budget.max_group_bytes,
            command_time_ms: command_time.unix_epoch_ms(),
        }
    }

    pub fn mode(self) -> Option<TableMaintenanceMode> {
        match self.mode {
            0 => Some(TableMaintenanceMode::Routine),
            1 => Some(TableMaintenanceMode::Full),
            _ => None,
        }
    }

    pub fn options(self) -> TableMaintenanceOptions {
        TableMaintenanceOptions {
            verbose: self.option_flags & OPTION_VERBOSE != 0,
            analyze: self.option_flags & OPTION_ANALYZE != 0,
            skip_locked: self.option_flags & OPTION_SKIP_LOCKED != 0,
            process_main: self.option_flags & OPTION_PROCESS_MAIN != 0,
        }
    }

    pub fn budget(self) -> TableMaintenanceBudget {
        TableMaintenanceBudget {
            max_input_objects: self.max_input_objects,
            max_input_bytes: self.max_input_bytes,
            max_group_objects: self.max_group_objects,
            max_group_bytes: self.max_group_bytes,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct MaintenanceReportV1 {
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

impl From<TableMaintenanceReport> for MaintenanceReportV1 {
    fn from(report: TableMaintenanceReport) -> Self {
        Self {
            groups_rewritten: report.groups_rewritten,
            input_objects: report.input_objects,
            input_bytes: report.input_bytes,
            output_objects: report.output_objects,
            output_bytes: report.output_bytes,
            snapshots_expired: report.snapshots_expired,
            manifests_rewritten: report.manifests_rewritten,
            objects_scheduled_for_deletion: report.objects_scheduled_for_deletion,
            cas_retries: report.cas_retries,
        }
    }
}

impl From<MaintenanceReportV1> for TableMaintenanceReport {
    fn from(report: MaintenanceReportV1) -> Self {
        Self {
            groups_rewritten: report.groups_rewritten,
            input_objects: report.input_objects,
            input_bytes: report.input_bytes,
            output_objects: report.output_objects,
            output_bytes: report.output_bytes,
            snapshots_expired: report.snapshots_expired,
            manifests_rewritten: report.manifests_rewritten,
            objects_scheduled_for_deletion: report.objects_scheduled_for_deletion,
            cas_retries: report.cas_retries,
            ..TableMaintenanceReport::default()
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct MaintenanceStatsV1 {
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

impl MaintenanceStatsV1 {
    pub fn try_from_stats(stats: TableMaintenanceStats) -> Option<Self> {
        let mut format = [0; FORMAT_NAME_CAPACITY];
        if let Some(value) = stats.format.as_deref() {
            let bytes = value.as_bytes();
            if bytes.len() >= FORMAT_NAME_CAPACITY || bytes.contains(&0) {
                return None;
            }
            format[..bytes.len()].copy_from_slice(bytes);
        }
        Some(Self {
            format,
            history_points: stats.history_points,
            current_content_objects: stats.current_content_objects,
            current_content_bytes: stats.current_content_bytes,
            retained_content_objects: stats.retained_content_objects,
            retained_content_bytes: stats.retained_content_bytes,
            current_data_objects: stats.current_data_objects,
            current_data_bytes: stats.current_data_bytes,
            retained_data_objects: stats.retained_data_objects,
            retained_data_bytes: stats.retained_data_bytes,
        })
    }

    pub fn into_stats(self, provider: String) -> TableMaintenanceStats {
        let length = self
            .format
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(FORMAT_NAME_CAPACITY);
        let format = (length != 0).then(|| {
            String::from_utf8_lossy(&self.format[..length]).into_owned()
        });
        TableMaintenanceStats {
            provider,
            format,
            history_points: self.history_points,
            current_content_objects: self.current_content_objects,
            current_content_bytes: self.current_content_bytes,
            retained_content_objects: self.retained_content_objects,
            retained_content_bytes: self.retained_content_bytes,
            current_data_objects: self.current_data_objects,
            current_data_bytes: self.current_data_bytes,
            retained_data_objects: self.retained_data_objects,
            retained_data_bytes: self.retained_data_bytes,
        }
    }
}

pub type AccessMethodOidCallback = unsafe extern "C-unwind" fn() -> pg_sys::Oid;
pub type ExecuteCallback = unsafe extern "C-unwind" fn(
    request: *const MaintenanceRequestV1,
    report: *mut MaintenanceReportV1,
);
pub type InspectCallback = unsafe extern "C-unwind" fn(
    relation: pg_sys::Relation,
    stats: *mut MaintenanceStatsV1,
);

#[repr(C)]
#[derive(Clone, Copy)]
pub struct MaintenanceProviderV1 {
    pub abi_version: u32,
    pub struct_size: u32,
    pub name: *const c_char,
    pub access_method_oid: AccessMethodOidCallback,
    pub execute: ExecuteCallback,
    pub inspect: InspectCallback,
}

#[repr(C)]
pub struct RuntimeApiV1 {
    pub abi_version: u32,
    pub struct_size: u32,
    pub register_provider: unsafe extern "C-unwind" fn(*const MaintenanceProviderV1) -> u32,
    pub has_providers: unsafe extern "C-unwind" fn() -> u8,
    pub provider_for_am:
        unsafe extern "C-unwind" fn(pg_sys::Oid) -> *const MaintenanceProviderV1,
    pub customscan_mode: unsafe extern "C-unwind" fn() -> u32,
    pub maintenance_config:
        unsafe extern "C-unwind" fn(*mut RuntimeMaintenanceConfigV1),
}

unsafe extern "C" {
    fn find_rendezvous_variable(name: *const c_char) -> *mut *mut c_void;
}

pub unsafe fn rendezvous_slot() -> *mut *mut c_void {
    unsafe { find_rendezvous_variable(RUNTIME_API_RENDEZVOUS.as_ptr()) }
}

static RUNTIME_API_CACHE: OnceLock<&'static RuntimeApiV1> = OnceLock::new();

pub fn runtime_api() -> Option<&'static RuntimeApiV1> {
    if let Some(api) = RUNTIME_API_CACHE.get() {
        return Some(*api);
    }
    let slot = unsafe { rendezvous_slot() };
    if slot.is_null() {
        return None;
    }
    let api = unsafe { *slot }.cast::<RuntimeApiV1>();
    let api = unsafe { api.as_ref() }?;
    let expected_size = u32::try_from(std::mem::size_of::<RuntimeApiV1>()).ok()?;
    let api = (api.abi_version == RUNTIME_API_VERSION
        && api.struct_size >= expected_size)
        .then_some(api)?;
    let _ = RUNTIME_API_CACHE.set(api);
    Some(api)
}

pub fn provider_name(provider: &MaintenanceProviderV1) -> Option<&CStr> {
    (!provider.name.is_null()).then(|| unsafe { CStr::from_ptr(provider.name) })
}
