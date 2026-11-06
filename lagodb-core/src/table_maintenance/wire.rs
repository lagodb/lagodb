//! Conversion between provider-facing maintenance types and the runtime's
//! exact-build wire records in [`crate::runtime_api`].
//!
//! The ABI layer defines only `#[repr(C)]` records; this domain layer owns the
//! mapping in both directions so the dependency runs one way.

use crate::handles::RelationHandle;
use crate::runtime_api::{
    FORMAT_NAME_CAPACITY, MaintenanceMode, MaintenanceReport, MaintenanceRequest,
    MaintenanceStats,
};

use super::{
    TableMaintenanceBudget, TableMaintenanceCommandTime, TableMaintenanceMode,
    TableMaintenanceOptions, TableMaintenanceReport, TableMaintenanceRequest,
    TableMaintenanceStats,
};

impl From<TableMaintenanceMode> for MaintenanceMode {
    fn from(mode: TableMaintenanceMode) -> Self {
        match mode {
            TableMaintenanceMode::Routine => Self::Routine,
            TableMaintenanceMode::Full => Self::Full,
        }
    }
}

impl From<MaintenanceMode> for TableMaintenanceMode {
    fn from(mode: MaintenanceMode) -> Self {
        match mode {
            MaintenanceMode::Routine => Self::Routine,
            MaintenanceMode::Full => Self::Full,
        }
    }
}

impl<'a> TableMaintenanceRequest<'a> {
    /// Encode this request for the provider's exact-build maintenance callback.
    pub(crate) fn to_wire(&self) -> MaintenanceRequest {
        MaintenanceRequest {
            relation: self.relation.as_raw(),
            mode: self.mode.into(),
            option_flags: u32::from(self.options.verbose)
                * MaintenanceRequest::OPTION_VERBOSE,
            max_input_objects: self.budget.max_input_objects,
            max_input_bytes: self.budget.max_input_bytes,
            max_group_objects: self.budget.max_group_objects,
            max_group_bytes: self.budget.max_group_bytes,
            command_time_ms: self.command_time.unix_epoch_ms(),
        }
    }

    /// Decode a runtime request whose `relation` field `relation` borrows.
    pub(crate) fn from_wire(
        relation: &'a RelationHandle<'a>,
        wire: &MaintenanceRequest,
    ) -> Self {
        Self {
            relation,
            mode: wire.mode.into(),
            options: TableMaintenanceOptions {
                verbose: wire.option_flags & MaintenanceRequest::OPTION_VERBOSE != 0,
            },
            budget: TableMaintenanceBudget {
                max_input_objects: wire.max_input_objects,
                max_input_bytes: wire.max_input_bytes,
                max_group_objects: wire.max_group_objects,
                max_group_bytes: wire.max_group_bytes,
            },
            command_time: TableMaintenanceCommandTime::from_unix_epoch_ms(
                wire.command_time_ms,
            ),
        }
    }
}

impl From<TableMaintenanceReport> for MaintenanceReport {
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

impl From<MaintenanceReport> for TableMaintenanceReport {
    fn from(report: MaintenanceReport) -> Self {
        // The runtime ABI carries only common counters. Start from Default so
        // the private provider-metrics collection remains valid and empty.
        let mut result = Self::default();
        result.groups_rewritten = report.groups_rewritten;
        result.input_objects = report.input_objects;
        result.input_bytes = report.input_bytes;
        result.output_objects = report.output_objects;
        result.output_bytes = report.output_bytes;
        result.snapshots_expired = report.snapshots_expired;
        result.manifests_rewritten = report.manifests_rewritten;
        result.objects_scheduled_for_deletion = report.objects_scheduled_for_deletion;
        result.cas_retries = report.cas_retries;
        result
    }
}

impl TableMaintenanceStats {
    /// Encode these stats, or `None` when the format name exceeds the ABI bound.
    pub(crate) fn to_wire(&self) -> Option<MaintenanceStats> {
        let mut format = [0; FORMAT_NAME_CAPACITY];
        if let Some(value) = self.format.as_deref() {
            let bytes = value.as_bytes();
            if bytes.len() >= FORMAT_NAME_CAPACITY || bytes.contains(&0) {
                return None;
            }
            format[..bytes.len()].copy_from_slice(bytes);
        }
        Some(MaintenanceStats {
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
        })
    }

    /// Decode stats reported by `provider`.
    pub(crate) fn from_wire(stats: &MaintenanceStats, provider: String) -> Self {
        let length = stats
            .format
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(FORMAT_NAME_CAPACITY);
        let format = (length != 0)
            .then(|| String::from_utf8_lossy(&stats.format[..length]).into_owned());
        Self {
            provider,
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
        }
    }
}
