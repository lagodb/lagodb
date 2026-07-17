use iceberg_lite::scan::FileScanTask;
use iceberg_lite::overlay::DeleteFileIdentity;
use iceberg_lite::spec::DataFile;
use pg_lakebase_core::table_maintenance::{
    TableMaintenanceBudget, TableMaintenanceCommandTime, TableMaintenanceMode,
    TableMaintenanceMetric, TableMaintenanceReport,
};
use std::ffi::CStr;

use crate::error::{IcebergError, IcebergResult, IcebergVacuumError};

#[derive(Clone, Debug)]
pub(crate) struct ManagedTableRoot {
    root: String,
    child_prefix: String,
}

impl ManagedTableRoot {
    pub(crate) fn new(expected: String, actual: &str) -> IcebergResult<Self> {
        let root = expected.trim_end_matches('/').to_owned();
        if root.is_empty()
            || root.split('/').any(|segment| segment == "..")
            || actual.trim_end_matches('/') != root
        {
            return Err(IcebergError::Vacuum {
                source: IcebergVacuumError::UnsafePath(actual.to_owned()),
            });
        }
        let child_prefix = format!("{root}/");
        Ok(Self { root, child_prefix })
    }

    pub(crate) fn ensure_table_location(&self, location: &str) -> IcebergResult<()> {
        if location.trim_end_matches('/') != self.root {
            return Err(IcebergError::Vacuum {
                source: IcebergVacuumError::UnsafePath(location.to_owned()),
            });
        }
        Ok(())
    }

    pub(crate) fn ensure_path(&self, path: &str) -> IcebergResult<()> {
        if path.split('/').any(|segment| segment == "..")
            || !path.starts_with(&self.child_prefix)
        {
            return Err(IcebergError::Vacuum {
                source: IcebergVacuumError::UnsafePath(path.to_owned()),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct VacuumPolicy {
    pub(crate) mode: TableMaintenanceMode,
    pub(crate) command_time: TableMaintenanceCommandTime,
    pub(crate) budget: TableMaintenanceBudget,
    pub(crate) compact_data_files: bool,
    pub(crate) orphan_retention_ms: i64,
}

impl VacuumPolicy {
    pub(crate) fn new(
        mode: TableMaintenanceMode,
        command_time: TableMaintenanceCommandTime,
        budget: TableMaintenanceBudget,
    ) -> Self {
        Self {
            mode,
            command_time,
            budget: budget.without_soft_limit(mode),
            compact_data_files: mode == TableMaintenanceMode::Full
                || crate::gucs::vacuum_compact_data_files(),
            orphan_retention_ms: crate::gucs::vacuum_orphan_retention_ms(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RewriteInput {
    pub(crate) file: DataFile,
    pub(crate) task: FileScanTask,
}

#[derive(Clone, Debug)]
pub(crate) struct RewriteGroup {
    pub(crate) inputs: Vec<RewriteInput>,
    pub(crate) input_bytes: u64,
    pub(crate) delete_heavy: bool,
    pub(crate) expected_file_reduction: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct VacuumPlan {
    pub(crate) policy: VacuumPolicy,
    pub(crate) starting_snapshot_id: Option<i64>,
    pub(crate) starting_sequence_number: i64,
    pub(crate) rewrite_groups: Vec<RewriteGroup>,
    pub(crate) materialized_delete_identities: Vec<DeleteFileIdentity>,
    pub(crate) metrics: VacuumPlanningMetrics,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct VacuumPlanningMetrics {
    pub(crate) scanned_manifests: u64,
    pub(crate) scanned_data_files: u64,
    pub(crate) scanned_delete_files: u64,
    pub(crate) eligible_groups: u64,
    pub(crate) eligible_files: u64,
    pub(crate) eligible_bytes: u64,
    pub(crate) selected_groups: u64,
    pub(crate) selected_files: u64,
    pub(crate) selected_bytes: u64,
}

pub(crate) fn record_metric(
    report: &mut TableMaintenanceReport,
    name: &'static CStr,
    value: u64,
) -> IcebergResult<()> {
    report
        .record_provider_metric(TableMaintenanceMetric { name, value })
        .map_err(|_| {
            IcebergError::InvariantViolated(
                "Iceberg VACUUM exceeded its statically bounded provider metrics",
            )
        })
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedRewrite {
    pub(crate) starting_snapshot_id: i64,
    pub(crate) starting_sequence_number: i64,
    pub(crate) input_files: Vec<DataFile>,
    pub(crate) output_files: Vec<DataFile>,
    pub(crate) materialized_delete_identities: Vec<DeleteFileIdentity>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PreparedExpiration {
    pub(crate) as_of_ms: i64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PreparedManifestRewrite {
    pub(crate) min_count_to_merge: usize,
    pub(crate) target_size_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PreparedOrphanPolicy {
    pub(crate) older_than_ms: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedVacuum {
    pub(crate) owned_table_root: ManagedTableRoot,
    pub(crate) policy: VacuumPolicy,
    pub(crate) rewrite: Option<PreparedRewrite>,
    pub(crate) expiration: PreparedExpiration,
    pub(crate) manifest_rewrite: Option<PreparedManifestRewrite>,
    pub(crate) orphan_cleanup: Option<PreparedOrphanPolicy>,
    pub(crate) verbose: bool,
    pub(crate) report: TableMaintenanceReport,
}

impl PreparedVacuum {
    pub(crate) fn report_success(&self, report: &TableMaintenanceReport) {
        if self.verbose {
            pg_lakebase_core::diag::report_notice(format_args!(
                "Iceberg VACUUM: groups={}, input_files={}, input_bytes={}, output_files={}, output_bytes={}, expired_snapshots={}, rewritten_manifests={}, queued_deletions={}, cas_retries={}",
                report.groups_rewritten,
                report.input_objects,
                report.input_bytes,
                report.output_objects,
                report.output_bytes,
                report.snapshots_expired,
                report.manifests_rewritten,
                report.objects_scheduled_for_deletion,
                report.cas_retries,
            ));
            if !report.provider_metrics().is_empty() {
                use std::fmt::Write;

                let mut metrics = String::new();
                for (index, metric) in report.provider_metrics().iter().enumerate() {
                    if index != 0 {
                        metrics.push_str(", ");
                    }
                    let _ = write!(
                        metrics,
                        "{}={}",
                        metric.name.to_string_lossy(),
                        metric.value
                    );
                }
                pg_lakebase_core::diag::report_notice(format_args!(
                    "Iceberg VACUUM stages: {metrics}"
                ));
            }
        }
    }
}
