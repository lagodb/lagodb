use std::collections::{HashMap, HashSet};

use iceberg_lite::scan::FileScanTask;
use iceberg_lite::spec::DataFile;
use iceberg_lite::table::Table;

use crate::error::{IcebergError, IcebergResult, IcebergVacuumError};

use super::types::{
    ManagedTableRoot, RewriteGroup, RewriteInput, VacuumPlan, VacuumPlanningMetrics,
    VacuumPolicy,
};

const UNDERSIZED_PERCENT: u64 = 75;
const OVERSIZED_PERCENT: u64 = 180;
const PERCENT_SCALE: u64 = 100;
const DELETE_RATIO_PERCENT: u64 = 30;
const MIN_INPUT_FILES: usize = 5;

pub(crate) struct VacuumPlanner;

impl VacuumPlanner {
    fn count(value: usize, description: &'static str) -> IcebergResult<u64> {
        u64::try_from(value).map_err(|_| IcebergError::Vacuum {
            source: IcebergVacuumError::ResourceLimit(format!(
                "{description} does not fit u64"
            )),
        })
    }

    pub(crate) fn plan(
        table: &Table,
        policy: VacuumPolicy,
        owned_table_root: &ManagedTableRoot,
    ) -> IcebergResult<VacuumPlan> {
        Self::validate_budget(policy)?;
        let properties = table.metadata().table_properties()?;
        if !properties.gc_enabled {
            return Err(IcebergError::Vacuum { source: IcebergVacuumError::GcDisabled });
        }
        Self::validate_properties(&properties)?;
        for property in [
            "write.data.path",
            "write.folder-storage.path",
            "write.metadata.path",
        ] {
            if table.metadata().properties().contains_key(property) {
                return Err(IcebergError::Vacuum {
                    source: IcebergVacuumError::InvalidPolicy(format!(
                        "managed Iceberg tables cannot override {property}"
                    )),
                });
            }
        }

        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok(VacuumPlan {
                policy,
                starting_snapshot_id: None,
                starting_sequence_number: 0,
                rewrite_groups: Vec::new(),
                metrics: VacuumPlanningMetrics::default(),
            });
        };

        if !policy.compact_data_files {
            return Ok(VacuumPlan {
                policy,
                starting_snapshot_id: Some(snapshot.snapshot_id()),
                starting_sequence_number: snapshot.sequence_number(),
                rewrite_groups: Vec::new(),
                metrics: VacuumPlanningMetrics::default(),
            });
        }

        let target_bytes = u64::try_from(properties.write_target_file_size_bytes)
            .map_err(|_| IcebergError::Vacuum { source: IcebergVacuumError::ResourceLimit(
                "write.target-file-size-bytes does not fit u64".to_owned(),
            )})?;
        let tasks = table.scan().select_empty().build()?.plan_files()?;
        let mut metrics = VacuumPlanningMetrics {
            scanned_data_files: Self::count(tasks.len(), "scanned data-file count")?,
            ..VacuumPlanningMetrics::default()
        };
        let mut scanned_delete_paths = HashSet::new();
        for task in &tasks {
            owned_table_root.ensure_path(task.data_file_path())?;
            for delete in &task.deletes {
                owned_table_root.ensure_path(&delete.file_path)?;
                scanned_delete_paths.insert(delete.file_path.as_str());
            }
        }
        metrics.scanned_delete_files =
            Self::count(scanned_delete_paths.len(), "scanned delete-file count")?;
        let mut tasks_by_path = HashMap::with_capacity(tasks.len());
        for task in tasks {
            let path = task.data_file_path().to_owned();
            if tasks_by_path.insert(path, task).is_some() {
                return Err(IcebergError::InvariantViolated(
                    "Iceberg scan planned duplicate live data-file paths",
                ));
            }
        }
        let (live_files, scanned_manifests) = Self::live_data_files(table)?;
        metrics.scanned_manifests = scanned_manifests;
        if target_bytes == 0 {
            return Err(IcebergError::Vacuum {
                source: IcebergVacuumError::ResourceLimit(
                    "write.target-file-size-bytes must be greater than zero".to_owned(),
                ),
            });
        }
        let mut candidates = Vec::new();
        for file in live_files {
            let task = tasks_by_path
                .remove(file.file_path())
                .ok_or_else(|| IcebergError::InvariantViolated(
                    "live data file has no corresponding scan task",
                ))?;
            let delete_heavy = Self::is_delete_heavy(&file, &task.deletes)?;
            let size = file.file_size_in_bytes();
            let undersized = u128::from(size) * u128::from(PERCENT_SCALE)
                < u128::from(target_bytes) * u128::from(UNDERSIZED_PERCENT);
            let oversized = u128::from(size) * u128::from(PERCENT_SCALE)
                > u128::from(target_bytes) * u128::from(OVERSIZED_PERCENT);
            if undersized || oversized || delete_heavy {
                candidates.push((file, task, delete_heavy));
            }
        }

        metrics.eligible_files = Self::count(candidates.len(), "eligible file count")?;
        metrics.eligible_bytes = candidates.iter().try_fold(
            0_u64,
            |total, (file, _, _)| total.checked_add(file.file_size_in_bytes()),
        ).ok_or_else(|| IcebergError::Vacuum {
            source: IcebergVacuumError::ResourceLimit(
                "eligible input byte count overflow".to_owned(),
            ),
        })?;

        let mut groups = Self::bin_pack(
            candidates,
            target_bytes,
            policy,
            table.metadata().format_version()
                == iceberg_lite::spec::FormatVersion::V3,
        )?;
        groups.sort_by(|left, right| {
            right.delete_heavy
                .cmp(&left.delete_heavy)
                .then_with(|| {
                    right
                        .expected_file_reduction
                        .cmp(&left.expected_file_reduction)
                })
                .then_with(|| left.input_bytes.cmp(&right.input_bytes))
                .then_with(|| {
                    left.inputs[0]
                        .file
                        .file_path()
                        .cmp(right.inputs[0].file.file_path())
                })
        });
        metrics.eligible_groups = Self::count(groups.len(), "eligible group count")?;
        Self::apply_command_budget(&mut groups, policy)?;
        metrics.selected_groups = Self::count(groups.len(), "selected group count")?;
        metrics.selected_files = groups.iter().try_fold(0_u64, |total, group| {
            total.checked_add(Self::count(group.inputs.len(), "selected file count")?)
                .ok_or_else(|| IcebergError::Vacuum {
                    source: IcebergVacuumError::ResourceLimit(
                        "selected file count overflow".to_owned(),
                    ),
                })
        })?;
        metrics.selected_bytes = groups.iter().try_fold(0_u64, |total, group| {
            total.checked_add(group.input_bytes).ok_or_else(|| IcebergError::Vacuum {
                source: IcebergVacuumError::ResourceLimit(
                    "selected input byte count overflow".to_owned(),
                ),
            })
        })?;

        Ok(VacuumPlan {
            policy,
            starting_snapshot_id: Some(snapshot.snapshot_id()),
            starting_sequence_number: snapshot.sequence_number(),
            rewrite_groups: groups,
            metrics,
        })
    }

    fn validate_properties(
        properties: &iceberg_lite::spec::TableProperties,
    ) -> IcebergResult<()> {
        let invalid = |message: &'static str| IcebergError::Vacuum {
            source: IcebergVacuumError::InvalidPolicy(message.to_owned()),
        };
        if properties.write_target_file_size_bytes == 0 {
            return Err(invalid("write.target-file-size-bytes must be greater than zero"));
        }
        if properties.max_snapshot_age_ms < 0 {
            return Err(invalid("history.expire.max-snapshot-age-ms must not be negative"));
        }
        if properties.min_snapshots_to_keep == 0 {
            return Err(invalid("history.expire.min-snapshots-to-keep must be greater than zero"));
        }
        if properties.max_ref_age_ms < 0 {
            return Err(invalid("history.expire.max-ref-age-ms must not be negative"));
        }
        if properties.metadata_previous_versions_max == 0 {
            return Err(invalid("write.metadata.previous-versions-max must be greater than zero"));
        }
        if properties.manifest_min_count_to_merge == 0 {
            return Err(invalid("commit.manifest.min-count-to-merge must be greater than zero"));
        }
        if properties.manifest_target_size_bytes == 0 {
            return Err(invalid("commit.manifest.target-size-bytes must be greater than zero"));
        }
        Ok(())
    }

    fn validate_budget(policy: VacuumPolicy) -> IcebergResult<()> {
        if policy.budget.max_group_objects == 0
            || policy.budget.max_group_bytes == 0
            || policy.budget.max_input_objects == 0
            || policy.budget.max_input_bytes == 0
        {
            return Err(IcebergError::Vacuum {
                source: IcebergVacuumError::InvalidPolicy(
                    "VACUUM object and byte budgets must be greater than zero"
                        .to_owned(),
                ),
            });
        }
        Ok(())
    }

    fn is_delete_heavy(
        file: &DataFile,
        deletes: &[iceberg_lite::scan::FileScanTaskDeleteFile],
    ) -> IcebergResult<bool> {
        if file.record_count() == 0 {
            return Ok(false);
        }
        let deleted_rows = deletes
            .iter()
            .filter(|delete| {
                delete.referenced_data_file_path() == Some(file.file_path())
            })
            .try_fold(0_u64, |total, delete| {
                total.checked_add(delete.record_count).ok_or_else(|| {
                    IcebergError::Vacuum {
                        source: IcebergVacuumError::ResourceLimit(
                            "file-scoped deleted-row count overflow".to_owned(),
                        ),
                    }
                })
            })?;
        Ok(u128::from(deleted_rows) * u128::from(PERCENT_SCALE)
            >= u128::from(file.record_count()) * u128::from(DELETE_RATIO_PERCENT))
    }

    fn live_data_files(table: &Table) -> IcebergResult<(Vec<DataFile>, u64)> {
        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok((Vec::new(), 0));
        };
        let manifest_list =
            snapshot.load_manifest_list(table.file_io(), &table.metadata_ref())?;
        let mut paths = HashSet::new();
        let mut files = Vec::new();
        let scanned_manifests = Self::count(
            manifest_list.entries().len(),
            "scanned manifest count",
        )?;
        for manifest_file in manifest_list.entries() {
            pgrx::pg_sys::check_for_interrupts!();
            if manifest_file.content != iceberg_lite::spec::ManifestContentType::Data {
                continue;
            }
            let manifest = manifest_file.load_manifest(table.file_io())?;
            for entry in manifest.entries() {
                if entry.is_alive() && paths.insert(entry.file_path().to_owned()) {
                    files.push(entry.data_file().clone());
                }
            }
        }
        Ok((files, scanned_manifests))
    }

    pub(crate) fn materialized_deletion_vectors(
        table: &Table,
        rewritten_paths: &HashSet<&str>,
    ) -> IcebergResult<Vec<DataFile>> {
        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok(Vec::new());
        };
        let manifest_list =
            snapshot.load_manifest_list(table.file_io(), &table.metadata_ref())?;
        let mut vectors = Vec::new();
        for manifest_file in manifest_list.entries() {
            pgrx::pg_sys::check_for_interrupts!();
            if manifest_file.content != iceberg_lite::spec::ManifestContentType::Deletes {
                continue;
            }
            let manifest = manifest_file.load_manifest(table.file_io())?;
            for entry in manifest.entries() {
                let file = entry.data_file();
                if entry.is_alive()
                    && file.is_deletion_vector()
                    && file
                        .referenced_data_file_path()
                        .is_some_and(|path| rewritten_paths.contains(path))
                {
                    vectors.push(file.clone());
                }
            }
        }
        Ok(vectors)
    }

    fn bin_pack(
        mut candidates: Vec<(DataFile, FileScanTask, bool)>,
        target_bytes: u64,
        policy: VacuumPolicy,
        preserve_v3_lineage_groups: bool,
    ) -> IcebergResult<Vec<RewriteGroup>> {
        candidates.sort_by_key(|(file, _, _)| {
            std::cmp::Reverse(file.file_size_in_bytes())
        });
        let mut partitions: HashMap<
            (i32, iceberg_lite::spec::Struct, bool),
            Vec<(DataFile, FileScanTask, bool)>,
        > = HashMap::new();
        for candidate in candidates {
            let file = &candidate.0;
            // A v3 output file cannot both preserve already-assigned row IDs
            // and reserve a fresh inherited range only for the unassigned
            // subset. Keep those lineages in separate rewrite groups.
            let has_assigned_row_ids =
                preserve_v3_lineage_groups && candidate.1.first_row_id.is_some();
            partitions
                .entry((
                    file.partition_spec_id,
                    file.partition().clone(),
                    has_assigned_row_ids,
                ))
                .or_default()
                .push(candidate);
        }

        let mut accepted = Vec::new();
        let max_group_objects = usize::try_from(policy.budget.max_group_objects)
            .map_err(|_| {
                IcebergError::Vacuum { source: IcebergVacuumError::ResourceLimit(
                    "vacuum_max_group_objects does not fit backend usize".to_owned(),
                )}
            })?;
        for (_, files) in partitions {
            let mut bins: Vec<RewriteGroup> = Vec::new();
            for (file, task, delete_heavy) in files {
                let file_bytes = file.file_size_in_bytes();
                if file_bytes > policy.budget.max_group_bytes {
                    return Err(IcebergError::Vacuum { source: IcebergVacuumError::ResourceLimit(format!(
                        "eligible input {} is {} bytes, above vacuum_max_group_mb",
                        file.file_path(), file_bytes
                    ))});
                }
                let destination = bins.iter_mut().find(|group| {
                    group.inputs.len() < max_group_objects
                        && group
                            .input_bytes
                            .checked_add(file_bytes)
                            .is_some_and(|bytes| bytes <= policy.budget.max_group_bytes)
                });
                if let Some(group) = destination {
                    group.input_bytes = group.input_bytes.checked_add(file_bytes).ok_or_else(|| {
                        IcebergError::Vacuum { source: IcebergVacuumError::ResourceLimit(
                            "rewrite group byte count overflow".to_owned(),
                        )}
                    })?;
                    group.delete_heavy |= delete_heavy;
                    group.inputs.push(RewriteInput { file, task });
                } else {
                    bins.push(RewriteGroup {
                        inputs: vec![RewriteInput { file, task }],
                        input_bytes: file_bytes,
                        delete_heavy,
                        expected_file_reduction: 0,
                    });
                }
            }

            for mut group in bins {
                let expected_outputs = group.input_bytes.div_ceil(target_bytes).max(1);
                let input_count = u64::try_from(group.inputs.len()).map_err(|_| {
                    IcebergError::Vacuum {
                        source: IcebergVacuumError::ResourceLimit(
                            "rewrite group input count does not fit u64".to_owned(),
                        ),
                    }
                })?;
                let reduces_files = expected_outputs < input_count;
                let repairs_oversize = group.inputs.iter().any(|input| {
                    u128::from(input.file.file_size_in_bytes())
                        * u128::from(PERCENT_SCALE)
                        > u128::from(target_bytes) * u128::from(OVERSIZED_PERCENT)
                });
                group.expected_file_reduction = input_count
                    .checked_sub(expected_outputs)
                    .unwrap_or(0);
                if (group.inputs.len() >= MIN_INPUT_FILES
                    || repairs_oversize
                    || group.delete_heavy)
                    && (reduces_files || repairs_oversize || group.delete_heavy)
                {
                    accepted.push(group);
                }
            }
        }
        Ok(accepted)
    }

    fn apply_command_budget(
        groups: &mut Vec<RewriteGroup>,
        policy: VacuumPolicy,
    ) -> IcebergResult<()> {
        if policy.mode == pg_lakebase_core::table_maintenance::TableMaintenanceMode::Full {
            return Ok(());
        }
        let mut selected_objects = 0_u64;
        let mut selected_bytes = 0_u64;
        let mut keep = 0_usize;
        for group in groups.iter() {
            let objects = u64::try_from(group.inputs.len()).map_err(|_| {
                IcebergError::Vacuum {
                    source: IcebergVacuumError::ResourceLimit(
                        "rewrite group input count does not fit u64".to_owned(),
                    ),
                }
            })?;
            let next_objects = selected_objects.checked_add(objects);
            let next_bytes = selected_bytes.checked_add(group.input_bytes);
            let would_exceed = next_objects
                .is_none_or(|value| value > policy.budget.max_input_objects)
                || next_bytes.is_none_or(|value| value > policy.budget.max_input_bytes);
            if would_exceed && keep > 0 {
                break;
            }
            selected_objects = next_objects.ok_or_else(|| IcebergError::Vacuum {
                source: IcebergVacuumError::ResourceLimit(
                    "selected input object count overflow".to_owned(),
                ),
            })?;
            selected_bytes = next_bytes.ok_or_else(|| IcebergError::Vacuum {
                source: IcebergVacuumError::ResourceLimit(
                    "selected input byte count overflow".to_owned(),
                ),
            })?;
            keep += 1;
        }
        groups.truncate(keep);
        Ok(())
    }
}
