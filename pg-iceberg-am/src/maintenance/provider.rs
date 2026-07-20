use std::ffi::CStr;

use iceberg_lite::spec::TableMetadata;
use iceberg_lite::table::Table;
use parquet::file::properties::WriterProperties;
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::table_maintenance::{
    LakebaseTableMaintenanceProvider, TableMaintenanceError, TableMaintenanceMode,
    TableMaintenanceReport, TableMaintenanceRequest, TableMaintenanceStats,
};
use pgrx::pg_sys;

use crate::catalog::IcebergAccessMethod;
use crate::catalog::bridge::IcebergTableId;
use crate::catalog::metadata_table::{
    IcebergMetadata, MaintenanceCandidate, MaintenanceCompletionToken,
};
use crate::catalog::metadata_tracker::TxMetadata;
use crate::constants::ICEBERG_AM_NAME;
use crate::error::{IcebergError, IcebergResult, IcebergVacuumError};
use crate::options::IcebergTableOptions;
use crate::storage::StorageContext;

use super::planner::VacuumPlanner;
use super::types::{
    ManagedTableRoot, PreparedExpiration, PreparedManifestRewrite,
    PreparedOrphanPolicy, PreparedRewrite, PreparedVacuum, VacuumPolicy,
    record_metric,
};
use super::writer::RewriteGroupWriter;

pub(crate) struct IcebergTableMaintenanceProvider;

pub(crate) enum MaintenanceExecution {
    Executed(TableMaintenanceReport),
    StaleCandidate,
}

impl IcebergTableMaintenanceProvider {
    fn checked_count(value: usize, description: &'static str) -> IcebergResult<u64> {
        u64::try_from(value).map_err(|_| IcebergError::Vacuum {
            source: IcebergVacuumError::ResourceLimit(format!(
                "{description} does not fit u64"
            )),
        })
    }

    fn execute_iceberg(
        request: TableMaintenanceRequest<'_>,
        expected_candidate: Option<&MaintenanceCandidate>,
    ) -> IcebergResult<MaintenanceExecution> {
        if request.relation.toast_relation_oid().is_some() {
            return Err(IcebergError::Vacuum {
                source: IcebergVacuumError::UnexpectedToastRelation {
                    relid: request.relation.oid(),
                },
            });
        }

        let storage = StorageContext::for_tablespace_with_wal(
            request.relation.locator().spc_oid,
            request.relation.needs_wal(),
        )?;
        let expected_table_location =
            crate::catalog::table_lifecycle::compute_table_location(
                request.relation,
                storage.base_path(),
                storage.is_distributed(),
            );
        let file_io = storage.into_file_io();
        let tracker = TxMetadata::current();
        let loaded = tracker.begin_table_modify(request.relation.oid(), &file_io)?;
        let completion_token = MaintenanceCompletionToken {
            metadata_location: loaded.location.clone(),
            maintenance_due_at: loaded.maintenance_due_at,
        };
        if expected_candidate
            .is_some_and(|candidate| !candidate.matches(&completion_token))
        {
            return Ok(MaintenanceExecution::StaleCandidate);
        }
        let owned_table_root = ManagedTableRoot::new(
            expected_table_location,
            loaded.metadata.location(),
        )?;
        owned_table_root.ensure_path(&loaded.location)?;
        let table = Table::builder()
            .metadata_location(loaded.location)
            .metadata(loaded.metadata)
            .identifier(
                IcebergTableId::for_relation(request.relation.oid())
                    .into_table_ident(),
            )
            .file_io(file_io.clone())
            .build()?;

        let policy =
            VacuumPolicy::new(request.mode, request.command_time, request.budget);
        let planning_started = std::time::Instant::now();
        let plan = VacuumPlanner::plan(&table, policy, &owned_table_root)?;
        let planning_ms = u64::try_from(planning_started.elapsed().as_millis())
            .map_err(|_| IcebergError::Vacuum {
                source: IcebergVacuumError::ResourceLimit(
                    "VACUUM planning duration does not fit u64 milliseconds"
                        .to_owned(),
                ),
            })?;
        let policy = plan.policy;
        let starting_snapshot_id = plan.starting_snapshot_id;
        let starting_sequence_number = plan.starting_sequence_number;
        let materialized_delete_identities = plan.materialized_delete_identities;
        let planning_metrics = plan.metrics;
        let writer_properties = WriterProperties::builder()
            .set_compression(
                IcebergTableOptions::for_relation(request.relation)?
                    .parquet_compression(),
            )
            .build();
        let mut report = TableMaintenanceReport::default();
        for (name, value) in [
            (c"scanned_manifests", planning_metrics.scanned_manifests),
            (c"scanned_data_files", planning_metrics.scanned_data_files),
            (
                c"scanned_delete_files",
                planning_metrics.scanned_delete_files,
            ),
            (c"eligible_groups", planning_metrics.eligible_groups),
            (c"eligible_files", planning_metrics.eligible_files),
            (c"eligible_bytes", planning_metrics.eligible_bytes),
            (c"selected_groups", planning_metrics.selected_groups),
            (c"selected_files", planning_metrics.selected_files),
            (c"selected_bytes", planning_metrics.selected_bytes),
        ] {
            record_metric(&mut report, name, value)?;
        }
        record_metric(&mut report, c"planning_ms", planning_ms)?;
        let mut input_files = Vec::new();
        let mut output_files = Vec::new();
        let mut rewritten_rows = 0_u64;
        let rewrite_started = std::time::Instant::now();
        if policy.compact_data_files {
            for group in plan.rewrite_groups {
                let outputs =
                    RewriteGroupWriter::rewrite(&table, &group, &writer_properties)?;
                report.groups_rewritten = report
                    .groups_rewritten
                    .checked_add(1)
                    .ok_or_else(|| IcebergError::Vacuum {
                        source: IcebergVacuumError::ResourceLimit(
                            "rewrite group count overflow".to_owned(),
                        ),
                    })?;
                report.input_objects = report
                    .input_objects
                    .checked_add(u64::try_from(group.inputs.len()).map_err(|_| {
                        IcebergError::Vacuum {
                            source: IcebergVacuumError::ResourceLimit(
                                "input file count does not fit u64".to_owned(),
                            ),
                        }
                    })?)
                    .ok_or_else(|| IcebergError::Vacuum {
                        source: IcebergVacuumError::ResourceLimit(
                            "input file count overflow".to_owned(),
                        ),
                    })?;
                report.input_bytes = report
                    .input_bytes
                    .checked_add(group.input_bytes)
                    .ok_or_else(|| IcebergError::Vacuum {
                        source: IcebergVacuumError::ResourceLimit(
                            "input byte count overflow".to_owned(),
                        ),
                    })?;
                report.output_objects = report
                    .output_objects
                    .checked_add(u64::try_from(outputs.len()).map_err(|_| {
                        IcebergError::Vacuum {
                            source: IcebergVacuumError::ResourceLimit(
                                "output file count does not fit u64".to_owned(),
                            ),
                        }
                    })?)
                    .ok_or_else(|| IcebergError::Vacuum {
                        source: IcebergVacuumError::ResourceLimit(
                            "output file count overflow".to_owned(),
                        ),
                    })?;
                let output_bytes = outputs
                    .iter()
                    .try_fold(0_u64, |bytes, file| {
                        bytes.checked_add(file.file_size_in_bytes())
                    })
                    .ok_or_else(|| IcebergError::Vacuum {
                        source: IcebergVacuumError::ResourceLimit(
                            "output byte count overflow".to_owned(),
                        ),
                    })?;
                let output_rows = outputs
                    .iter()
                    .try_fold(0_u64, |rows, file| {
                        rows.checked_add(file.record_count())
                    })
                    .ok_or_else(|| IcebergError::Vacuum {
                        source: IcebergVacuumError::ResourceLimit(
                            "rewritten row count overflow".to_owned(),
                        ),
                    })?;
                rewritten_rows =
                    rewritten_rows.checked_add(output_rows).ok_or_else(|| {
                        IcebergError::Vacuum {
                            source: IcebergVacuumError::ResourceLimit(
                                "rewritten row count overflow".to_owned(),
                            ),
                        }
                    })?;
                report.output_bytes = report
                    .output_bytes
                    .checked_add(output_bytes)
                    .ok_or_else(|| IcebergError::Vacuum {
                    source: IcebergVacuumError::ResourceLimit(
                        "output byte count overflow".to_owned(),
                    ),
                })?;
                input_files.extend(group.inputs.into_iter().map(|input| input.file));
                output_files.extend(outputs);
            }
        }
        record_metric(&mut report, c"rewritten_rows", rewritten_rows)?;
        record_metric(
            &mut report,
            c"rewrite_ms",
            u64::try_from(rewrite_started.elapsed().as_millis()).map_err(|_| {
                IcebergError::Vacuum {
                    source: IcebergVacuumError::ResourceLimit(
                        "VACUUM rewrite duration does not fit u64 milliseconds"
                            .to_owned(),
                    ),
                }
            })?,
        )?;

        let rewrite = match starting_snapshot_id {
            Some(starting_snapshot_id) if !input_files.is_empty() => {
                Some(PreparedRewrite {
                    starting_snapshot_id,
                    starting_sequence_number,
                    input_files,
                    output_files,
                    materialized_delete_identities,
                })
            }
            _ => None,
        };
        let orphan_cleanup = if request.mode == TableMaintenanceMode::Full {
            Some(PreparedOrphanPolicy {
                older_than_ms: policy
                    .command_time
                    .unix_epoch_ms()
                    .checked_sub(policy.orphan_retention_ms)
                    .ok_or_else(|| IcebergError::Vacuum {
                        source: IcebergVacuumError::ResourceLimit(
                            "orphan retention cutoff underflow".to_owned(),
                        ),
                    })?,
            })
        } else {
            None
        };
        if let Some(orphan_cleanup) = orphan_cleanup {
            record_metric(
                &mut report,
                c"orphan_cutoff_ms",
                u64::try_from(orphan_cleanup.older_than_ms).map_err(|_| {
                    IcebergError::Vacuum {
                        source: IcebergVacuumError::InvalidPolicy(
                            "orphan cutoff predates the Unix epoch".to_owned(),
                        ),
                    }
                })?,
            )?;
        }
        let manifest_rewrite = if request.mode == TableMaintenanceMode::Full {
            let properties = table.metadata().table_properties()?;
            Some(PreparedManifestRewrite {
                min_count_to_merge: properties.manifest_min_count_to_merge,
                target_size_bytes: properties.manifest_target_size_bytes,
            })
        } else {
            None
        };
        tracker.stage_vacuum(
            request.relation.oid(),
            PreparedVacuum {
                completion_token,
                owned_table_root,
                rewrite,
                expiration: PreparedExpiration {
                    as_of_ms: policy.command_time.unix_epoch_ms(),
                },
                manifest_rewrite,
                orphan_cleanup,
                verbose: request.options.verbose,
                report: report.clone(),
            },
            &file_io,
        )?;
        Ok(MaintenanceExecution::Executed(report))
    }

    pub(crate) fn execute_scheduled(
        request: TableMaintenanceRequest<'_>,
        candidate: &MaintenanceCandidate,
    ) -> IcebergResult<MaintenanceExecution> {
        Self::execute_iceberg(request, Some(candidate))
    }

    fn inspect_iceberg(
        relation: &RelationHandle<'_>,
    ) -> IcebergResult<TableMaintenanceStats> {
        let storage = StorageContext::for_tablespace(relation.locator().spc_oid)?;
        let location = IcebergMetadata::get(relation.oid())?
            .metadata_location
            .ok_or(IcebergError::MetadataLocationNull)?;
        let metadata = TableMetadata::read_from(storage.file_io(), &location)?;
        let table = Table::builder()
            .metadata_location(location)
            .metadata(metadata)
            .identifier(
                IcebergTableId::for_relation(relation.oid()).into_table_ident(),
            )
            .file_io(storage.into_file_io())
            .build()?;
        let tasks = table.scan().select_empty().build()?.plan_files()?;
        let mut delete_paths = std::collections::HashSet::new();
        let mut current_content_bytes = 0_u64;
        let mut current_data_bytes = 0_u64;
        for task in &tasks {
            current_data_bytes = current_data_bytes
                .checked_add(task.file_size_in_bytes)
                .ok_or_else(|| IcebergError::Vacuum {
                    source: IcebergVacuumError::ResourceLimit(
                        "current data byte count overflow".to_owned(),
                    ),
                })?;
            for delete in &task.deletes {
                if delete_paths.insert(delete.file_path.as_str()) {
                    current_content_bytes = current_content_bytes
                        .checked_add(delete.file_size_in_bytes)
                        .ok_or_else(|| IcebergError::Vacuum {
                            source: IcebergVacuumError::ResourceLimit(
                                "current content byte count overflow".to_owned(),
                            ),
                        })?;
                }
            }
        }
        current_content_bytes = current_content_bytes
            .checked_add(current_data_bytes)
            .ok_or_else(|| IcebergError::Vacuum {
                source: IcebergVacuumError::ResourceLimit(
                    "current content byte count overflow".to_owned(),
                ),
            })?;
        let mut retained = std::collections::HashMap::new();
        let mut visited_manifest_lists = std::collections::HashSet::new();
        let mut visited_manifests = std::collections::HashSet::new();
        for snapshot in table.metadata().snapshots() {
            if !visited_manifest_lists.insert(snapshot.manifest_list().to_owned()) {
                continue;
            }
            let manifests = snapshot
                .load_manifest_list(table.file_io(), &table.metadata_ref())?;
            for manifest_file in manifests.entries() {
                if !visited_manifests.insert(manifest_file.manifest_path.clone()) {
                    continue;
                }
                let manifest = manifest_file.load_manifest(table.file_io())?;
                for entry in manifest.entries() {
                    if entry.is_alive() {
                        retained.entry(entry.file_path().to_owned()).or_insert((
                            entry.data_file().content_type()
                                == iceberg_lite::spec::DataContentType::Data,
                            entry.file_size_in_bytes(),
                        ));
                    }
                }
            }
        }
        let retained_content_bytes = retained
            .values()
            .try_fold(0_u64, |total, (_, bytes)| total.checked_add(*bytes))
            .ok_or_else(|| IcebergError::Vacuum {
                source: IcebergVacuumError::ResourceLimit(
                    "retained content byte count overflow".to_owned(),
                ),
            })?;
        let retained_data_bytes = retained
            .values()
            .filter(|(data, _)| *data)
            .try_fold(0_u64, |total, (_, bytes)| total.checked_add(*bytes))
            .ok_or_else(|| IcebergError::Vacuum {
                source: IcebergVacuumError::ResourceLimit(
                    "retained data byte count overflow".to_owned(),
                ),
            })?;
        Ok(TableMaintenanceStats {
            provider: String::new(),
            format: Some(table.metadata().format_version().to_string()),
            history_points: Self::checked_count(
                table.metadata().snapshots().count(),
                "history-point count",
            )?,
            current_content_objects: Self::checked_count(
                tasks.len().checked_add(delete_paths.len()).ok_or_else(|| {
                    IcebergError::Vacuum {
                        source: IcebergVacuumError::ResourceLimit(
                            "current content object count overflow".to_owned(),
                        ),
                    }
                })?,
                "current content object count",
            )?,
            current_content_bytes,
            retained_content_objects: Self::checked_count(
                retained.len(),
                "retained content object count",
            )?,
            retained_content_bytes,
            current_data_objects: Self::checked_count(
                tasks.len(),
                "current data object count",
            )?,
            current_data_bytes,
            retained_data_objects: Self::checked_count(
                retained.values().filter(|(data, _)| *data).count(),
                "retained data object count",
            )?,
            retained_data_bytes,
        })
    }
}

impl LakebaseTableMaintenanceProvider for IcebergTableMaintenanceProvider {
    const NAME: &'static CStr = c"iceberg";
    const ACCESS_METHOD_NAME: &'static CStr = ICEBERG_AM_NAME;
    const SUPPORTS_ANALYZE: bool = true;

    fn access_method_oid() -> Option<pg_sys::Oid> {
        IcebergAccessMethod::oid()
    }

    fn execute(
        request: TableMaintenanceRequest<'_>,
    ) -> Result<TableMaintenanceReport, TableMaintenanceError> {
        match Self::execute_iceberg(request, None)
            .map_err(TableMaintenanceError::from)?
        {
            MaintenanceExecution::Executed(report) => Ok(report),
            MaintenanceExecution::StaleCandidate => Err(TableMaintenanceError::from(
                IcebergError::InvariantViolated(
                    "explicit Iceberg maintenance cannot have a stale scheduler candidate",
                ),
            )),
        }
    }

    fn inspect(
        relation: &RelationHandle<'_>,
    ) -> Result<TableMaintenanceStats, TableMaintenanceError> {
        Self::inspect_iceberg(relation).map_err(TableMaintenanceError::from)
    }
}
