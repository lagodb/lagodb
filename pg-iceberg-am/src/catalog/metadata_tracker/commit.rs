//! Per-table optimistic catalog commit and VACUUM attempt materialization.
//!
//! A coordinator owns one immutable transaction action plan. Every loop
//! iteration rebuilds its base table and all metadata/manifest state, so no
//! attempt-scoped cache can survive a failed catalog CAS.

use super::*;

struct TableCommitCoordinator<'a> {
    relid: pg_sys::Oid,
    plan: TxTableCommitPlan<'a>,
    file_io: FileIO,
}

impl<'a> TableCommitCoordinator<'a> {
    fn new(
        relid: pg_sys::Oid,
        plan: TxTableCommitPlan<'a>,
        file_io: FileIO,
    ) -> Self {
        Self { relid, plan, file_io }
    }

    fn commit(self) -> IcebergResult<()> {
        let Self { relid, plan, file_io } = self;
        let mut retries = 0;
        let max_retries = gucs::max_commit_retries();
        let vacuum_commit_started = plan
            .vacuum
            .map(|_| std::time::Instant::now());

        loop {
            if retries > max_retries {
                return Err(IcebergError::MetadataCommitConflict {
                    relid,
                    max_retries,
                });
            }
            retries += 1;

            let latest_global_location = IcebergMetadata::get(relid)?
                .metadata_location
                .ok_or(IcebergError::MetadataLocationNull)?;
            if plan.expected_metadata_location.is_some_and(|expected| {
                expected != latest_global_location.as_str()
            }) {
                return Err(IcebergError::TruncateCommitConflict { relid });
            }
            let metadata =
                TableMetadata::read_from(&file_io, &latest_global_location)?;
            let base_table = Table::builder()
                .metadata_location(latest_global_location.clone())
                .metadata(metadata)
                .identifier(
                    IcebergTableId::for_relation(relid).into_table_ident(),
                )
                .file_io(file_io.clone())
                .build()?;

            let catalog = StagedCatalog::new(&base_table);
            let mut tx = Transaction::new(&base_table);
            let owned_table_root = plan
                .vacuum
                .map(|vacuum| &vacuum.owned_table_root);
            if let Some(vacuum) = plan.vacuum {
                vacuum
                    .owned_table_root
                    .ensure_table_location(base_table.metadata().location())?;
                vacuum
                    .owned_table_root
                    .ensure_path(&latest_global_location)?;
                if let Some(rewrite) = &vacuum.rewrite {
                    let action = tx
                        .rewrite_files(
                            rewrite.starting_snapshot_id,
                            rewrite.starting_sequence_number,
                        )
                        .rewrite_data_files(
                            rewrite.input_files.clone(),
                            rewrite.output_files.clone(),
                        )
                        .rewrite_delete_file_identities(
                            rewrite.materialized_delete_identities.clone(),
                        );
                    tx = action.apply(tx)?;
                }
                if let Some(manifests) = vacuum.manifest_rewrite {
                    tx = tx
                        .rewrite_manifests(
                            manifests.min_count_to_merge,
                            manifests.target_size_bytes,
                        )
                        .apply(tx)?;
                }
                tx = tx
                    .expire_snapshots()
                    .as_of_ms(vacuum.expiration.as_of_ms)
                    .apply(tx)?;
            }
            let mut schema_metadata = base_table.metadata().clone();
            for action in &plan.actions {
                match action {
                    EffectiveCommitAction::Schema(schema_update) => {
                        schema_update
                            .validate_base_metadata(&schema_metadata)
                            .map_err(IcebergError::schema_evolution_conflict)?;
                        schema_metadata = schema_update
                            .apply_to_metadata(&schema_metadata)
                            .map_err(IcebergError::from)?;
                        tx = (**schema_update).clone().apply(tx)?;
                    }
                    EffectiveCommitAction::Data {
                        epoch,
                        truncate_base,
                    } => {
                        tx = if epoch.validations.is_empty() {
                            let mut action =
                                tx.snapshot_delta(Arc::clone(&epoch.delta));
                            if *truncate_base {
                                action = action.truncate_base();
                            }
                            action.apply(tx)?
                        } else {
                            let mut action = tx
                                .row_delta(Arc::clone(&epoch.delta))
                                .add_validations(epoch.validations.clone());
                            if *truncate_base {
                                action = action.truncate_base();
                            }
                            action.apply(tx)?
                        };
                    }
                    EffectiveCommitAction::TruncateOnly => {
                        tx = tx
                            .snapshot_delta(Arc::new(SnapshotDelta::new()))
                            .truncate_base()
                            .apply(tx)?;
                    }
                }
            }
            // FileIO registrations during materialization belong to this
            // attempt until the catalog CAS decides whether they survive.
            let metadata_attempt = MetadataAttempt::begin()?;
            let updated_table = tx.commit(&catalog)?;
            let new_metadata_location = updated_table
                .metadata_location()
                .ok_or(IcebergError::MetadataLocationNull)?;
            if let Some(owned_table_root) = owned_table_root {
                owned_table_root.ensure_table_location(
                    updated_table.metadata().location(),
                )?;
                owned_table_root.ensure_path(new_metadata_location)?;
            }
            let mut vacuum_report = plan.vacuum.map(|vacuum| vacuum.report.clone());
            if let Some(report) = &mut vacuum_report {
                let base_snapshot_ids: HashSet<i64> = base_table
                    .metadata()
                    .snapshots()
                    .map(|snapshot| snapshot.snapshot_id())
                    .collect();
                let expired_snapshot_count = base_table
                    .metadata()
                    .snapshots()
                    .filter(|snapshot| {
                        updated_table
                            .metadata()
                            .snapshot_by_id(snapshot.snapshot_id())
                            .is_none()
                    })
                    .count();
                report.snapshots_expired = u64::try_from(expired_snapshot_count)
                .map_err(|_| IcebergError::Vacuum {
                    source: crate::error::IcebergVacuumError::ResourceLimit(
                        "expired snapshot count does not fit u64".to_owned(),
                    ),
                })?;
                let expired_refs = base_table
                    .metadata()
                    .snapshot_references()
                    .filter(|(name, _)| {
                        updated_table.metadata().snapshot_reference(name).is_none()
                    })
                    .count();
                crate::maintenance::record_metric(
                    report,
                    c"expired_refs",
                    u64::try_from(expired_refs).map_err(|_| IcebergError::Vacuum {
                        source: crate::error::IcebergVacuumError::ResourceLimit(
                            "expired reference count does not fit u64".to_owned(),
                        ),
                    })?,
                )?;
                let created_snapshot_count = updated_table
                    .metadata()
                    .snapshots()
                    .filter(|snapshot| {
                        !base_snapshot_ids.contains(&snapshot.snapshot_id())
                    })
                    .count();
                let rewrite_snapshot_count = if plan
                    .vacuum
                    .is_some_and(|vacuum| vacuum.rewrite.is_some())
                {
                    1
                } else {
                    0
                };
                if plan
                    .vacuum
                    .is_some_and(|vacuum| vacuum.manifest_rewrite.is_some())
                    && created_snapshot_count > rewrite_snapshot_count
                {
                    let current = updated_table.metadata().current_snapshot().ok_or(
                        IcebergError::InvariantViolated(
                            "manifest rewrite committed without a current snapshot",
                        ),
                    )?;
                    let manifests = current.load_manifest_list(
                        updated_table.file_io(),
                        &updated_table.metadata_ref(),
                    )?;
                    let rewritten = manifests
                        .entries()
                        .iter()
                        .filter(|manifest| {
                            manifest.added_snapshot_id == current.snapshot_id()
                        })
                        .count();
                    report.manifests_rewritten = u64::try_from(rewritten).map_err(
                        |_| IcebergError::Vacuum {
                            source: crate::error::IcebergVacuumError::ResourceLimit(
                                "rewritten manifest count does not fit u64".to_owned(),
                            ),
                        },
                    )?;
                }
                let cas_retries = retries.checked_sub(1).ok_or_else(|| {
                    IcebergError::Vacuum {
                        source: crate::error::IcebergVacuumError::ResourceLimit(
                            "CAS retry counter underflow".to_owned(),
                        ),
                    }
                })?;
                report.cas_retries = u64::from(cas_retries);
                crate::maintenance::record_metric(
                    report,
                    c"validation_conflicts",
                    report.cas_retries,
                )?;
            }
            let cleanup_planning_started = plan
                .vacuum
                .map(|_| std::time::Instant::now());
            let mut reachability = crate::maintenance::IcebergReachabilityPlanner::default();
            let orphan_cutoff = plan
                .vacuum
                .and_then(|vacuum| vacuum.orphan_cleanup)
                .map(|orphan| orphan.older_than_ms);
            let (expiration_candidates, orphan_candidates) = if plan.vacuum.is_some()
                && (new_metadata_location != latest_global_location
                    || orphan_cutoff.is_some())
            {
                reachability.cleanup_candidates(
                    &base_table,
                    &updated_table,
                    orphan_cutoff,
                    owned_table_root.ok_or(IcebergError::InvariantViolated(
                        "VACUUM reachability has no managed table root",
                    ))?,
                )?
            } else {
                (
                    crate::maintenance::ReachabilityDeletionCandidates::default(),
                    HashSet::new(),
                )
            };
            if let Some(report) = &mut vacuum_report {
                crate::maintenance::record_metric(
                    report,
                    c"expiration_unreachable_objects",
                    u64::try_from(expiration_candidates.paths.len()).map_err(|_| {
                        IcebergError::Vacuum {
                            source: crate::error::IcebergVacuumError::ResourceLimit(
                                "expiration cleanup count does not fit u64".to_owned(),
                            ),
                        }
                    })?,
                )?;
                for (name, value) in [
                    (c"unreachable_data_files", expiration_candidates.data),
                    (
                        c"unreachable_delete_files",
                        expiration_candidates.delete,
                    ),
                    (
                        c"unreachable_manifests",
                        expiration_candidates.manifest,
                    ),
                    (
                        c"unreachable_manifest_lists",
                        expiration_candidates.manifest_list,
                    ),
                    (
                        c"unreachable_statistics",
                        expiration_candidates.statistics,
                    ),
                    (
                        c"unreachable_metadata_files",
                        expiration_candidates.metadata,
                    ),
                ] {
                    crate::maintenance::record_metric(report, name, value)?;
                }
            }
            let mut cleanup_candidates = expiration_candidates.paths;
            if orphan_cutoff.is_some() {
                if let Some(report) = &mut vacuum_report {
                    crate::maintenance::record_metric(
                        report,
                        c"orphan_candidates",
                        u64::try_from(orphan_candidates.len()).map_err(|_| {
                            IcebergError::Vacuum {
                                source: crate::error::IcebergVacuumError::ResourceLimit(
                                    "orphan candidate count does not fit u64".to_owned(),
                                ),
                            }
                        })?,
                    )?;
                }
                cleanup_candidates.extend(orphan_candidates);
            }
            if let (Some(report), Some(started)) =
                (&mut vacuum_report, cleanup_planning_started)
            {
                crate::maintenance::record_metric(
                    report,
                    c"cleanup_planning_ms",
                    u64::try_from(started.elapsed().as_millis()).map_err(|_| {
                        IcebergError::Vacuum {
                            source: crate::error::IcebergVacuumError::ResourceLimit(
                                "cleanup planning duration does not fit u64 milliseconds"
                                    .to_owned(),
                            ),
                        }
                    })?,
                )?;
            }
            if new_metadata_location == latest_global_location {
                metadata_attempt.discard()?;
                if plan.vacuum.is_some() && !cleanup_candidates.is_empty() {
                    if let Some(report) = &mut vacuum_report {
                        report.objects_scheduled_for_deletion =
                            u64::try_from(cleanup_candidates.len()).map_err(|_| {
                                IcebergError::Vacuum {
                                    source: crate::error::IcebergVacuumError::ResourceLimit(
                                        "cleanup candidate count does not fit u64"
                                            .to_owned(),
                                    ),
                                }
                            })?;
                    }
                    match IcebergMetadata::lock_and_validate_location(
                        relid,
                        &latest_global_location,
                    ) {
                        Ok(()) => {
                            let registration =
                                crate::maintenance::VacuumCleanup::register(
                                    relid,
                                    &file_io,
                                    cleanup_candidates,
                                )?;
                            if let Some(report) = &mut vacuum_report {
                                registration.record(report)?;
                            }
                        }
                        Err(IcebergError::MetadataCatalogConflict) => {
                            diag::report_notice(
                                "Concurrent Iceberg update detected, rebasing...",
                            );
                            continue;
                        }
                        Err(error) => return Err(error),
                    }
                }
                if let (Some(vacuum), Some(report)) = (plan.vacuum, &vacuum_report) {
                    let mut report = report.clone();
                    if let Some(started) = &vacuum_commit_started {
                        crate::maintenance::record_metric(
                            &mut report,
                            c"commit_ms",
                            u64::try_from(started.elapsed().as_millis()).map_err(|_| {
                                IcebergError::Vacuum {
                                    source: crate::error::IcebergVacuumError::ResourceLimit(
                                        "VACUUM commit duration does not fit u64 milliseconds"
                                            .to_owned(),
                                    ),
                                }
                            })?,
                        )?;
                    }
                    vacuum.report_success(&report);
                }
                crate::storage::transactional_artifacts::register_canceled_files_for_commit(
                    file_io.clone(),
                    plan.canceled_created_paths.clone(),
                );
                break;
            }

            match IcebergMetadata::cas_update(
                relid,
                Some(&latest_global_location),
                CasUpdate {
                    metadata_location: Some(new_metadata_location),
                    previous_metadata_location: Some(&latest_global_location),
                },
            ) {
                Ok(()) => {
                    metadata_attempt.promote()?;
                    if plan.vacuum.is_some() {
                        if let Some(report) = &mut vacuum_report {
                            report.objects_scheduled_for_deletion =
                                u64::try_from(cleanup_candidates.len()).map_err(|_| {
                                    IcebergError::Vacuum {
                                        source: crate::error::IcebergVacuumError::ResourceLimit(
                                            "cleanup candidate count does not fit u64"
                                                .to_owned(),
                                        ),
                                    }
                                })?;
                        }
                        let registration = crate::maintenance::VacuumCleanup::register(
                            relid,
                            &file_io,
                            cleanup_candidates,
                        )?;
                        if let Some(report) = &mut vacuum_report {
                            registration.record(report)?;
                        }
                    }
                    if let (Some(vacuum), Some(report)) = (plan.vacuum, &vacuum_report) {
                        let mut report = report.clone();
                        if let Some(started) = &vacuum_commit_started {
                            crate::maintenance::record_metric(
                                &mut report,
                                c"commit_ms",
                                u64::try_from(started.elapsed().as_millis()).map_err(|_| {
                                    IcebergError::Vacuum {
                                        source: crate::error::IcebergVacuumError::ResourceLimit(
                                            "VACUUM commit duration does not fit u64 milliseconds"
                                                .to_owned(),
                                        ),
                                    }
                                })?,
                            )?;
                        }
                        vacuum.report_success(&report);
                    }
                    crate::storage::transactional_artifacts::register_canceled_files_for_commit(
                        file_io.clone(),
                        plan.canceled_created_paths.clone(),
                    );
                    break;
                }
                Err(IcebergError::MetadataCatalogConflict) => {
                    metadata_attempt.discard()?;
                    diag::report_notice(
                        "Concurrent Iceberg update detected, rebasing...",
                    );
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }
}

impl TxMetadata {
    /// Detach one immutable action snapshot at a time, then hand all I/O,
    /// optimistic retry, artifact and cleanup work to the table coordinator.
    pub(super) fn commit_all(&self) -> IcebergResult<()> {
        let mut table_oids: Vec<pg_sys::Oid> =
            self.inner.borrow().tables.keys().copied().collect();
        table_oids.sort_unstable_by_key(|oid| u32::from(*oid));

        for relid in table_oids {
            let Some(TableCommitInput { actions, file_io }) =
                self.commit_input(relid)?
            else {
                continue;
            };
            TableCommitCoordinator::new(relid, actions.commit_plan()?, file_io)
                .commit()?;
        }
        Ok(())
    }
}
