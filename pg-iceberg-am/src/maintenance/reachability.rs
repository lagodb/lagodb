use std::collections::{HashMap, HashSet};

use iceberg_lite::spec::{ManifestContentType, Snapshot, TableMetadata};
use iceberg_lite::table::Table;

use crate::error::IcebergResult;
use crate::storage::{LocalStorage, ObjectStorage};

use super::types::ManagedTableRoot;

/// Java-compatible incremental reachable-file cleanup.
pub(crate) struct IcebergReachabilityPlanner;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CandidateKind {
    Data,
    Delete,
    Manifest,
    ManifestList,
    Statistics,
    Metadata,
}

#[derive(Debug, Default)]
pub(crate) struct ReachabilityDeletionCandidates {
    pub(crate) paths: HashSet<String>,
    pub(crate) data: u64,
    pub(crate) delete: u64,
    pub(crate) manifest: u64,
    pub(crate) manifest_list: u64,
    pub(crate) statistics: u64,
    pub(crate) metadata: u64,
}

impl IcebergReachabilityPlanner {
    pub(crate) fn deletion_candidates(
        before: &Table,
        after: &Table,
        owned_table_root: &ManagedTableRoot,
    ) -> IcebergResult<ReachabilityDeletionCandidates> {
        let retained_ids: HashSet<i64> = after
            .metadata()
            .snapshots()
            .map(|snapshot| snapshot.snapshot_id())
            .collect();
        let mut candidates = HashMap::new();
        for snapshot in before.metadata().snapshots() {
            pgrx::pg_sys::check_for_interrupts!();
            if !retained_ids.contains(&snapshot.snapshot_id()) {
                Self::seed_snapshot_candidates(before, snapshot, &mut candidates)?;
                Self::visit_statistics(before.metadata(), snapshot.snapshot_id(), |path| {
                    candidates
                        .entry(path.to_owned())
                        .or_insert(CandidateKind::Statistics);
                });
            }
        }

        // Metadata JSON retention is independent of snapshot expiration. The
        // metadata builder trims the previous-file log while producing the new
        // commit, so collect only entries that disappeared from that bounded
        // log instead of listing or materializing the table's live data set.
        let mut retained_metadata = HashSet::new();
        if let Some(location) = after.metadata_location() {
            retained_metadata.insert(location);
        }
        retained_metadata.extend(
            after
                .metadata()
                .metadata_log()
                .iter()
                .map(|entry| entry.metadata_file.as_str()),
        );
        if let Some(location) = before.metadata_location()
            && !retained_metadata.contains(location)
        {
            candidates
                .entry(location.to_owned())
                .or_insert(CandidateKind::Metadata);
        }
        for entry in before.metadata().metadata_log() {
            if !retained_metadata.contains(entry.metadata_file.as_str()) {
                candidates
                    .entry(entry.metadata_file.clone())
                    .or_insert(CandidateKind::Metadata);
            }
        }

        // Do not materialize a second table-sized live set. Every retained
        // manifest is streamed and removes paths directly from candidates.
        for snapshot in after.metadata().snapshots() {
            pgrx::pg_sys::check_for_interrupts!();
            Self::visit_snapshot(after, snapshot, |path| {
                candidates.remove(path);
            })?;
            Self::visit_statistics(after.metadata(), snapshot.snapshot_id(), |path| {
                candidates.remove(path);
            });
        }
        for location in retained_metadata {
            candidates.remove(location);
        }
        Self::validate_owned_candidates(owned_table_root, candidates.keys())?;
        let mut result = ReachabilityDeletionCandidates::default();
        for kind in candidates.values() {
            let counter = match kind {
                CandidateKind::Data => &mut result.data,
                CandidateKind::Delete => &mut result.delete,
                CandidateKind::Manifest => &mut result.manifest,
                CandidateKind::ManifestList => &mut result.manifest_list,
                CandidateKind::Statistics => &mut result.statistics,
                CandidateKind::Metadata => &mut result.metadata,
            };
            *counter = counter.checked_add(1).ok_or_else(|| {
                crate::error::IcebergError::Vacuum {
                    source: crate::error::IcebergVacuumError::ResourceLimit(
                        "reachability candidate count overflow".to_owned(),
                    ),
                }
            })?;
        }
        result.paths = candidates.into_keys().collect();
        Ok(result)
    }

    pub(crate) fn orphan_candidates(
        table: &Table,
        older_than_ms: i64,
        owned_table_root: &ManagedTableRoot,
    ) -> IcebergResult<HashSet<String>> {
        let storage = table.file_io().storage();
        let mut candidates = if let Some(local) = storage.as_any().downcast_ref::<LocalStorage>() {
            local.list_older_than(table.metadata().location(), older_than_ms)?
        } else if let Some(object) = storage.as_any().downcast_ref::<ObjectStorage>() {
            object.list_older_than(table.metadata().location(), older_than_ms)?
        } else {
            return Err(crate::error::IcebergError::InvariantViolated(
                "orphan cleanup requires a known local or object storage backend",
            ));
        };
        Self::remove_reachable(table, &mut candidates)?;
        Self::validate_owned_candidates(owned_table_root, candidates.iter())?;
        Ok(candidates)
    }

    fn validate_owned_candidates<'a>(
        owned_table_root: &ManagedTableRoot,
        candidates: impl IntoIterator<Item = &'a String>,
    ) -> IcebergResult<()> {
        for path in candidates {
            owned_table_root.ensure_path(path)?;
        }
        Ok(())
    }

    fn seed_snapshot_candidates(
        table: &Table,
        snapshot: &Snapshot,
        candidates: &mut HashMap<String, CandidateKind>,
    ) -> IcebergResult<()> {
        candidates
            .entry(snapshot.manifest_list().to_owned())
            .or_insert(CandidateKind::ManifestList);
        let manifest_list =
            snapshot.load_manifest_list(table.file_io(), &table.metadata_ref())?;
        for manifest_file in manifest_list.entries() {
            pgrx::pg_sys::check_for_interrupts!();
            candidates
                .entry(manifest_file.manifest_path.clone())
                .or_insert(CandidateKind::Manifest);
            let content_kind = match manifest_file.content {
                ManifestContentType::Data => CandidateKind::Data,
                ManifestContentType::Deletes => CandidateKind::Delete,
            };
            let manifest = manifest_file.load_manifest(table.file_io())?;
            for entry in manifest.entries() {
                if entry.is_alive() {
                    candidates
                        .entry(entry.file_path().to_owned())
                        .or_insert(content_kind);
                }
            }
        }
        Ok(())
    }

    fn remove_reachable(
        table: &Table,
        candidates: &mut HashSet<String>,
    ) -> IcebergResult<()> {
        if let Some(location) = table.metadata_location() {
            candidates.remove(location);
        }
        for log in table.metadata().metadata_log() {
            candidates.remove(&log.metadata_file);
        }
        for snapshot in table.metadata().snapshots() {
            Self::visit_snapshot(table, snapshot, |path| {
                candidates.remove(path);
            })?;
            Self::visit_statistics(table.metadata(), snapshot.snapshot_id(), |path| {
                candidates.remove(path);
            });
        }
        Ok(())
    }

    fn visit_snapshot(
        table: &Table,
        snapshot: &Snapshot,
        mut visit: impl FnMut(&str),
    ) -> IcebergResult<()> {
        visit(snapshot.manifest_list());
        let manifest_list =
            snapshot.load_manifest_list(table.file_io(), &table.metadata_ref())?;
        for manifest_file in manifest_list.entries() {
            pgrx::pg_sys::check_for_interrupts!();
            visit(&manifest_file.manifest_path);
            let manifest = manifest_file.load_manifest(table.file_io())?;
            for entry in manifest.entries() {
                if entry.is_alive() {
                    visit(entry.file_path());
                }
            }
        }
        Ok(())
    }

    fn visit_statistics(
        metadata: &TableMetadata,
        snapshot_id: i64,
        mut visit: impl FnMut(&str),
    ) {
        if let Some(statistics) = metadata.statistics_for_snapshot(snapshot_id) {
            visit(&statistics.statistics_path);
        }
        if let Some(statistics) = metadata.partition_statistics_for_snapshot(snapshot_id) {
            visit(&statistics.statistics_path);
        }
    }
}
