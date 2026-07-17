// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use uuid::Uuid;

use crate::overlay::{
    DeleteFileIdentity, ResolvedSnapshotDelta, SnapshotDelta,
    SnapshotDeltaRemovalLookup, SnapshotDeltaRemovals,
};
use crate::spec::{
    DataContentType, DataFile, DataFileFormat, FirstRowIdInheritance, FormatVersion,
    MAIN_BRANCH, Manifest, ManifestContentType, ManifestFile, ManifestListWriter,
    ManifestWriter, ManifestWriterBuilder, Operation, PartitionSpecRef, Snapshot,
    SnapshotReference, SnapshotRetention, SnapshotSummaryCollector, Struct,
    StructType, Summary, TableProperties, update_snapshot_summaries,
};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableRequirement, TableUpdate};

const META_ROOT_PATH: &str = "metadata";

/// Transaction action that materializes a [`SnapshotDelta`] as standard
/// Iceberg manifests, manifest list, and snapshot metadata.
///
/// AddData is part of the transaction-local overlay model, so this action
/// intentionally supports append-only deltas as well as mixed
/// append/delete/remove deltas. [`super::append::FastAppendAction`] remains as
/// the upstream-compatible pure append API for callers that do not use overlay
/// semantics.
pub struct SnapshotDeltaAction {
    delta: Arc<SnapshotDelta>,
    truncate_base: bool,
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
}

impl SnapshotDeltaAction {
    pub(crate) fn new(delta: Arc<SnapshotDelta>) -> Self {
        Self {
            delta,
            truncate_base: false,
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::new(),
        }
    }

    /// Remove all content inherited from the action's base snapshot.
    #[must_use]
    pub fn truncate_base(mut self) -> Self {
        self.truncate_base = true;
        self
    }

    /// Set whether to check duplicate file paths against the current snapshot.
    pub fn with_check_duplicate(mut self, value: bool) -> Self {
        self.check_duplicate = value;
        self
    }

    /// Set commit UUID for generated metadata file names.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(
        mut self,
        snapshot_properties: HashMap<String, String>,
    ) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }
}

impl TransactionAction for SnapshotDeltaAction {
    fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let plan =
            DeltaPlan::from_delta_with_truncate(&self.delta, self.truncate_base);
        if plan.is_empty() {
            return Ok(ActionCommit::new(Vec::new(), Vec::new()));
        }

        let producer = DeltaSnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
        );
        producer.validate_plan(&plan)?;

        if self.check_duplicate {
            producer.validate_duplicate_files(&plan.added_file_paths)?;
        }

        producer.commit(plan)
    }
}

pub(super) struct DeltaCommitSemantics {
    pub(super) operation: Operation,
    pub(super) added_sequence_number: i64,
}

/// Current-snapshot manifests loaded once for one transaction commit attempt.
/// The value is owned by the snapshot producer and is never retained across a
/// catalog retry.
pub(super) struct CurrentSnapshotInventory {
    manifests: Vec<(ManifestFile, Manifest)>,
}

impl CurrentSnapshotInventory {
    pub(super) fn load(table: &Table) -> Result<Self> {
        let current_snapshot = table.metadata().current_snapshot().ok_or_else(|| {
            Error::new(
                ErrorKind::DataConflict,
                "current snapshot inventory requires a current snapshot",
            )
        })?;
        let manifest_list =
            current_snapshot.load_manifest_list(table.file_io(), &table.metadata_ref())?;
        let mut manifests = Vec::with_capacity(manifest_list.entries().len());
        for manifest_file in manifest_list.entries() {
            manifests.push((
                manifest_file.clone(),
                manifest_file.load_manifest(table.file_io())?,
            ));
        }
        Ok(Self { manifests })
    }

    pub(super) fn manifests(&self) -> &[(ManifestFile, Manifest)] {
        &self.manifests
    }
}

#[derive(Default)]
pub(super) struct DeltaPlan {
    pub(super) added_data_files: Vec<DataFile>,
    pub(super) position_delete_files: Vec<DataFile>,
    pub(super) removals: SnapshotDeltaRemovals,
    pub(super) added_file_paths: HashSet<String>,
    pub(super) referenced_data_files: BTreeSet<String>,
}

impl DeltaPlan {
    pub(super) fn from_delta_with_truncate(
        delta: &SnapshotDelta,
        truncate_base: bool,
    ) -> Self {
        let mut plan = Self::from_resolved(delta.resolve());
        if truncate_base {
            plan.removals.set_truncates_base();
        }
        plan
    }

    fn from_resolved(resolved: ResolvedSnapshotDelta<'_>) -> Self {
        let removals = resolved.removals();
        let added_data_files = resolved
            .added_data_files
            .into_iter()
            .map(|data_file| data_file.file.clone())
            .collect();

        let mut position_delete_files =
            Vec::with_capacity(resolved.position_delete_files.len());
        let mut referenced_data_file_set = BTreeSet::new();
        for pending in resolved.position_delete_files {
            let referenced_data_files = pending.referenced_data_files.as_slice();
            debug_assert!(
                !referenced_data_files.is_empty(),
                "resolved position delete should reference at least one data file"
            );
            let Some((path, remaining_paths)) = referenced_data_files.split_first()
            else {
                continue;
            };
            referenced_data_file_set.extend(
                referenced_data_files
                    .iter()
                    .map(|referenced| (*referenced).to_owned()),
            );

            let mut file = (*pending.file).clone();
            if remaining_paths.is_empty() {
                file.referenced_data_file = Some((*path).to_owned());
            } else {
                file.referenced_data_file = None;
            }
            position_delete_files.push(file);
        }

        Self {
            added_data_files,
            position_delete_files,
            removals,
            added_file_paths: resolved
                .added_file_paths
                .into_iter()
                .map(str::to_owned)
                .collect(),
            referenced_data_files: referenced_data_file_set,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.added_data_files.is_empty()
            && self.position_delete_files.is_empty()
            && self.removals.is_empty()
    }

    fn operation(&self) -> Operation {
        let has_adds = !self.added_data_files.is_empty();
        let has_deletes =
            !self.position_delete_files.is_empty() || !self.removals.is_empty();

        match (has_adds, has_deletes) {
            (true, false) => Operation::Append,
            (false, true) => Operation::Delete,
            (true, true) => Operation::Overwrite,
            (false, false) => Operation::Append,
        }
    }
}

pub(super) struct DeltaSnapshotProducer<'a> {
    table: &'a Table,
    snapshot_id: i64,
    commit_uuid: Uuid,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    manifest_counter: u64,
    current_inventory: Option<CurrentSnapshotInventory>,
}

impl<'a> DeltaSnapshotProducer<'a> {
    pub(super) fn new(
        table: &'a Table,
        commit_uuid: Uuid,
        key_metadata: Option<Vec<u8>>,
        snapshot_properties: HashMap<String, String>,
    ) -> Self {
        Self {
            table,
            snapshot_id: Self::generate_unique_snapshot_id(table),
            commit_uuid,
            key_metadata,
            snapshot_properties,
            manifest_counter: 0,
            current_inventory: None,
        }
    }

    pub(super) fn with_current_inventory(
        mut self,
        inventory: CurrentSnapshotInventory,
    ) -> Self {
        self.current_inventory = Some(inventory);
        self
    }

    pub(super) fn validate_plan(&self, plan: &DeltaPlan) -> Result<()> {
        if self.table.metadata().format_version() == FormatVersion::V1
            && !plan.position_delete_files.is_empty()
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "delete files require Iceberg table format v2 or newer",
            ));
        }

        let mut added_dv_targets = HashSet::new();
        for data_file in &plan.added_data_files {
            self.validate_data_file(data_file, DataContentType::Data)?;
        }
        for delete_file in &plan.position_delete_files {
            self.validate_data_file(delete_file, DataContentType::PositionDeletes)?;
            self.validate_position_delete_file_for_version(delete_file)?;
            if Self::is_deletion_vector(delete_file) {
                let target = delete_file.referenced_data_file_path().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "deletion vector delete file must set referenced_data_file",
                    )
                })?;
                if !added_dv_targets.insert(target.to_owned()) {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "snapshot delta contains multiple deletion vectors for data file {target}"
                        ),
                    ));
                }
            }
        }

        Ok(())
    }

    fn validate_position_delete_file_for_version(
        &self,
        delete_file: &DataFile,
    ) -> Result<()> {
        match self.table.metadata().format_version() {
            FormatVersion::V1 => unreachable!("v1 delete files are rejected earlier"),
            FormatVersion::V2 => {
                if delete_file.file_format() == DataFileFormat::Puffin {
                    return Err(Error::new(
                        ErrorKind::FeatureUnsupported,
                        "deletion vectors require Iceberg table format v3 or newer",
                    ));
                }
            }
            FormatVersion::V3 => {
                if !Self::is_deletion_vector(delete_file) {
                    return Err(Error::new(
                        ErrorKind::FeatureUnsupported,
                        "Iceberg table format v3 requires new position deletes to be deletion vectors",
                    ));
                }
                if delete_file.content_offset().is_none()
                    || delete_file.content_size_in_bytes().is_none()
                {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        "deletion vector delete file must set content_offset and content_size_in_bytes",
                    ));
                }
            }
        }

        Ok(())
    }

    fn is_deletion_vector(delete_file: &DataFile) -> bool {
        delete_file.content_type() == DataContentType::PositionDeletes
            && delete_file.file_format() == DataFileFormat::Puffin
    }

    fn validate_data_file(
        &self,
        data_file: &DataFile,
        expected_content: DataContentType,
    ) -> Result<()> {
        if data_file.content_type() != expected_content {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "snapshot delta file {} has content {:?}, expected {:?}",
                    data_file.file_path(),
                    data_file.content_type(),
                    expected_content
                ),
            ));
        }

        let spec = self.partition_spec(data_file.partition_spec_id)?;
        let partition_type =
            spec.partition_type(self.table.metadata().current_schema())?;
        Self::validate_partition_value(data_file.partition(), &partition_type)
    }

    pub(super) fn validate_duplicate_files(
        &self,
        added_paths: &HashSet<String>,
    ) -> Result<()> {
        if added_paths.is_empty() {
            return Ok(());
        }

        let mut referenced_files = Vec::new();
        let Some(current_snapshot) = self.table.metadata().current_snapshot() else {
            return Ok(());
        };
        let manifest_list = current_snapshot
            .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())?;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(self.table.file_io())?;
            for entry in manifest.entries() {
                if entry.is_alive() && added_paths.contains(entry.file_path()) {
                    referenced_files.push(entry.file_path().to_owned());
                }
            }
        }

        if referenced_files.is_empty() {
            return Ok(());
        }

        referenced_files.sort_unstable();
        referenced_files.dedup();
        Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "cannot add files that are already referenced by table, files: {}",
                referenced_files.join(", ")
            ),
        ))
    }

    pub(super) fn commit(mut self, plan: DeltaPlan) -> Result<ActionCommit> {
        let semantics = DeltaCommitSemantics {
            operation: plan.operation(),
            added_sequence_number: self.table.metadata().next_sequence_number(),
        };
        self.commit_with_semantics(plan, semantics)
    }

    pub(super) fn commit_with_semantics(
        mut self,
        plan: DeltaPlan,
        semantics: DeltaCommitSemantics,
    ) -> Result<ActionCommit> {
        let truncate_full_table = plan.removals.truncates_base();
        let mut summary_collector = self.new_summary_collector();
        let mut manifests =
            self.rewrite_removed_manifests(&plan.removals, &mut summary_collector)?;

        self.write_added_manifests(
            ManifestContentType::Data,
            plan.added_data_files,
            semantics.added_sequence_number,
            &mut summary_collector,
            &mut manifests,
        )?;

        self.write_added_manifests(
            ManifestContentType::Deletes,
            plan.position_delete_files,
            semantics.added_sequence_number,
            &mut summary_collector,
            &mut manifests,
        )?;

        if manifests.is_empty() {
            return Ok(ActionCommit::new(Vec::new(), Vec::new()));
        }

        let summary =
            self.summary(semantics.operation, summary_collector, truncate_full_table)?;
        let (manifest_list_path, writer_next_row_id) =
            self.write_manifest_list(manifests)?;
        let new_snapshot =
            self.new_snapshot(manifest_list_path, summary, writer_next_row_id)?;

        let updates = vec![
            TableUpdate::AddSnapshot {
                snapshot: new_snapshot,
            },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_owned(),
                reference: SnapshotReference::new(
                    self.snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            },
        ];

        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: self.table.metadata().uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_owned(),
                snapshot_id: self.table.metadata().current_snapshot_id(),
            },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }

    fn rewrite_removed_manifests(
        &mut self,
        removals: &SnapshotDeltaRemovals,
        summary_collector: &mut SnapshotSummaryCollector,
    ) -> Result<Vec<ManifestFile>> {
        if !removals.is_empty()
            && let Some(inventory) = self.current_inventory.take()
        {
            return self.rewrite_loaded_manifests(
                inventory.manifests(),
                removals,
                summary_collector,
            );
        }
        let Some(current_snapshot) = self.table.metadata().current_snapshot() else {
            if removals.is_empty() || removals.truncates_base() {
                return Ok(Vec::new());
            }
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "cannot remove files from a table without a current snapshot",
            ));
        };

        let manifest_list = current_snapshot
            .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())?;

        if removals.is_empty() {
            // Carry existing manifests directly from the manifest list. No
            // entries can be affected when this delta does not remove files.
            let mut manifests = Vec::with_capacity(manifest_list.entries().len());
            for manifest_file in manifest_list.entries() {
                if manifest_file.has_added_files()
                    || manifest_file.has_existing_files()
                {
                    manifests.push(manifest_file.clone());
                }
            }
            return Ok(manifests);
        }

        let mut loaded = Vec::with_capacity(manifest_list.entries().len());
        for manifest_file in manifest_list.entries() {
            loaded.push((
                manifest_file.clone(),
                manifest_file.load_manifest(self.table.file_io())?,
            ));
        }
        self.rewrite_loaded_manifests(&loaded, removals, summary_collector)
    }

    fn rewrite_loaded_manifests(
        &mut self,
        loaded: &[(ManifestFile, Manifest)],
        removals: &SnapshotDeltaRemovals,
        summary_collector: &mut SnapshotSummaryCollector,
    ) -> Result<Vec<ManifestFile>> {

        let mut manifests = Vec::new();
        let mut found_removed_paths = HashSet::new();
        let mut found_removed_delete_files = HashSet::new();
        for (manifest_file, manifest) in loaded {
            let is_affected = manifest.entries().iter().any(|entry| {
                entry.is_alive()
                    && removals.removes_manifest_entry(manifest_file.content, entry)
            });

            if is_affected {
                let rewritten = self.rewrite_manifest(
                    manifest_file,
                    manifest.entries(),
                    removals,
                    &mut found_removed_paths,
                    &mut found_removed_delete_files,
                    summary_collector,
                )?;
                manifests.push(rewritten);
            } else if manifest_file.has_added_files()
                || manifest_file.has_existing_files()
            {
                manifests.push(manifest_file.clone());
            }
        }

        if found_removed_paths.len() != removals.removed_data_paths().len() {
            let mut missing: Vec<&str> = removals
                .removed_data_paths()
                .iter()
                .filter_map(|path| {
                    (!found_removed_paths.contains(path.as_str()))
                        .then_some(path.as_str())
                })
                .collect();
            missing.sort_unstable();
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "cannot remove files that are not live in current snapshot: {}",
                    missing.join(", ")
                ),
            ));
        }

        if found_removed_delete_files.len() != removals.removed_delete_files().len() {
            let mut missing: Vec<&str> = removals
                .removed_delete_files()
                .iter()
                .filter_map(|identity| {
                    (!found_removed_delete_files.contains(identity))
                        .then_some(identity.file_path())
                })
                .collect();
            missing.sort_unstable();
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "cannot remove delete files that are not live in current snapshot: {}",
                    missing.join(", ")
                ),
            ));
        }

        Ok(manifests)
    }

    pub(super) fn commit_manifest_rewrite(
        &mut self,
        min_count_to_merge: usize,
        target_size_bytes: u64,
    ) -> Result<ActionCommit> {
        let Some(current_snapshot) = self.table.metadata().current_snapshot() else {
            return Ok(ActionCommit::new(Vec::new(), Vec::new()));
        };
        let manifest_list = current_snapshot
            .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())?;
        let rewrite_plan = super::manifest_rewrite::ManifestRewritePlan::build(
            manifest_list.entries(),
            min_count_to_merge,
            target_size_bytes,
        )?;
        if rewrite_plan.is_empty() {
            return Ok(ActionCommit::new(Vec::new(), Vec::new()));
        }
        let (by_group, selected) = rewrite_plan.into_parts();
        let mut by_group: Vec<_> = by_group.into_iter().collect();
        by_group.sort_unstable_by(
            |((left_spec, left_content), _), ((right_spec, right_content), _)| {
                left_spec
                    .cmp(right_spec)
                    .then_with(|| (*left_content as i32).cmp(&(*right_content as i32)))
            },
        );

        struct ExistingEntry {
            file: DataFile,
            snapshot_id: i64,
            sequence_number: i64,
            file_sequence_number: i64,
        }

        let mut output = Vec::new();
        for ((spec_id, content), manifests) in by_group {
            let selected_group: Vec<&ManifestFile> = manifests
                .iter()
                .filter(|manifest| selected.contains(manifest.manifest_path.as_str()))
                .collect();
            if selected_group.is_empty() {
                output.extend(manifests);
                continue;
            }

            let total_bytes = selected_group.iter().try_fold(
                0_u64,
                |total, manifest| {
                    total
                        .checked_add(u64::try_from(manifest.manifest_length).map_err(
                            |_| Error::new(ErrorKind::DataInvalid, "negative manifest length"),
                        )?)
                        .ok_or_else(|| {
                            Error::new(ErrorKind::DataInvalid, "manifest byte count overflow")
                        })
                },
            )?;
            let output_count = usize::try_from(
                total_bytes.div_ceil(target_size_bytes).max(1),
            )
            .map_err(|_| Error::new(ErrorKind::DataInvalid, "manifest count overflow"))?;
            let mut entries = Vec::new();
            for manifest_file in selected_group {
                let manifest = manifest_file.load_manifest(self.table.file_io())?;
                let mut row_ids = FirstRowIdInheritance::new(manifest_file.first_row_id);
                for entry in manifest.entries() {
                    let effective_first_row_id = row_ids.resolve(entry)?;
                    if !entry.is_alive() {
                        continue;
                    }
                    let mut file = entry.data_file().clone();
                    if self.table.metadata().format_version() == FormatVersion::V3
                        && file.content_type() == DataContentType::Data
                    {
                        file.first_row_id = effective_first_row_id
                            .map(|value| {
                                i64::try_from(value).map_err(|_| {
                                    Error::new(
                                        ErrorKind::DataInvalid,
                                        "first row id does not fit Iceberg long",
                                    )
                                })
                            })
                            .transpose()?;
                    }
                    entries.push(ExistingEntry {
                        file,
                        snapshot_id: entry.snapshot_id().ok_or_else(|| {
                            Error::new(ErrorKind::DataInvalid, "live manifest entry has no snapshot id")
                        })?,
                        sequence_number: entry.sequence_number().ok_or_else(|| {
                            Error::new(ErrorKind::DataInvalid, "live manifest entry has no sequence number")
                        })?,
                        file_sequence_number: entry.file_sequence_number.ok_or_else(|| {
                            Error::new(ErrorKind::DataInvalid, "live manifest entry has no file sequence number")
                        })?,
                    });
                }
            }
            entries.sort_unstable_by(|left, right| {
                left.file.file_path().cmp(right.file.file_path())
            });
            let entries_per_manifest = entries.len().div_ceil(output_count).max(1);
            for chunk in entries.chunks(entries_per_manifest) {
                let mut writer = self.new_manifest_writer(content, spec_id)?;
                for entry in chunk {
                    writer.add_existing_file(
                        entry.file.clone(),
                        entry.snapshot_id,
                        entry.sequence_number,
                        Some(entry.file_sequence_number),
                    )?;
                }
                output.push(writer.write_manifest_file()?);
            }
        }

        let summary = update_snapshot_summaries(
            Summary {
                operation: Operation::Replace,
                additional_properties: HashMap::new(),
            },
            Some(current_snapshot.summary()),
            false,
        )?;
        let (manifest_list_path, writer_next_row_id) =
            self.write_manifest_list(output)?;
        let snapshot =
            self.new_snapshot(manifest_list_path, summary, writer_next_row_id)?;
        Ok(ActionCommit::new(
            vec![
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef {
                    ref_name: MAIN_BRANCH.to_owned(),
                    reference: SnapshotReference::new(
                        self.snapshot_id,
                        SnapshotRetention::branch(None, None, None),
                    ),
                },
            ],
            vec![
                TableRequirement::UuidMatch {
                    uuid: self.table.metadata().uuid(),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_owned(),
                    snapshot_id: self.table.metadata().current_snapshot_id(),
                },
            ],
        ))
    }

    fn rewrite_manifest(
        &mut self,
        manifest_file: &ManifestFile,
        entries: &[Arc<crate::spec::ManifestEntry>],
        removals: &SnapshotDeltaRemovals,
        found_removed_paths: &mut HashSet<String>,
        found_removed_delete_files: &mut HashSet<DeleteFileIdentity>,
        summary_collector: &mut SnapshotSummaryCollector,
    ) -> Result<ManifestFile> {
        let mut writer = self.new_manifest_writer(
            manifest_file.content,
            manifest_file.partition_spec_id,
        )?;
        let mut row_id_inheritance =
            FirstRowIdInheritance::new(manifest_file.first_row_id);

        for entry in entries {
            let effective_first_row_id = row_id_inheritance.resolve(entry)?;
            if !entry.is_alive() {
                continue;
            }

            let snapshot_id = entry.snapshot_id().ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "manifest entry {} has no snapshot id",
                        entry.file_path()
                    ),
                )
            })?;
            let sequence_number = entry.sequence_number().ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "manifest entry {} has no sequence number",
                        entry.file_path()
                    ),
                )
            })?;
            let file_sequence_number =
                entry.file_sequence_number.ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "manifest entry {} has no file sequence number",
                            entry.file_path()
                        ),
                    )
                })?;

            let mut data_file = entry.data_file().clone();
            if self.table.metadata().format_version() == FormatVersion::V3
                && data_file.content_type() == DataContentType::Data
            {
                data_file.first_row_id = effective_first_row_id
                    .map(|first_row_id| {
                        i64::try_from(first_row_id).map_err(|_| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "first_row_id for data file {} does not fit Iceberg long",
                                    entry.file_path()
                                ),
                            )
                        })
                    })
                    .transpose()?;
            }

            if removals.removes_manifest_entry(manifest_file.content, entry) {
                match manifest_file.content {
                    ManifestContentType::Data => {
                        if removals.has_removed_data_path(entry.file_path()) {
                            found_removed_paths.insert(entry.file_path().to_owned());
                        }
                    }
                    ManifestContentType::Deletes => {
                        let identity =
                            DeleteFileIdentity::from_data_file(entry.data_file());
                        if removals.has_removed_delete_file(entry.data_file()) {
                            found_removed_delete_files.insert(identity);
                        }
                    }
                }
                writer.add_delete_file(
                    data_file,
                    sequence_number,
                    Some(file_sequence_number),
                )?;
                self.collect_removed_file(summary_collector, entry.data_file())?;
            } else {
                writer.add_existing_file(
                    data_file,
                    snapshot_id,
                    sequence_number,
                    Some(file_sequence_number),
                )?;
            }
        }

        writer.write_manifest_file()
    }

    fn write_added_manifests(
        &mut self,
        content: ManifestContentType,
        files: Vec<DataFile>,
        added_sequence_number: i64,
        summary_collector: &mut SnapshotSummaryCollector,
        manifests: &mut Vec<ManifestFile>,
    ) -> Result<()> {
        if files.is_empty() {
            return Ok(());
        }

        let mut files_by_spec: HashMap<i32, Vec<DataFile>> = HashMap::new();
        for file in files {
            files_by_spec
                .entry(file.partition_spec_id)
                .or_default()
                .push(file);
        }

        for (spec_id, grouped_files) in files_by_spec {
            let mut writer = self.new_manifest_writer(content, spec_id)?;
            for file in grouped_files {
                self.collect_added_file(summary_collector, &file)?;
                writer.add_file(file, added_sequence_number)?;
            }
            manifests.push(writer.write_manifest_file()?);
        }

        Ok(())
    }

    fn write_manifest_list(
        &self,
        manifests: Vec<ManifestFile>,
    ) -> Result<(String, Option<u64>)> {
        let manifest_list_path = self.generate_manifest_list_file_path(0);
        let next_sequence_number = self.table.metadata().next_sequence_number();
        let first_row_id = self.table.metadata().next_row_id();
        let mut writer = match self.table.metadata().format_version() {
            FormatVersion::V1 => ManifestListWriter::v1(
                self.table
                    .file_io()
                    .new_output(manifest_list_path.clone())?
                    .create_file_writer()?,
                self.snapshot_id,
                self.table.metadata().current_snapshot_id(),
            ),
            FormatVersion::V2 => ManifestListWriter::v2(
                self.table
                    .file_io()
                    .new_output(manifest_list_path.clone())?
                    .create_file_writer()?,
                self.snapshot_id,
                self.table.metadata().current_snapshot_id(),
                next_sequence_number,
            ),
            FormatVersion::V3 => ManifestListWriter::v3(
                self.table
                    .file_io()
                    .new_output(manifest_list_path.clone())?
                    .create_file_writer()?,
                self.snapshot_id,
                self.table.metadata().current_snapshot_id(),
                next_sequence_number,
                // V3 snapshots require first-row-id/added-rows even when no
                // new ID space is consumed. The writer advances this cursor
                // only for data manifests whose first_row_id is unassigned.
                Some(first_row_id),
            ),
        };

        writer.add_manifests(manifests.into_iter())?;
        let writer_next_row_id = writer.next_row_id();
        writer.close()?;
        Ok((manifest_list_path, writer_next_row_id))
    }

    fn new_snapshot(
        &self,
        manifest_list_path: String,
        summary: Summary,
        writer_next_row_id: Option<u64>,
    ) -> Result<Snapshot> {
        let timestamp_ms = chrono::Utc::now().timestamp_millis();
        let snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(self.snapshot_id)
            .with_parent_snapshot_id(self.table.metadata().current_snapshot_id())
            .with_sequence_number(self.table.metadata().next_sequence_number())
            .with_summary(summary)
            .with_schema_id(self.table.metadata().current_schema_id())
            .with_timestamp_ms(timestamp_ms);

        if let Some(writer_next_row_id) = writer_next_row_id {
            let first_row_id = self.table.metadata().next_row_id();
            let assigned_rows = writer_next_row_id
                .checked_sub(first_row_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "snapshot delta row-id assignment moved backwards",
                    )
                })?;
            return Ok(snapshot.with_row_range(first_row_id, assigned_rows).build());
        }

        Ok(snapshot.build())
    }

    fn summary(
        &self,
        operation: Operation,
        summary_collector: SnapshotSummaryCollector,
        truncate_full_table: bool,
    ) -> Result<Summary> {
        let mut additional_properties = summary_collector.build();
        additional_properties.extend(self.snapshot_properties.clone());

        let summary = Summary {
            operation,
            additional_properties,
        };

        update_snapshot_summaries(
            summary,
            self.table
                .metadata()
                .current_snapshot()
                .map(|s| s.summary()),
            truncate_full_table,
        )
    }

    fn new_summary_collector(&self) -> SnapshotSummaryCollector {
        let mut summary_collector = SnapshotSummaryCollector::default();
        let limit = self
            .table
            .metadata()
            .properties()
            .get(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT)
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(
                TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT,
            );
        summary_collector.set_partition_summary_limit(limit);
        summary_collector
    }

    fn collect_added_file(
        &self,
        summary_collector: &mut SnapshotSummaryCollector,
        data_file: &DataFile,
    ) -> Result<()> {
        let partition_spec = self.partition_spec(data_file.partition_spec_id)?;
        summary_collector.add_file(
            data_file,
            self.table.metadata().current_schema().clone(),
            partition_spec,
        );
        Ok(())
    }

    fn collect_removed_file(
        &self,
        summary_collector: &mut SnapshotSummaryCollector,
        data_file: &DataFile,
    ) -> Result<()> {
        let partition_spec = self.partition_spec(data_file.partition_spec_id)?;
        summary_collector.remove_file(
            data_file,
            self.table.metadata().current_schema().clone(),
            partition_spec,
        );
        Ok(())
    }

    fn new_manifest_writer(
        &mut self,
        content: ManifestContentType,
        partition_spec_id: i32,
    ) -> Result<ManifestWriter> {
        let manifest_counter = self.manifest_counter;
        self.manifest_counter = self
            .manifest_counter
            .checked_add(1)
            .expect("snapshot delta manifest counter should not overflow");
        let new_manifest_path = format!(
            "{}/{}/{}-m{}.{}",
            self.table.metadata().location(),
            META_ROOT_PATH,
            self.commit_uuid,
            manifest_counter,
            DataFileFormat::Avro
        );
        let output_file = self.table.file_io().new_output(new_manifest_path)?;
        let partition_spec = self.partition_spec(partition_spec_id)?;
        let builder = ManifestWriterBuilder::new(
            output_file,
            Some(self.snapshot_id),
            self.key_metadata.clone(),
            self.table.metadata().current_schema().clone(),
            partition_spec.as_ref().clone(),
        );

        match self.table.metadata().format_version() {
            FormatVersion::V1 => {
                if content != ManifestContentType::Data {
                    return Err(Error::new(
                        ErrorKind::FeatureUnsupported,
                        "Iceberg table format v1 cannot write delete manifests",
                    ));
                }
                Ok(builder.build_v1())
            }
            FormatVersion::V2 => match content {
                ManifestContentType::Data => Ok(builder.build_v2_data()),
                ManifestContentType::Deletes => Ok(builder.build_v2_deletes()),
            },
            FormatVersion::V3 => match content {
                ManifestContentType::Data => Ok(builder.build_v3_data()),
                ManifestContentType::Deletes => Ok(builder.build_v3_deletes()),
            },
        }
    }

    fn partition_spec(&self, spec_id: i32) -> Result<PartitionSpecRef> {
        self.table
            .metadata()
            .partition_spec_by_id(spec_id)
            .cloned()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("partition spec id {spec_id} does not exist"),
                )
            })
    }

    fn validate_partition_value(
        partition_value: &Struct,
        partition_type: &StructType,
    ) -> Result<()> {
        if partition_value.fields().len() != partition_type.fields().len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "partition value is not compatible with partition type",
            ));
        }

        for (value, field) in
            partition_value.fields().iter().zip(partition_type.fields())
        {
            let Some(field_type) = field.field_type.as_primitive_type() else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "partition field should only be primitive type",
                ));
            };
            let Some(value) = value else {
                continue;
            };
            let Some(primitive_value) = value.as_primitive_literal() else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "partition value should be primitive literal",
                ));
            };
            if !field_type.compatible(&primitive_value) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "partition value is not compatible with partition type",
                ));
            }
        }

        Ok(())
    }

    fn generate_manifest_list_file_path(&self, attempt: i64) -> String {
        format!(
            "{}/{}/snap-{}-{}-{}.{}",
            self.table.metadata().location(),
            META_ROOT_PATH,
            self.snapshot_id,
            attempt,
            self.commit_uuid,
            DataFileFormat::Avro
        )
    }

    fn generate_unique_snapshot_id(table: &Table) -> i64 {
        fn random_positive_i64() -> i64 {
            let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
            let raw = (lhs ^ rhs) & (i64::MAX as u64);
            i64::try_from(raw).expect("masked UUID bits fit into i64")
        }

        let mut snapshot_id = random_positive_i64();
        while table
            .metadata()
            .snapshots()
            .any(|snapshot| snapshot.snapshot_id() == snapshot_id)
        {
            snapshot_id = random_positive_i64();
        }
        snapshot_id
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::memory::tests::new_memory_catalog;
    use crate::overlay::SnapshotDelta;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion,
        Literal, ManifestStatus, Struct, TableMetadata,
    };
    use crate::table::Table;
    use crate::transaction::tests::{
        make_v2_minimal_table, make_v3_minimal_table_in_catalog,
    };
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, TableCreation, TableIdent};

    use super::*;

    #[test]
    fn snapshot_delta_add_data_commits_data_manifest() {
        let catalog = new_memory_catalog();
        let table = make_v2_table_in_catalog(&catalog);
        let mut delta = SnapshotDelta::new();
        delta
            .add_data_file(data_file("test/data-a.parquet"))
            .unwrap();

        let updated = commit_delta(&catalog, &table, delta);

        let tasks = updated.scan().build().unwrap().plan_files().unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].data_file_path(), "test/data-a.parquet");

        let manifest_list = current_manifest_list(&updated);
        assert_eq!(manifest_list.entries().len(), 1);
        assert_eq!(
            manifest_list.entries()[0].content,
            ManifestContentType::Data
        );
    }

    #[test]
    fn snapshot_delta_add_then_remove_is_noop() {
        let catalog = new_memory_catalog();
        let table = make_v2_table_in_catalog(&catalog);
        let mut delta = SnapshotDelta::new();
        delta
            .add_data_file(data_file("test/transient.parquet"))
            .unwrap();
        delta.remove_data_file("test/transient.parquet").unwrap();

        let tx = Transaction::new(&table);
        let tx = tx.snapshot_delta(Arc::new(delta)).apply(tx).unwrap();
        let updated = tx.commit(&catalog).unwrap();

        assert_eq!(
            updated.metadata().current_snapshot_id(),
            table.metadata().current_snapshot_id()
        );
        assert_eq!(updated.metadata_location(), table.metadata_location());
        assert!(
            updated
                .scan()
                .build()
                .unwrap()
                .plan_files()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn snapshot_delta_position_delete_with_add_writes_delete_manifest() {
        let catalog = new_memory_catalog();
        let table = make_v2_table_in_catalog(&catalog);
        let mut delta = SnapshotDelta::new();
        delta
            .add_data_file(data_file("test/data-a.parquet"))
            .unwrap();
        delta
            .add_position_delete_file(
                position_delete_file("test/pos-a.parquet"),
                ["test/data-a.parquet"],
            )
            .unwrap();

        let updated = commit_delta(&catalog, &table, delta);

        let tasks = updated.scan().build().unwrap().plan_files().unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].deletes.len(), 1);
        assert_eq!(tasks[0].deletes[0].file_path, "test/pos-a.parquet");
        assert_eq!(
            tasks[0].deletes[0].file_type,
            DataContentType::PositionDeletes
        );

        let manifest_list = current_manifest_list(&updated);
        let data_manifests = manifest_list
            .entries()
            .iter()
            .filter(|entry| entry.content == ManifestContentType::Data)
            .count();
        let delete_manifests = manifest_list
            .entries()
            .iter()
            .filter(|entry| entry.content == ManifestContentType::Deletes)
            .count();
        assert_eq!(data_manifests, 1);
        assert_eq!(delete_manifests, 1);
    }

    #[test]
    fn snapshot_delta_append_only_reuses_manifest_list_without_loading_old_manifests()
    {
        let catalog = new_memory_catalog();
        let table = make_v2_table_in_catalog(&catalog);
        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files([data_file("test/base.parquet")])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).unwrap();
        let base_manifest_path = current_manifest_list(&table).entries()[0]
            .manifest_path
            .clone();

        table
            .file_io()
            .new_output(&base_manifest_path)
            .unwrap()
            .write(b"not an avro manifest")
            .unwrap();

        let mut delta = SnapshotDelta::new();
        delta
            .add_data_file(data_file("test/append.parquet"))
            .unwrap();
        let tx = Transaction::new(&table);
        let tx = tx
            .snapshot_delta(Arc::new(delta))
            // Duplicate checks legitimately read existing manifests; this test
            // isolates the no-remove materialization path.
            .with_check_duplicate(false)
            .apply(tx)
            .unwrap();
        let updated = tx.commit(&catalog).unwrap();

        let manifest_list = current_manifest_list(&updated);
        assert_eq!(manifest_list.entries().len(), 2);
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|entry| entry.manifest_path == base_manifest_path)
        );
    }

    #[test]
    fn snapshot_delta_remove_existing_file_rewrites_manifest() {
        let catalog = new_memory_catalog();
        let table = make_v2_table_in_catalog(&catalog);
        let base_file = data_file("test/base.parquet");
        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files([base_file.clone()])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).unwrap();

        let mut delta = SnapshotDelta::new();
        delta.remove_data_file(base_file.file_path()).unwrap();
        let updated = commit_delta(&catalog, &table, delta);

        assert!(
            updated
                .scan()
                .build()
                .unwrap()
                .plan_files()
                .unwrap()
                .is_empty()
        );

        let manifest_list = current_manifest_list(&updated);
        assert_eq!(manifest_list.entries().len(), 1);
        assert!(manifest_list.entries()[0].has_deleted_files());

        let manifest = manifest_list.entries()[0]
            .load_manifest(updated.file_io())
            .unwrap();
        assert_eq!(manifest.entries().len(), 1);
        assert_eq!(manifest.entries()[0].status(), ManifestStatus::Deleted);
        assert_eq!(manifest.entries()[0].file_path(), "test/base.parquet");
    }

    #[test]
    fn snapshot_delta_truncate_rewrites_data_and_delete_manifests() {
        let catalog = new_memory_catalog();
        let table = make_v2_table_in_catalog(&catalog);
        let mut append = SnapshotDelta::new();
        append
            .add_data_file(data_file("test/base.parquet"))
            .unwrap();
        append
            .add_position_delete_file(
                position_delete_file("test/base-delete.parquet"),
                ["test/base.parquet"],
            )
            .unwrap();
        let table = commit_delta(&catalog, &table, append);
        let parent_snapshot_id = table.metadata().current_snapshot_id();

        let tx = Transaction::new(&table);
        let tx = tx
            .snapshot_delta(Arc::new(SnapshotDelta::new()))
            .truncate_base()
            .apply(tx)
            .unwrap();
        let updated = tx.commit(&catalog).unwrap();

        assert_ne!(updated.metadata().current_snapshot_id(), parent_snapshot_id);
        assert!(
            updated
                .scan()
                .build()
                .unwrap()
                .plan_files()
                .unwrap()
                .is_empty()
        );
        let snapshot = updated.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.parent_snapshot_id(), parent_snapshot_id);
        assert_eq!(snapshot.summary().operation, Operation::Delete);
        assert_eq!(
            snapshot.summary().additional_properties["total-data-files"],
            "0"
        );
        assert_eq!(
            snapshot.summary().additional_properties["total-delete-files"],
            "0"
        );

        let manifest_list = current_manifest_list(&updated);
        assert_eq!(manifest_list.entries().len(), 2);
        for manifest_file in manifest_list.entries() {
            assert!(manifest_file.has_deleted_files());
            let manifest = manifest_file.load_manifest(updated.file_io()).unwrap();
            assert!(
                manifest
                    .entries()
                    .iter()
                    .all(|entry| entry.status() == ManifestStatus::Deleted)
            );
        }
    }

    #[test]
    fn snapshot_delta_truncate_with_add_is_overwrite() {
        let catalog = new_memory_catalog();
        let table = make_v2_table_in_catalog(&catalog);
        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files([data_file("test/base.parquet")])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).unwrap();
        let mut replacement = SnapshotDelta::new();
        replacement
            .add_data_file(data_file("test/replacement.parquet"))
            .unwrap();

        let tx = Transaction::new(&table);
        let tx = tx
            .snapshot_delta(Arc::new(replacement))
            .truncate_base()
            .apply(tx)
            .unwrap();
        let updated = tx.commit(&catalog).unwrap();

        let tasks = updated.scan().build().unwrap().plan_files().unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].data_file_path(), "test/replacement.parquet");
        let summary = updated.metadata().current_snapshot().unwrap().summary();
        assert_eq!(summary.operation, Operation::Overwrite);
        assert_eq!(summary.additional_properties["total-data-files"], "1");
        assert_eq!(summary.additional_properties["total-records"], "1");
    }

    #[test]
    fn snapshot_delta_truncate_empty_table_is_noop() {
        let catalog = new_memory_catalog();
        let table = make_v2_table_in_catalog(&catalog);

        let tx = Transaction::new(&table);
        let tx = tx
            .snapshot_delta(Arc::new(SnapshotDelta::new()))
            .truncate_base()
            .apply(tx)
            .unwrap();
        let updated = tx.commit(&catalog).unwrap();

        assert_eq!(
            updated.metadata().current_snapshot_id(),
            table.metadata().current_snapshot_id()
        );
        assert_eq!(updated.metadata_location(), table.metadata_location());
    }

    #[test]
    fn snapshot_delta_v3_sets_row_range() {
        let catalog = new_memory_catalog();
        let table = make_v3_minimal_table_in_catalog(&catalog);
        let mut delta = SnapshotDelta::new();
        delta.add_data_file(data_file("test/v3.parquet")).unwrap();

        let updated = commit_delta(&catalog, &table, delta);
        let snapshot = updated.metadata().current_snapshot().unwrap();

        assert_eq!(snapshot.first_row_id(), Some(0));
        assert_eq!(snapshot.added_rows_count(), Some(1));
        assert_eq!(updated.metadata().next_row_id(), 1);
    }

    #[test]
    fn snapshot_delta_v3_suppresses_preassigned_id_on_added_file() {
        let catalog = new_memory_catalog();
        let table = make_v3_minimal_table_in_catalog(&catalog);
        let mut file = data_file("test/preassigned.parquet");
        file.first_row_id = Some(99);
        let mut delta = SnapshotDelta::new();
        delta.add_data_file(file).unwrap();

        let updated = commit_delta(&catalog, &table, delta);
        let manifest_list = current_manifest_list(&updated);
        assert_eq!(manifest_list.entries()[0].first_row_id, Some(0));
        let manifest = manifest_list.entries()[0]
            .load_manifest(updated.file_io())
            .unwrap();
        let added = manifest.entries().iter().find(|entry| entry.is_alive()).unwrap();
        assert_eq!(added.data_file().first_row_id(), None);
        assert_eq!(updated.scan().build().unwrap().plan_files().unwrap()[0].first_row_id, Some(0));
        assert_eq!(updated.metadata().next_row_id(), 1);
    }

    #[test]
    fn snapshot_delta_v3_rewrite_preserves_inherited_file_row_id() {
        let catalog = new_memory_catalog();
        let table = make_v3_minimal_table_in_catalog(&catalog);
        let mut append = SnapshotDelta::new();
        append.add_data_file(data_file("test/a.parquet")).unwrap();
        append.add_data_file(data_file("test/b.parquet")).unwrap();
        let table = commit_delta(&catalog, &table, append);

        let mut remove = SnapshotDelta::new();
        remove.remove_data_file("test/a.parquet").unwrap();
        let updated = commit_delta(&catalog, &table, remove);

        let snapshot = updated.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.first_row_id(), Some(2));
        assert_eq!(snapshot.added_rows_count(), Some(1));
        assert_eq!(updated.metadata().next_row_id(), 3);

        let tasks = updated.scan().build().unwrap().plan_files().unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].data_file_path(), "test/b.parquet");
        assert_eq!(tasks[0].first_row_id, Some(1));

        let manifest_list = current_manifest_list(&updated);
        assert_eq!(manifest_list.entries()[0].first_row_id, Some(2));
        let manifest = manifest_list.entries()[0]
            .load_manifest(updated.file_io())
            .unwrap();
        let remaining = manifest
            .entries()
            .iter()
            .find(|entry| entry.is_alive())
            .unwrap();
        assert_eq!(remaining.data_file().first_row_id(), Some(1));
    }

    fn commit_delta(
        catalog: &impl Catalog,
        table: &Table,
        delta: SnapshotDelta,
    ) -> Table {
        let tx = Transaction::new(table);
        let tx = tx.snapshot_delta(Arc::new(delta)).apply(tx).unwrap();
        tx.commit(catalog).unwrap()
    }

    fn current_manifest_list(table: &Table) -> crate::spec::ManifestList {
        table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .unwrap()
    }

    fn make_v2_table_in_catalog(catalog: &impl Catalog) -> Table {
        let table_ident = TableIdent::from_strs([
            format!("ns-{}", Uuid::new_v4()),
            "test".to_owned(),
        ])
        .unwrap();
        catalog
            .create_namespace(table_ident.namespace(), HashMap::new())
            .unwrap();

        let base_table = make_v2_minimal_table();
        let base_metadata: &TableMetadata = base_table.metadata();
        let table_creation = TableCreation::builder()
            .schema((**base_metadata.current_schema()).clone())
            .partition_spec((**base_metadata.default_partition_spec()).clone())
            .sort_order((**base_metadata.default_sort_order()).clone())
            .name(table_ident.name().to_owned())
            .format_version(FormatVersion::V2)
            .build();

        catalog
            .create_table(table_ident.namespace(), table_creation)
            .unwrap()
    }

    fn data_file(path: &str) -> DataFile {
        file_builder(DataContentType::Data, path).build().unwrap()
    }

    fn position_delete_file(path: &str) -> DataFile {
        file_builder(DataContentType::PositionDeletes, path)
            .build()
            .unwrap()
    }

    fn file_builder(content: DataContentType, path: &str) -> DataFileBuilder {
        let mut builder = DataFileBuilder::default();
        builder
            .content(content)
            .file_path(path.to_owned())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(300))]));
        builder
    }
}
