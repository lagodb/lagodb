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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use uuid::Uuid;

use crate::overlay::{ResolvedSnapshotDelta, SnapshotDelta};
use crate::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH,
    ManifestContentType, ManifestFile, ManifestListWriter, ManifestWriter,
    ManifestWriterBuilder, Operation, PartitionSpecRef, Snapshot, SnapshotReference,
    SnapshotRetention, SnapshotSummaryCollector, Struct, StructType, Summary,
    TableProperties, update_snapshot_summaries,
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
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
}

impl SnapshotDeltaAction {
    pub(crate) fn new(delta: Arc<SnapshotDelta>) -> Self {
        Self {
            delta,
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::new(),
        }
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
        let plan = DeltaPlan::from_delta(&self.delta);
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

#[derive(Default)]
struct DeltaPlan {
    added_data_files: Vec<DataFile>,
    position_delete_files: Vec<DataFile>,
    removed_paths: HashSet<String>,
    added_file_paths: HashSet<String>,
}

impl DeltaPlan {
    fn from_delta(delta: &SnapshotDelta) -> Self {
        Self::from_resolved(delta.resolve())
    }

    fn from_resolved(resolved: ResolvedSnapshotDelta<'_>) -> Self {
        let added_data_files = resolved
            .added_data_files
            .into_iter()
            .map(|data_file| data_file.file.clone())
            .collect();

        let mut position_delete_files =
            Vec::with_capacity(resolved.position_delete_files.len());
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
            removed_paths: resolved
                .removed_data_paths
                .into_iter()
                .map(str::to_owned)
                .collect(),
            added_file_paths: resolved
                .added_file_paths
                .into_iter()
                .map(str::to_owned)
                .collect(),
        }
    }

    fn is_empty(&self) -> bool {
        self.added_data_files.is_empty()
            && self.position_delete_files.is_empty()
            && self.removed_paths.is_empty()
    }

    fn operation(&self) -> Operation {
        let has_adds = !self.added_data_files.is_empty();
        let has_deletes =
            !self.position_delete_files.is_empty() || !self.removed_paths.is_empty();

        match (has_adds, has_deletes) {
            (true, false) => Operation::Append,
            (false, true) => Operation::Delete,
            (true, true) => Operation::Overwrite,
            (false, false) => Operation::Append,
        }
    }
}

struct DeltaSnapshotProducer<'a> {
    table: &'a Table,
    snapshot_id: i64,
    commit_uuid: Uuid,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    manifest_counter: u64,
}

impl<'a> DeltaSnapshotProducer<'a> {
    fn new(
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
        }
    }

    fn validate_plan(&self, plan: &DeltaPlan) -> Result<()> {
        if self.table.metadata().format_version() == FormatVersion::V1
            && !plan.position_delete_files.is_empty()
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "delete files require Iceberg table format v2 or newer",
            ));
        }

        for data_file in &plan.added_data_files {
            self.validate_data_file(data_file, DataContentType::Data)?;
        }
        for delete_file in &plan.position_delete_files {
            self.validate_data_file(delete_file, DataContentType::PositionDeletes)?;
        }

        Ok(())
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

    fn validate_duplicate_files(&self, added_paths: &HashSet<String>) -> Result<()> {
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

    fn commit(mut self, plan: DeltaPlan) -> Result<ActionCommit> {
        let operation = plan.operation();
        let mut summary_collector = self.new_summary_collector();
        let mut manifests = self
            .rewrite_removed_manifests(&plan.removed_paths, &mut summary_collector)?;

        self.write_added_manifests(
            ManifestContentType::Data,
            plan.added_data_files,
            &mut summary_collector,
            &mut manifests,
        )?;

        self.write_added_manifests(
            ManifestContentType::Deletes,
            plan.position_delete_files,
            &mut summary_collector,
            &mut manifests,
        )?;

        if manifests.is_empty() {
            return Ok(ActionCommit::new(Vec::new(), Vec::new()));
        }

        let summary = self.summary(operation, summary_collector)?;
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
        removed_paths: &HashSet<String>,
        summary_collector: &mut SnapshotSummaryCollector,
    ) -> Result<Vec<ManifestFile>> {
        let Some(current_snapshot) = self.table.metadata().current_snapshot() else {
            if removed_paths.is_empty() {
                return Ok(Vec::new());
            }
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "cannot remove files from a table without a current snapshot",
            ));
        };

        let manifest_list = current_snapshot
            .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())?;

        if removed_paths.is_empty() {
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

        let mut manifests = Vec::new();
        let mut found_removed_paths = HashSet::new();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(self.table.file_io())?;
            let is_affected = manifest.entries().iter().any(|entry| {
                entry.is_alive() && removed_paths.contains(entry.file_path())
            });

            if is_affected {
                let rewritten = self.rewrite_manifest(
                    manifest_file,
                    manifest.entries(),
                    removed_paths,
                    &mut found_removed_paths,
                    summary_collector,
                )?;
                manifests.push(rewritten);
            } else if manifest_file.has_added_files()
                || manifest_file.has_existing_files()
            {
                manifests.push(manifest_file.clone());
            }
        }

        if found_removed_paths.len() != removed_paths.len() {
            let mut missing: Vec<&str> = removed_paths
                .iter()
                .filter_map(|path| {
                    (!found_removed_paths.contains(path)).then_some(path.as_str())
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

        Ok(manifests)
    }

    fn rewrite_manifest(
        &mut self,
        manifest_file: &ManifestFile,
        entries: &[Arc<crate::spec::ManifestEntry>],
        removed_paths: &HashSet<String>,
        found_removed_paths: &mut HashSet<String>,
        summary_collector: &mut SnapshotSummaryCollector,
    ) -> Result<ManifestFile> {
        let mut writer = self.new_manifest_writer(
            manifest_file.content,
            manifest_file.partition_spec_id,
        )?;

        for entry in entries {
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

            if removed_paths.contains(entry.file_path()) {
                found_removed_paths.insert(entry.file_path().to_owned());
                writer.add_delete_file(
                    entry.data_file().clone(),
                    sequence_number,
                    Some(file_sequence_number),
                )?;
                self.collect_removed_file(summary_collector, entry.data_file())?;
            } else {
                writer.add_existing_file(
                    entry.data_file().clone(),
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
                writer
                    .add_file(file, self.table.metadata().next_sequence_number())?;
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
            false,
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
