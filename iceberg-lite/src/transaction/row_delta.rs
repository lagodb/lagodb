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

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use uuid::Uuid;

use crate::expr::visitors::inclusive_metrics_evaluator::InclusiveMetricsEvaluator;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::overlay::{SnapshotDelta, SnapshotDeltaRemovalLookup};
use crate::spec::{
    DataContentType, DataFileFormat, IsolationLevel, ManifestContentType,
    ManifestEntry, ManifestStatus, SnapshotRef,
};
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::transaction::snapshot_delta::{DeltaPlan, DeltaSnapshotProducer};
use crate::{Error, ErrorKind, Result};

/// SQL row-level command whose conflict rules are applied to a row delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowLevelCommand {
    /// DELETE writes delete files only.
    Delete,
    /// UPDATE writes delete files for old rows and data files for new rows.
    Update,
    /// MERGE may combine UPDATE/DELETE/INSERT semantics.
    Merge,
}

impl RowLevelCommand {
    fn validates_conflicting_delete_files(self) -> bool {
        matches!(self, Self::Update | Self::Merge)
    }
}

/// Validation context for one SQL row-level statement in a row delta commit.
#[derive(Debug, Clone)]
pub struct RowDeltaValidation {
    /// Command type whose Iceberg validation rules should be applied.
    pub command: RowLevelCommand,
    /// Snapshot that was used to plan/scan the affected rows.
    pub starting_snapshot_id: Option<i64>,
    /// Predicate used for conflict detection against concurrently added files.
    pub conflict_detection_filter: Predicate,
    /// Data files whose row positions are referenced by newly added delete files.
    pub referenced_data_files: BTreeSet<String>,
    /// Isolation level for this statement.
    pub isolation_level: IsolationLevel,
}

impl RowDeltaValidation {
    /// Creates validation for a row-level statement.
    pub fn new(
        command: RowLevelCommand,
        conflict_detection_filter: Predicate,
        isolation_level: IsolationLevel,
    ) -> Self {
        Self {
            command,
            starting_snapshot_id: None,
            conflict_detection_filter,
            referenced_data_files: BTreeSet::new(),
            isolation_level,
        }
    }

    /// Sets the snapshot that was used to scan the affected rows.
    #[must_use]
    pub fn with_starting_snapshot_id(
        mut self,
        starting_snapshot_id: Option<i64>,
    ) -> Self {
        self.starting_snapshot_id = starting_snapshot_id;
        self
    }

    /// Adds a referenced data file path.
    pub fn add_referenced_data_file(
        &mut self,
        file_path: impl Into<String>,
    ) -> &mut Self {
        self.referenced_data_files.insert(file_path.into());
        self
    }

    /// Sets the complete referenced data file set.
    #[must_use]
    pub fn with_referenced_data_files<I, S>(mut self, files: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.referenced_data_files = files.into_iter().map(Into::into).collect();
        self
    }
}

/// Transaction action that commits a [`SnapshotDelta`] with row-delta conflict validation.
pub struct RowDeltaAction {
    delta: Arc<SnapshotDelta>,
    truncate_base: bool,
    validations: Vec<RowDeltaValidation>,
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
}

impl RowDeltaAction {
    pub(crate) fn new(delta: Arc<SnapshotDelta>) -> Self {
        Self {
            delta,
            truncate_base: false,
            validations: Vec::new(),
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

    /// Adds validation for one row-level SQL statement.
    #[must_use]
    pub fn add_validation(mut self, validation: RowDeltaValidation) -> Self {
        self.validations.push(validation);
        self
    }

    /// Adds validations for multiple row-level SQL statements.
    #[must_use]
    pub fn add_validations<I>(mut self, validations: I) -> Self
    where
        I: IntoIterator<Item = RowDeltaValidation>,
    {
        self.validations.extend(validations);
        self
    }

    /// Set whether to check duplicate file paths against the current snapshot.
    #[must_use]
    pub fn with_check_duplicate(mut self, value: bool) -> Self {
        self.check_duplicate = value;
        self
    }

    /// Set commit UUID for generated metadata file names.
    #[must_use]
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files.
    #[must_use]
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot summary properties.
    #[must_use]
    pub fn set_snapshot_properties(
        mut self,
        snapshot_properties: HashMap<String, String>,
    ) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }
}

impl TransactionAction for RowDeltaAction {
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

        RowDeltaValidator::new(table, &plan).validate_all(&self.validations)?;
        producer.commit(plan)
    }
}

struct RowDeltaValidator<'a> {
    table: &'a Table,
    plan: &'a DeltaPlan,
}

impl<'a> RowDeltaValidator<'a> {
    fn new(table: &'a Table, plan: &'a DeltaPlan) -> Self {
        Self { table, plan }
    }

    fn validate_all(&self, validations: &[RowDeltaValidation]) -> Result<()> {
        self.validate_file_and_position_delete_conflicts()?;
        let live_data_file_paths = self.live_data_file_paths_for(validations)?;

        for validation in validations {
            self.validate(validation, live_data_file_paths.as_ref())?;
        }

        Ok(())
    }

    fn validate(
        &self,
        validation: &RowDeltaValidation,
        live_data_file_paths: Option<&BTreeSet<String>>,
    ) -> Result<()> {
        let snapshot_ids =
            self.snapshot_ids_after(validation.starting_snapshot_id)?;
        self.validate_referenced_data_files_exist(validation, live_data_file_paths)?;

        if validation.command.validates_conflicting_delete_files() {
            self.validate_no_conflicting_delete_files(validation, &snapshot_ids)?;
        }

        self.validate_no_concurrent_deletion_vectors(validation, &snapshot_ids)?;

        if validation.isolation_level == IsolationLevel::Serializable {
            self.validate_no_conflicting_data_files(validation, &snapshot_ids)?;
        }

        Ok(())
    }

    fn validate_no_concurrent_deletion_vectors(
        &self,
        validation: &RowDeltaValidation,
        snapshot_ids: &BTreeSet<i64>,
    ) -> Result<()> {
        if snapshot_ids.is_empty()
            || validation.referenced_data_files.is_empty()
            || !self.plan.position_delete_files.iter().any(|file| {
                file.content_type() == DataContentType::PositionDeletes
                    && file.file_format() == DataFileFormat::Puffin
            })
        {
            return Ok(());
        }

        let conflicts = self.find_conflicting_entries(
            ManifestContentType::Deletes,
            snapshot_ids,
            |entry| {
                if !Self::is_added_in_history(entry, snapshot_ids)
                    || entry.content_type() != DataContentType::PositionDeletes
                    || entry.data_file().file_format() != DataFileFormat::Puffin
                {
                    return Ok(false);
                }

                Ok(entry.data_file().referenced_data_file_path().is_some_and(
                    |path| validation.referenced_data_files.contains(path),
                ))
            },
        )?;

        if conflicts.is_empty() {
            return Ok(());
        }

        Err(self.conflict_error(format!(
            "row delta conflicts with deletion vectors added after scan snapshot: {} conflicting deletion vectors",
            conflicts.len()
        )))
    }

    fn live_data_file_paths_for(
        &self,
        validations: &[RowDeltaValidation],
    ) -> Result<Option<BTreeSet<String>>> {
        if validations
            .iter()
            .any(|validation| !validation.referenced_data_files.is_empty())
        {
            Ok(Some(self.live_data_file_paths()?))
        } else {
            Ok(None)
        }
    }

    fn validate_file_and_position_delete_conflicts(&self) -> Result<()> {
        let conflicts: Vec<&str> = self
            .plan
            .referenced_data_files
            .iter()
            .filter_map(|path| {
                self.plan
                    .removals
                    .has_removed_data_path(path)
                    .then_some(path.as_str())
            })
            .collect();

        if conflicts.is_empty() {
            return Ok(());
        }

        Err(self.validation_error(format!(
            "row delta cannot both remove data files and add position deletes referencing them: {} conflicting data files",
            conflicts.len()
        )))
    }

    fn validate_referenced_data_files_exist(
        &self,
        validation: &RowDeltaValidation,
        live_data_file_paths: Option<&BTreeSet<String>>,
    ) -> Result<()> {
        if validation.referenced_data_files.is_empty() {
            return Ok(());
        }

        let live_files = live_data_file_paths.ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "live data file set was not prepared for row delta validation",
            )
        })?;
        let missing: Vec<&str> = validation
            .referenced_data_files
            .iter()
            .filter_map(|path| (!live_files.contains(path)).then_some(path.as_str()))
            .collect();

        if missing.is_empty() {
            return Ok(());
        }

        Err(self.conflict_error(format!(
            "row delta referenced data files are no longer live: {} missing data files",
            missing.len()
        )))
    }

    fn validate_no_conflicting_data_files(
        &self,
        validation: &RowDeltaValidation,
        snapshot_ids: &BTreeSet<i64>,
    ) -> Result<()> {
        if snapshot_ids.is_empty() {
            return Ok(());
        }

        let filter = self.bind_conflict_filter(validation)?;
        let conflicts = self.find_conflicting_entries(
            ManifestContentType::Data,
            snapshot_ids,
            |entry| {
                if !Self::is_added_in_history(entry, snapshot_ids)
                    || entry.content_type() != DataContentType::Data
                {
                    return Ok(false);
                }

                self.file_may_match(&filter, entry)
            },
        )?;

        if conflicts.is_empty() {
            return Ok(());
        }

        Err(self.conflict_error(format!(
            "row delta conflicts with data files added after scan snapshot: {} conflicting data files",
            conflicts.len()
        )))
    }

    fn validate_no_conflicting_delete_files(
        &self,
        validation: &RowDeltaValidation,
        snapshot_ids: &BTreeSet<i64>,
    ) -> Result<()> {
        if snapshot_ids.is_empty() {
            return Ok(());
        }

        let filter = self.bind_conflict_filter(validation)?;
        let conflicts = self.find_conflicting_entries(
            ManifestContentType::Deletes,
            snapshot_ids,
            |entry| {
                if !Self::is_added_in_history(entry, snapshot_ids) {
                    return Ok(false);
                }

                self.delete_file_conflicts(validation, &filter, entry)
            },
        )?;

        if conflicts.is_empty() {
            return Ok(());
        }

        Err(self.conflict_error(format!(
            "row delta conflicts with delete files added after scan snapshot: {} conflicting delete files",
            conflicts.len()
        )))
    }

    fn delete_file_conflicts(
        &self,
        validation: &RowDeltaValidation,
        filter: &BoundPredicate,
        entry: &ManifestEntry,
    ) -> Result<bool> {
        match entry.content_type() {
            DataContentType::PositionDeletes => {
                // A position delete only conflicts when it removes rows from a
                // data file this row delta references or removes. Such files
                // carry statistics for `file_path`/`pos` only, so the
                // data-column conflict filter cannot refine the decision (it
                // would treat every concurrent position delete as a possible
                // match) — the target-path overlap is the entire test.
                match entry.data_file().position_delete_target_data_file_path() {
                    Some(target) => {
                        Ok(validation.referenced_data_files.contains(target)
                            || self.plan.removals.has_removed_data_path(target))
                    }
                    // A position delete without a referenced data file can
                    // target rows in any data file, so it must be treated
                    // conservatively as a conflict.
                    None => Ok(true),
                }
            }
            DataContentType::EqualityDeletes => self.file_may_match(filter, entry),
            DataContentType::Data => Ok(false),
        }
    }

    /// Find manifest entries added within the snapshot interval that match a
    /// caller-supplied predicate. Uses two levels of early filtering to
    /// minimize IO:
    ///
    /// 1. **Manifest-level**: only load manifests whose `added_snapshot_id` is
    ///    in the interval — inherited manifests cannot contain newly-added
    ///    entries in this interval (matching Java Iceberg's `validationHistory`).
    /// 2. **ObjectCache**: manifest lists and manifests are loaded through the
    ///    table's shared cache, so repeated encounters of the same object
    ///    across snapshots or across multiple validations in a single commit
    ///    hit memory instead of storage.
    fn find_conflicting_entries<F>(
        &self,
        content: ManifestContentType,
        snapshot_ids: &BTreeSet<i64>,
        mut predicate: F,
    ) -> Result<Vec<String>>
    where
        F: FnMut(&ManifestEntry) -> Result<bool>,
    {
        let cache = self.table.object_cache();
        let metadata_ref = self.table.metadata_ref();
        let mut conflicts = Vec::new();

        for snapshot_id in snapshot_ids {
            let snapshot_ref = self.snapshot_ref(*snapshot_id)?;
            let manifest_list =
                cache.get_manifest_list(snapshot_ref, &metadata_ref)?;

            for manifest_file in manifest_list.entries() {
                if manifest_file.content != content
                    || !manifest_file.has_added_files()
                {
                    continue;
                }
                // Manifest-level early filter: a manifest first written before
                // the validation interval cannot contain entries whose
                // snapshot_id falls within [starting..current]. Loading and
                // scanning it would yield zero matches from is_added_in_history.
                if !snapshot_ids.contains(&manifest_file.added_snapshot_id) {
                    continue;
                }

                let manifest = cache.get_manifest(manifest_file)?;
                for entry in manifest.entries() {
                    if predicate(entry)? {
                        conflicts.push(entry.file_path().to_owned());
                    }
                }
            }
        }

        conflicts.sort_unstable();
        conflicts.dedup();
        Ok(conflicts)
    }

    fn live_data_file_paths(&self) -> Result<BTreeSet<String>> {
        // Validation runs against the table state produced by this commit, not
        // only the currently committed snapshot. Position deletes may legally
        // reference data files appended earlier in the same transaction.
        // `DeltaPlan` is already resolved, so add-then-remove files are absent.
        let mut paths: BTreeSet<String> = self
            .plan
            .added_data_files
            .iter()
            .map(|file| file.file_path().to_owned())
            .collect();
        if self.plan.removals.truncates_base() {
            return Ok(paths);
        }
        let Some(current_snapshot) = self.table.metadata().current_snapshot() else {
            return Ok(paths);
        };

        let cache = self.table.object_cache();
        let metadata_ref = self.table.metadata_ref();
        let manifest_list =
            cache.get_manifest_list(current_snapshot, &metadata_ref)?;

        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data
                || (!manifest_file.has_added_files()
                    && !manifest_file.has_existing_files())
            {
                continue;
            }

            let manifest = cache.get_manifest(manifest_file)?;
            for entry in manifest.entries() {
                if entry.is_alive() && entry.content_type() == DataContentType::Data {
                    paths.insert(entry.file_path().to_owned());
                }
            }
        }

        Ok(paths)
    }

    fn snapshot_ids_after(
        &self,
        starting_snapshot_id: Option<i64>,
    ) -> Result<BTreeSet<i64>> {
        let mut after_start = BTreeSet::new();
        let Some(current_snapshot) = self.table.metadata().current_snapshot() else {
            return match starting_snapshot_id {
                None => Ok(after_start),
                Some(_) => Err(self.conflict_error(
                    "row delta has a scan snapshot but table has no current snapshot",
                )),
            };
        };

        let mut cursor = Some(current_snapshot.snapshot_id());
        while let Some(snapshot_id) = cursor {
            if starting_snapshot_id == Some(snapshot_id) {
                return Ok(after_start);
            }

            let snapshot = self.snapshot_ref(snapshot_id)?;
            after_start.insert(snapshot_id);
            cursor = snapshot.parent_snapshot_id();
        }

        // `None` means the statement scanned a table before it had any
        // snapshot. Every snapshot now present was therefore committed after
        // the scan and belongs to the validation interval.
        let Some(starting_snapshot_id) = starting_snapshot_id else {
            return Ok(after_start);
        };

        Err(self.conflict_error(format!(
            "row delta scan snapshot {starting_snapshot_id} is not an ancestor of current snapshot {}",
            current_snapshot.snapshot_id()
        )))
    }

    fn snapshot_ref(&self, snapshot_id: i64) -> Result<&'a SnapshotRef> {
        self.table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| {
                self.conflict_error(format!(
                    "snapshot {snapshot_id} referenced by row delta validation does not exist"
                ))
            })
    }

    fn bind_conflict_filter(
        &self,
        validation: &RowDeltaValidation,
    ) -> Result<BoundPredicate> {
        validation
            .conflict_detection_filter
            .bind(self.table.metadata().current_schema().clone(), true)
    }

    fn file_may_match(
        &self,
        filter: &BoundPredicate,
        entry: &ManifestEntry,
    ) -> Result<bool> {
        InclusiveMetricsEvaluator::eval(filter, entry.data_file(), true)
    }

    fn is_added_in_history(
        entry: &ManifestEntry,
        snapshot_ids: &BTreeSet<i64>,
    ) -> bool {
        entry.status() == ManifestStatus::Added
            && entry
                .snapshot_id()
                .is_some_and(|snapshot_id| snapshot_ids.contains(&snapshot_id))
    }

    fn validation_error(&self, message: impl Into<String>) -> Error {
        Error::new(ErrorKind::PreconditionFailed, message)
    }

    fn conflict_error(&self, message: impl Into<String>) -> Error {
        Error::new(ErrorKind::DataConflict, message)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataFile, DataFileBuilder, DataFileFormat, FormatVersion, Literal, Struct,
        TableMetadata,
    };
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, TableCreation, TableIdent};

    use super::*;

    #[test]
    fn data_conflict_requires_rebuilding_the_outer_transaction() {
        let table = make_v2_minimal_table();
        let plan = DeltaPlan::default();
        let validator = RowDeltaValidator::new(&table, &plan);

        let error = validator.conflict_error("concurrent data change");

        assert_eq!(error.kind(), ErrorKind::DataConflict);
        assert!(!error.retryable());
    }

    #[test]
    fn live_files_include_transaction_local_adds() {
        let table = make_v2_minimal_table();
        let mut plan = DeltaPlan::default();
        plan.added_data_files.push(data_file("test/staged.parquet"));
        let validator = RowDeltaValidator::new(&table, &plan);

        let paths = validator.live_data_file_paths().unwrap();

        assert!(paths.contains("test/staged.parquet"));
    }

    #[test]
    fn no_starting_snapshot_includes_entire_current_history() {
        let catalog = new_memory_catalog();
        let table = make_v2_table_in_catalog(&catalog);
        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files([data_file("test/concurrent.parquet")])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).unwrap();
        let plan = DeltaPlan::default();
        let validator = RowDeltaValidator::new(&table, &plan);

        let snapshot_ids = validator.snapshot_ids_after(None).unwrap();
        let expected: BTreeSet<i64> = table
            .metadata()
            .snapshots()
            .map(|snapshot| snapshot.snapshot_id())
            .collect();

        assert_eq!(snapshot_ids, expected);
        assert!(!snapshot_ids.is_empty());
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
        let mut builder = DataFileBuilder::default();
        builder
            .content(DataContentType::Data)
            .file_path(path.to_owned())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(300))]));
        builder.build().unwrap()
    }
}
