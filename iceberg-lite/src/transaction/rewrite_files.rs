//! Iceberg `RewriteFiles` transaction action.
//!
//! Unlike [`super::SnapshotDeltaAction`], this action always produces a
//! `replace` snapshot, preserves the starting sequence number for replacement
//! data, and validates fixed rewrite inputs against the current snapshot each
//! time a catalog retry replays the action.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use uuid::Uuid;

use crate::overlay::{DeleteFileIdentity, SnapshotDelta};
use crate::spec::{DataContentType, DataFile, ManifestContentType, Operation};
use crate::table::Table;
use crate::transaction::snapshot_delta::{
    DeltaCommitSemantics, DeltaPlan, DeltaSnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result};

/// A fixed-input file rewrite planned from one starting snapshot.
pub struct RewriteFilesAction {
    starting_snapshot_id: i64,
    starting_sequence_number: i64,
    rewritten_data_files: Vec<DataFile>,
    replacement_data_files: Vec<DataFile>,
    rewritten_delete_files: Vec<DataFile>,
    added_data_files_have_row_ids: bool,
    commit_uuid: Option<Uuid>,
}

impl RewriteFilesAction {
    pub(crate) fn new(
        starting_snapshot_id: i64,
        starting_sequence_number: i64,
    ) -> Self {
        Self {
            starting_snapshot_id,
            starting_sequence_number,
            rewritten_data_files: Vec::new(),
            replacement_data_files: Vec::new(),
            rewritten_delete_files: Vec::new(),
            added_data_files_have_row_ids: false,
            commit_uuid: None,
        }
    }

    pub fn rewrite_data_files(
        mut self,
        rewritten: impl IntoIterator<Item = DataFile>,
        replacements: impl IntoIterator<Item = DataFile>,
    ) -> Self {
        self.rewritten_data_files.extend(rewritten);
        self.replacement_data_files.extend(replacements);
        self
    }

    /// Remove exact delete entries whose effects were materialized into
    /// replacement rows. Callers rewriting deletion-vector-protected data must
    /// supply those vectors here; commit validation rejects an incomplete set.
    pub fn rewrite_delete_files(
        mut self,
        rewritten: impl IntoIterator<Item = DataFile>,
    ) -> Self {
        self.rewritten_delete_files.extend(rewritten);
        self
    }

    /// Declare that every replacement row carries a materialized, non-null
    /// `_row_id`. This prevents a format-v3 replace snapshot from reserving a
    /// fresh inherited range for rows whose identities were preserved.
    pub fn with_preassigned_row_ids(mut self) -> Self {
        self.added_data_files_have_row_ids = true;
        self
    }

    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    fn validate_definition(&self) -> Result<()> {
        if self.starting_sequence_number < 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "RewriteFiles starting sequence number must be non-negative",
            ));
        }
        if self.rewritten_data_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "RewriteFiles requires at least one rewritten data file",
            ));
        }
        let mut paths = HashSet::new();
        for file in &self.rewritten_data_files {
            if file.content_type() != DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "RewriteFiles inputs must be data files",
                ));
            }
            if !paths.insert(file.file_path()) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("duplicate RewriteFiles input: {}", file.file_path()),
                ));
            }
        }
        let mut replacement_paths = HashSet::new();
        for file in &self.replacement_data_files {
            if file.content_type() != DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "RewriteFiles replacements must be data files",
                ));
            }
            if paths.contains(file.file_path()) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "RewriteFiles replacement reuses input path: {}",
                        file.file_path()
                    ),
                ));
            }
            if !replacement_paths.insert(file.file_path()) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "duplicate RewriteFiles replacement: {}",
                        file.file_path()
                    ),
                ));
            }
        }
        let mut delete_identities = HashSet::new();
        for file in &self.rewritten_delete_files {
            if file.content_type() == DataContentType::Data {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "RewriteFiles delete replacements must be delete files",
                ));
            }
            if !delete_identities.insert(DeleteFileIdentity::from_data_file(file)) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("duplicate RewriteFiles delete input: {}", file.file_path()),
                ));
            }
        }
        Ok(())
    }

    fn validate_current_table(&self, table: &Table) -> Result<()> {
        let metadata = table.metadata();
        let starting_snapshot = metadata
            .snapshot_by_id(self.starting_snapshot_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataConflict,
                    format!(
                        "RewriteFiles starting snapshot {} is no longer present",
                        self.starting_snapshot_id
                    ),
                )
            })?;
        if starting_snapshot.sequence_number() != self.starting_sequence_number {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "RewriteFiles starting sequence mismatch: snapshot {} has {}, planned {}",
                    self.starting_snapshot_id,
                    starting_snapshot.sequence_number(),
                    self.starting_sequence_number
                ),
            ));
        }

        let input_paths: HashSet<&str> = self
            .rewritten_data_files
            .iter()
            .map(DataFile::file_path)
            .collect();
        let rewritten_delete_files: HashSet<DeleteFileIdentity> = self
            .rewritten_delete_files
            .iter()
            .map(DeleteFileIdentity::from_data_file)
            .collect();
        let current_snapshot = metadata.current_snapshot().ok_or_else(|| {
            Error::new(
                ErrorKind::DataConflict,
                "RewriteFiles inputs are not live because the table has no current snapshot",
            )
        })?;
        let manifest_list =
            current_snapshot.load_manifest_list(table.file_io(), &table.metadata_ref())?;
        let mut live_inputs = HashSet::new();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io())?;
            for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
                match manifest_file.content {
                    ManifestContentType::Data => {
                        if input_paths.contains(entry.file_path()) {
                            live_inputs.insert(entry.file_path());
                        }
                    }
                    ManifestContentType::Deletes => {
                        let sequence_number = entry.sequence_number().ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "delete manifest entry {} has no sequence number",
                                    entry.file_path()
                                ),
                            )
                        })?;
                        if sequence_number > self.starting_sequence_number
                            && Self::delete_may_apply(
                                entry.data_file(),
                                &input_paths,
                                &self.rewritten_data_files,
                            )
                        {
                            return Err(Error::new(
                                ErrorKind::DataConflict,
                                format!(
                                    "a row-level delete added after snapshot {} may apply to a rewritten input",
                                    self.starting_snapshot_id
                                ),
                            ));
                        }
                        if entry.data_file().is_deletion_vector()
                            && entry
                                .data_file()
                                .referenced_data_file_path()
                                .is_some_and(|path| input_paths.contains(path))
                            && !rewritten_delete_files.contains(
                                &DeleteFileIdentity::from_data_file(entry.data_file()),
                            )
                        {
                            return Err(Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "RewriteFiles must remove deletion vector {} whose rows were materialized",
                                    entry.file_path()
                                ),
                            ));
                        }
                    }
                }
            }
        }

        if live_inputs.len() != input_paths.len() {
            let mut missing: Vec<&str> = input_paths
                .into_iter()
                .filter(|path| !live_inputs.contains(path))
                .collect();
            missing.sort_unstable();
            return Err(Error::new(
                ErrorKind::DataConflict,
                format!(
                    "RewriteFiles inputs are no longer live in the current snapshot: {}",
                    missing.join(", ")
                ),
            ));
        }
        Ok(())
    }

    fn delete_may_apply(
        delete_file: &DataFile,
        input_paths: &HashSet<&str>,
        input_files: &[DataFile],
    ) -> bool {
        if let Some(referenced_path) = delete_file.referenced_data_file_path() {
            return input_paths.contains(referenced_path);
        }

        // Equality deletes and multi-target position deletes apply within a
        // compatible spec/partition. Conservatively treating every input in
        // that partition as affected avoids resurrecting rows without reading
        // delete payloads during validation.
        input_files.iter().any(|input| {
            input.partition_spec_id == delete_file.partition_spec_id
                && input.partition() == delete_file.partition()
        })
    }

    fn delta_plan(&self) -> Result<DeltaPlan> {
        let mut delta = SnapshotDelta::new();
        for file in &self.replacement_data_files {
            delta.add_data_file(file.clone())?;
        }
        for file in &self.rewritten_data_files {
            delta.remove_data_file(file.file_path())?;
        }
        for file in &self.rewritten_delete_files {
            delta.remove_delete_file(DeleteFileIdentity::from_data_file(file))?;
        }
        Ok(DeltaPlan::from_delta_with_truncate(&delta, false))
    }
}

impl TransactionAction for RewriteFilesAction {
    fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        self.validate_definition()?;
        self.validate_current_table(table)?;
        let plan = self.delta_plan()?;
        let producer = DeltaSnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            None,
            HashMap::new(),
        );
        producer.validate_plan(&plan)?;
        producer.validate_duplicate_files(&plan.added_file_paths)?;
        producer.commit_with_semantics(
            plan,
            DeltaCommitSemantics {
                operation: Operation::Replace,
                added_sequence_number: self.starting_sequence_number,
                added_data_files_have_row_ids: self.added_data_files_have_row_ids,
            },
        )
    }
}
