//! Transaction-local snapshot overlay support.
//!
//! [`SnapshotDelta`] is an in-memory operation log that can be layered over a
//! committed Iceberg snapshot for statement-local reads. It deliberately keeps
//! storage side effects out of the read path: callers record file-level changes
//! here, scan planning merges the delta with committed manifests, and commit
//! code can later materialize the same logical changes through normal Iceberg
//! metadata writes.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::scan::DeleteFileContext;
use crate::spec::{
    DataContentType, DataFile, ManifestEntry, ManifestEntryRef, ManifestStatus,
};
use crate::{Error, ErrorKind, Result};

/// File-level statistics exposed by a transaction-local [`SnapshotDelta`].
///
/// This is intentionally narrower than the delta operation log. Callers that
/// only need planner estimates can account for transaction-local files without
/// depending on operation ordering or concrete delta variants.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotDeltaStats {
    /// Records in newly added data files that remain visible in the delta.
    pub added_data_records: u64,
    /// Bytes in newly added data files that remain visible in the delta.
    pub added_data_file_bytes: u64,
    /// Bytes in newly added delete files that remain relevant in the delta.
    pub added_delete_file_bytes: u64,
    /// Committed data-file paths removed by this delta.
    pub removed_data_paths: Vec<String>,
}

/// Borrowed net-effect view of a [`SnapshotDelta`].
///
/// This is the single shared interpretation used by planner statistics and
/// commit materialization. It hides operation-log ordering details and exposes
/// only the files that remain effective after local add/remove cancellation.
pub(crate) struct ResolvedSnapshotDelta<'a> {
    pub(crate) added_data_files: Vec<ResolvedDataFile<'a>>,
    pub(crate) position_delete_files: Vec<ResolvedPositionDeleteFile<'a>>,
    pub(crate) removed_data_paths: Vec<&'a str>,
    pub(crate) added_file_paths: Vec<&'a str>,
}

pub(crate) struct ResolvedDataFile<'a> {
    pub(crate) file: &'a DataFile,
    ordinal: i64,
}

pub(crate) struct ResolvedPositionDeleteFile<'a> {
    pub(crate) file: &'a DataFile,
    pub(crate) referenced_data_files: Vec<&'a str>,
    ordinal: i64,
}

impl ResolvedSnapshotDelta<'_> {
    pub(crate) fn data_manifest_entries(
        &self,
        base_sequence_number: i64,
    ) -> Result<Vec<ManifestEntryRef>> {
        let mut entries = Vec::with_capacity(self.added_data_files.len());
        for data_file in &self.added_data_files {
            entries.push(SnapshotDelta::manifest_entry(
                data_file.file.clone(),
                base_sequence_number,
                data_file.ordinal,
            )?);
        }
        Ok(entries)
    }

    pub(crate) fn delete_file_contexts(
        &self,
        base_sequence_number: i64,
    ) -> Result<Vec<DeleteFileContext>> {
        let mut contexts = Vec::new();
        for delete_file in &self.position_delete_files {
            for referenced_data_file in &delete_file.referenced_data_files {
                let mut file = delete_file.file.clone();
                file.referenced_data_file = Some((*referenced_data_file).to_owned());
                let partition_spec_id = file.partition_spec_id;
                let entry = SnapshotDelta::manifest_entry(
                    file,
                    base_sequence_number,
                    delete_file.ordinal,
                )?;
                contexts.push(DeleteFileContext {
                    manifest_entry: entry,
                    partition_spec_id,
                });
            }
        }
        Ok(contexts)
    }
}

/// A rollback marker for [`SnapshotDelta`].
///
/// Savepoints should store a marker before adding operations and pass it back
/// to [`SnapshotDelta::truncate`] when the savepoint is aborted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotDeltaMarker {
    len: usize,
    next_ordinal: i64,
}

/// In-memory file-level changes layered on top of a committed snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotDelta {
    ops: Vec<DeltaOp>,
    added_data_file_paths: HashSet<String>,
    added_delete_file_paths: HashSet<String>,
    removed_data_paths: HashSet<String>,
    next_ordinal: i64,
}

#[derive(Debug, Clone)]
struct DeltaOp {
    ordinal: i64,
    kind: DeltaOpKind,
}

#[derive(Debug, Clone)]
enum DeltaOpKind {
    AddData(Arc<DataFile>),
    AddPositionDelete {
        file: Arc<DataFile>,
        referenced_data_files: Vec<String>,
    },
    RemoveDataFile(String),
}

impl Default for SnapshotDelta {
    fn default() -> Self {
        Self {
            ops: Vec::new(),
            added_data_file_paths: HashSet::new(),
            added_delete_file_paths: HashSet::new(),
            removed_data_paths: HashSet::new(),
            next_ordinal: 1,
        }
    }
}

impl SnapshotDelta {
    /// Create an empty delta.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Return true when no transaction-local file operation is recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Mark the current end of the operation log.
    #[must_use]
    pub fn mark(&self) -> SnapshotDeltaMarker {
        SnapshotDeltaMarker {
            len: self.ops.len(),
            next_ordinal: self.next_ordinal,
        }
    }

    /// Truncate this delta back to a previous marker.
    pub fn truncate(&mut self, marker: SnapshotDeltaMarker) {
        if marker.len <= self.ops.len() {
            self.ops.truncate(marker.len);
            self.next_ordinal = marker.next_ordinal;
            self.rebuild_path_indexes();
        }
    }

    /// Add a data file that is visible to later statement-local scans.
    pub fn add_data_file(&mut self, data_file: DataFile) -> Result<&mut Self> {
        if data_file.content_type() != DataContentType::Data {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "snapshot delta data file must have data content type",
            ));
        }
        self.ensure_can_add_path(data_file.file_path())?;
        self.push(DeltaOpKind::AddData(Arc::new(data_file)))?;
        Ok(self)
    }

    /// Add a position delete file with explicit referenced data-file mapping.
    pub fn add_position_delete_file<I, S>(
        &mut self,
        delete_file: DataFile,
        referenced_data_files: I,
    ) -> Result<&mut Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        if delete_file.content_type() != DataContentType::PositionDeletes {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "snapshot delta position delete must have position-delete content type",
            ));
        }

        let mut referenced_data_files: Vec<String> =
            referenced_data_files.into_iter().map(Into::into).collect();
        referenced_data_files.sort_unstable();
        referenced_data_files.dedup();
        if referenced_data_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "snapshot delta position delete requires referenced data files",
            ));
        }

        self.ensure_can_add_path(delete_file.file_path())?;
        self.push(DeltaOpKind::AddPositionDelete {
            file: Arc::new(delete_file),
            referenced_data_files,
        })?;
        Ok(self)
    }

    /// Remove a data file from the overlaid snapshot by path.
    pub fn remove_data_file(
        &mut self,
        file_path: impl Into<String>,
    ) -> Result<&mut Self> {
        let file_path = file_path.into();
        if file_path.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "snapshot delta remove requires a non-empty file path",
            ));
        }
        if self.added_delete_file_paths.contains(&file_path) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "snapshot delta cannot remove newly added delete file: {file_path}"
                ),
            ));
        }
        if self.removed_data_paths.contains(&file_path) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "snapshot delta cannot remove an already removed file: {file_path}"
                ),
            ));
        }
        self.push(DeltaOpKind::RemoveDataFile(file_path))?;
        Ok(self)
    }

    /// Return planner-facing file statistics for the current delta.
    #[must_use]
    pub fn stats(&self) -> SnapshotDeltaStats {
        let resolved = self.resolve();
        let mut stats = SnapshotDeltaStats::default();

        for data_file in resolved.added_data_files {
            stats.added_data_records = stats
                .added_data_records
                .saturating_add(data_file.file.record_count());
            stats.added_data_file_bytes = stats
                .added_data_file_bytes
                .saturating_add(data_file.file.file_size_in_bytes());
        }

        for delete_file in resolved.position_delete_files {
            stats.added_delete_file_bytes = stats
                .added_delete_file_bytes
                .saturating_add(delete_file.file.file_size_in_bytes());
        }

        stats.removed_data_paths = resolved
            .removed_data_paths
            .into_iter()
            .map(str::to_owned)
            .collect();
        stats
    }

    pub(crate) fn resolve(&self) -> ResolvedSnapshotDelta<'_> {
        let mut builder = ResolvedSnapshotDeltaBuilder::default();
        for op in &self.ops {
            builder.apply(op);
        }
        builder.build()
    }

    fn push(&mut self, kind: DeltaOpKind) -> Result<()> {
        let ordinal = self.next_ordinal;
        self.next_ordinal = self.next_ordinal.checked_add(1).ok_or_else(|| {
            Error::new(ErrorKind::DataInvalid, "snapshot delta ordinal overflow")
        })?;
        self.index_op(&kind);
        self.ops.push(DeltaOp { ordinal, kind });
        Ok(())
    }

    fn ensure_can_add_path(&self, file_path: &str) -> Result<()> {
        if self.added_data_file_paths.contains(file_path)
            || self.added_delete_file_paths.contains(file_path)
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("duplicate file path in snapshot delta: {file_path}"),
            ));
        }
        if self.removed_data_paths.contains(file_path) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "snapshot delta cannot re-add a removed file path: {file_path}"
                ),
            ));
        }
        Ok(())
    }

    /// Return true when the delta currently exposes a newly added data file
    /// with `file_path`.
    #[must_use]
    pub fn has_live_added_data_file_path(&self, file_path: &str) -> bool {
        self.added_data_file_paths.contains(file_path)
            && !self.removed_data_paths.contains(file_path)
    }

    /// Return true when the delta contains a remove operation for `file_path`.
    #[must_use]
    pub fn has_removed_data_path(&self, file_path: &str) -> bool {
        self.removed_data_paths.contains(file_path)
    }

    fn rebuild_path_indexes(&mut self) {
        let mut added_data_file_paths = HashSet::new();
        let mut added_delete_file_paths = HashSet::new();
        let mut removed_data_paths = HashSet::new();

        for op in &self.ops {
            Self::index_kind(
                &op.kind,
                &mut added_data_file_paths,
                &mut added_delete_file_paths,
                &mut removed_data_paths,
            );
        }

        self.added_data_file_paths = added_data_file_paths;
        self.added_delete_file_paths = added_delete_file_paths;
        self.removed_data_paths = removed_data_paths;
    }

    fn index_op(&mut self, kind: &DeltaOpKind) {
        Self::index_kind(
            kind,
            &mut self.added_data_file_paths,
            &mut self.added_delete_file_paths,
            &mut self.removed_data_paths,
        );
    }

    fn index_kind(
        kind: &DeltaOpKind,
        added_data_file_paths: &mut HashSet<String>,
        added_delete_file_paths: &mut HashSet<String>,
        removed_data_paths: &mut HashSet<String>,
    ) {
        match kind {
            DeltaOpKind::AddData(data_file) => {
                let file_path = data_file.file_path().to_owned();
                added_data_file_paths.insert(file_path);
            }
            DeltaOpKind::AddPositionDelete { file, .. } => {
                let file_path = file.file_path().to_owned();
                added_delete_file_paths.insert(file_path);
            }
            DeltaOpKind::RemoveDataFile(path) => {
                removed_data_paths.insert(path.clone());
            }
        }
    }

    fn manifest_entry(
        data_file: DataFile,
        base_sequence_number: i64,
        ordinal: i64,
    ) -> Result<ManifestEntryRef> {
        // TODO: Overlay scan planning clones Arc-backed delta files into owned
        // DataFiles here because ManifestEntry owns DataFile. If large deltas
        // become a hot repeated-read path, consider an Arc-backed manifest
        // entry representation so planning can keep these files shared.
        let sequence_number =
            base_sequence_number.checked_add(ordinal).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "snapshot delta sequence number overflow",
                )
            })?;

        Ok(Arc::new(
            ManifestEntry::builder()
                .status(ManifestStatus::Added)
                .sequence_number(sequence_number)
                .file_sequence_number(sequence_number)
                .data_file(data_file)
                .build(),
        ))
    }
}

#[derive(Default)]
struct ResolvedSnapshotDeltaBuilder<'a> {
    added_data_files: Vec<Option<ResolvedDataFile<'a>>>,
    added_data_index: HashMap<&'a str, usize>,
    canceled_added_paths: HashSet<&'a str>,
    removed_paths: HashSet<&'a str>,
    added_file_paths: HashSet<&'a str>,
    position_delete_files: Vec<PendingPositionDeleteResolution<'a>>,
}

struct PendingPositionDeleteResolution<'a> {
    file: &'a DataFile,
    referenced_data_files: &'a [String],
    ordinal: i64,
}

impl<'a> ResolvedSnapshotDeltaBuilder<'a> {
    fn apply(&mut self, op: &'a DeltaOp) {
        match &op.kind {
            DeltaOpKind::AddData(data_file) => {
                let path = data_file.file_path();
                let index = self.added_data_files.len();
                self.added_data_index.insert(path, index);
                self.added_file_paths.insert(path);
                self.added_data_files.push(Some(ResolvedDataFile {
                    file: data_file.as_ref(),
                    ordinal: op.ordinal,
                }));
            }
            DeltaOpKind::AddPositionDelete {
                file,
                referenced_data_files,
            } => {
                self.position_delete_files
                    .push(PendingPositionDeleteResolution {
                        file: file.as_ref(),
                        referenced_data_files: referenced_data_files.as_slice(),
                        ordinal: op.ordinal,
                    });
            }
            DeltaOpKind::RemoveDataFile(path) => {
                let path = path.as_str();
                if let Some(index) = self.added_data_index.remove(path) {
                    self.added_data_files[index] = None;
                    self.added_file_paths.remove(path);
                    self.canceled_added_paths.insert(path);
                } else {
                    self.removed_paths.insert(path);
                }
            }
        }
    }

    fn build(mut self) -> ResolvedSnapshotDelta<'a> {
        let added_data_files = self.added_data_files.into_iter().flatten().collect();
        let mut position_delete_files = Vec::new();
        for pending in self.position_delete_files {
            let mut referenced_data_files: Vec<&str> = pending
                .referenced_data_files
                .iter()
                .map(String::as_str)
                .filter(|path| {
                    !self.removed_paths.contains(*path)
                        && !self.canceled_added_paths.contains(*path)
                })
                .collect();
            referenced_data_files.sort_unstable();
            referenced_data_files.dedup();

            if !referenced_data_files.is_empty() {
                self.added_file_paths.insert(pending.file.file_path());
                position_delete_files.push(ResolvedPositionDeleteFile {
                    file: pending.file,
                    referenced_data_files,
                    ordinal: pending.ordinal,
                });
            }
        }

        let mut removed_data_paths: Vec<&str> =
            self.removed_paths.into_iter().collect();
        removed_data_paths.sort_unstable();

        let mut added_file_paths: Vec<&str> =
            self.added_file_paths.into_iter().collect();
        added_file_paths.sort_unstable();

        ResolvedSnapshotDelta {
            added_data_files,
            position_delete_files,
            removed_data_paths,
            added_file_paths,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DataFileBuilder, DataFileFormat, Struct};

    #[test]
    fn pending_sequences_start_after_base_snapshot() {
        let mut delta = SnapshotDelta::new();
        delta.add_data_file(data_file("file-a.parquet")).unwrap();

        let entries = delta.resolve().data_manifest_entries(10).unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence_number(), Some(11));
    }

    #[test]
    fn marker_truncate_restores_delta() {
        let mut delta = SnapshotDelta::new();
        delta.add_data_file(data_file("file-a.parquet")).unwrap();
        let marker = delta.mark();
        delta.add_data_file(data_file("file-b.parquet")).unwrap();

        delta.truncate(marker);

        let entries = delta.resolve().data_manifest_entries(0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].file_path(), "file-a.parquet");
    }

    #[test]
    fn marker_truncate_restores_path_indexes() {
        let mut delta = SnapshotDelta::new();
        delta.add_data_file(data_file("file-a.parquet")).unwrap();
        let marker = delta.mark();
        delta.add_data_file(data_file("file-b.parquet")).unwrap();

        delta.truncate(marker);

        delta.add_data_file(data_file("file-b.parquet")).unwrap();
    }

    #[test]
    fn readd_removed_path_is_rejected_at_stage_time() {
        let mut delta = SnapshotDelta::new();
        delta.add_data_file(data_file("file-a.parquet")).unwrap();
        delta.remove_data_file("file-a.parquet").unwrap();

        let err = delta
            .add_data_file(data_file("file-a.parquet"))
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn duplicate_remove_is_rejected_at_stage_time() {
        let mut delta = SnapshotDelta::new();
        delta.add_data_file(data_file("file-a.parquet")).unwrap();
        delta.remove_data_file("file-a.parquet").unwrap();

        let err = delta.remove_data_file("file-a.parquet").unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn remove_newly_added_delete_file_is_rejected_at_stage_time() {
        let mut delta = SnapshotDelta::new();
        delta
            .add_position_delete_file(
                position_delete_file("delete-a.parquet"),
                ["data-a.parquet"],
            )
            .unwrap();

        let err = delta.remove_data_file("delete-a.parquet").unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn stats_count_visible_added_data_and_delete_files() {
        let mut delta = SnapshotDelta::new();
        delta
            .add_data_file(data_file_with_metrics("file-a.parquet", 3, 300))
            .unwrap();
        delta
            .add_position_delete_file(
                data_file_with_content(
                    DataContentType::PositionDeletes,
                    "delete-a.parquet",
                    2,
                    40,
                ),
                ["file-a.parquet"],
            )
            .unwrap();

        let stats = delta.stats();

        assert_eq!(stats.added_data_records, 3);
        assert_eq!(stats.added_data_file_bytes, 300);
        assert_eq!(stats.added_delete_file_bytes, 40);
        assert!(stats.removed_data_paths.is_empty());
    }

    #[test]
    fn stats_ignore_added_data_canceled_by_remove() {
        let mut delta = SnapshotDelta::new();
        delta
            .add_data_file(data_file_with_metrics("file-a.parquet", 3, 300))
            .unwrap();
        delta.remove_data_file("file-a.parquet").unwrap();

        let stats = delta.stats();

        assert_eq!(stats.added_data_records, 0);
        assert_eq!(stats.added_data_file_bytes, 0);
        assert!(stats.removed_data_paths.is_empty());
    }

    #[test]
    fn stats_keep_committed_remove_paths() {
        let mut delta = SnapshotDelta::new();
        delta.remove_data_file("file-a.parquet").unwrap();

        let stats = delta.stats();

        assert_eq!(stats.removed_data_paths, vec!["file-a.parquet".to_owned()]);
    }

    #[test]
    fn stats_ignore_position_delete_when_all_references_are_removed() {
        let mut delta = SnapshotDelta::new();
        delta.remove_data_file("file-a.parquet").unwrap();
        delta
            .add_position_delete_file(
                data_file_with_content(
                    DataContentType::PositionDeletes,
                    "delete-a.parquet",
                    2,
                    40,
                ),
                ["file-a.parquet"],
            )
            .unwrap();

        let stats = delta.stats();

        assert_eq!(stats.added_delete_file_bytes, 0);
        assert_eq!(stats.removed_data_paths, vec!["file-a.parquet".to_owned()]);
    }

    fn data_file(path: &str) -> DataFile {
        data_file_with_metrics(path, 1, 100)
    }

    fn data_file_with_metrics(path: &str, record_count: u64, bytes: u64) -> DataFile {
        data_file_with_content(DataContentType::Data, path, record_count, bytes)
    }

    fn data_file_with_content(
        content: DataContentType,
        path: &str,
        record_count: u64,
        bytes: u64,
    ) -> DataFile {
        DataFileBuilder::default()
            .content(content)
            .file_path(path.to_owned())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(record_count)
            .file_size_in_bytes(bytes)
            .build()
            .unwrap()
    }

    fn position_delete_file(path: &str) -> DataFile {
        data_file_with_content(DataContentType::PositionDeletes, path, 1, 100)
    }
}
