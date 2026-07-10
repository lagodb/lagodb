//! Transaction-local snapshot overlay support.
//!
//! [`SnapshotDelta`] is an in-memory operation log that can be layered over a
//! committed Iceberg snapshot for statement-local reads. It deliberately keeps
//! storage side effects out of the read path: callers record file-level changes
//! here, scan planning merges the delta with committed manifests, and commit
//! code can later materialize the same logical changes through normal Iceberg
//! metadata writes.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::scan::DeleteFileContext;
use crate::spec::{
    DataContentType, DataFile, ManifestContentType, ManifestEntry, ManifestEntryRef,
    ManifestStatus,
};
use crate::{Error, ErrorKind, Result};

/// Stable identity of a delete manifest entry.
///
/// Puffin deletion vectors can share one physical file path, so delete-file
/// removal must include the blob offset and size when they are present.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DeleteFileIdentity {
    file_path: String,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
}

impl DeleteFileIdentity {
    /// Creates a delete-file identity.
    #[must_use]
    pub fn new(
        file_path: impl Into<String>,
        content_offset: Option<i64>,
        content_size_in_bytes: Option<i64>,
    ) -> Self {
        Self {
            file_path: file_path.into(),
            content_offset,
            content_size_in_bytes,
        }
    }

    /// Creates a delete-file identity from manifest data-file metadata.
    #[must_use]
    pub fn from_data_file(data_file: &DataFile) -> Self {
        Self::new(
            data_file.file_path(),
            data_file.content_offset(),
            data_file.content_size_in_bytes(),
        )
    }

    /// Returns the delete file path.
    #[must_use]
    pub fn file_path(&self) -> &str {
        &self.file_path
    }

    /// Returns the Puffin blob offset, if present.
    #[must_use]
    pub fn content_offset(&self) -> Option<i64> {
        self.content_offset
    }

    /// Returns the Puffin blob size, if present.
    #[must_use]
    pub fn content_size_in_bytes(&self) -> Option<i64> {
        self.content_size_in_bytes
    }

    fn cmp_data_file(&self, data_file: &DataFile) -> Ordering {
        self.file_path
            .as_str()
            .cmp(data_file.file_path())
            .then_with(|| self.content_offset.cmp(&data_file.content_offset()))
            .then_with(|| {
                self.content_size_in_bytes
                    .cmp(&data_file.content_size_in_bytes())
            })
    }
}

/// Owned net-effect view of manifest entries removed by a snapshot delta.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SnapshotDeltaRemovals {
    removed_data_paths: Vec<String>,
    removed_delete_files: Vec<DeleteFileIdentity>,
}

impl SnapshotDeltaRemovals {
    fn from_resolved(resolved: &ResolvedSnapshotDelta<'_>) -> Self {
        // Resolved removals are sorted by the builder; preserve that order so
        // lookups can use binary search without per-entry allocations.
        Self {
            removed_data_paths: resolved
                .removed_data_paths
                .iter()
                .map(|path| (*path).to_owned())
                .collect(),
            removed_delete_files: resolved
                .removed_delete_files
                .iter()
                .map(|identity| (*identity).clone())
                .collect(),
        }
    }

    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.removed_data_paths.is_empty() && self.removed_delete_files.is_empty()
    }

    pub(crate) fn removed_data_paths(&self) -> &[String] {
        &self.removed_data_paths
    }

    pub(crate) fn removed_delete_files(&self) -> &[DeleteFileIdentity] {
        &self.removed_delete_files
    }
}

/// Lookup interface for manifest entries hidden by a resolved snapshot delta.
pub(crate) trait SnapshotDeltaRemovalLookup {
    /// Return true when this delta explicitly removes a committed data path.
    fn has_removed_data_path(&self, path: &str) -> bool;

    /// Return true when this delta explicitly removes this delete-file entry.
    ///
    /// This checks exact delete-file identity only. [`Self::removes_manifest_entry`]
    /// also handles deletion vectors made obsolete by removing their referenced
    /// data file.
    fn has_removed_delete_file(&self, data_file: &DataFile) -> bool;

    /// Return true when a committed manifest entry is hidden by this delta.
    fn removes_manifest_entry(
        &self,
        manifest_content: ManifestContentType,
        entry: &ManifestEntry,
    ) -> bool {
        match manifest_content {
            ManifestContentType::Data => {
                self.has_removed_data_path(entry.file_path())
            }
            ManifestContentType::Deletes => {
                let data_file = entry.data_file();
                self.has_removed_delete_file(data_file)
                    || (data_file.is_deletion_vector()
                        && data_file
                            .referenced_data_file_path()
                            .is_some_and(|path| self.has_removed_data_path(path)))
            }
        }
    }

    /// Return true when a committed manifest entry remains visible.
    fn retains_manifest_entry(
        &self,
        manifest_content: ManifestContentType,
        entry: &ManifestEntry,
    ) -> bool {
        !self.removes_manifest_entry(manifest_content, entry)
    }
}

impl SnapshotDeltaRemovalLookup for SnapshotDeltaRemovals {
    fn has_removed_data_path(&self, path: &str) -> bool {
        self.removed_data_paths
            .binary_search_by(|removed| removed.as_str().cmp(path))
            .is_ok()
    }

    fn has_removed_delete_file(&self, data_file: &DataFile) -> bool {
        self.removed_delete_files
            .binary_search_by(|identity| identity.cmp_data_file(data_file))
            .is_ok()
    }
}

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
    pub(crate) removed_delete_files: Vec<&'a DeleteFileIdentity>,
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
    pub(crate) fn removals(&self) -> SnapshotDeltaRemovals {
        SnapshotDeltaRemovals::from_resolved(self)
    }

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

impl SnapshotDeltaRemovalLookup for ResolvedSnapshotDelta<'_> {
    fn has_removed_data_path(&self, path: &str) -> bool {
        self.removed_data_paths
            .binary_search_by(|removed| (*removed).cmp(path))
            .is_ok()
    }

    fn has_removed_delete_file(&self, data_file: &DataFile) -> bool {
        self.removed_delete_files
            .binary_search_by(|identity| identity.cmp_data_file(data_file))
            .is_ok()
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
    added_delete_files: HashSet<DeleteFileIdentity>,
    removed_data_paths: HashSet<String>,
    removed_delete_files: HashSet<DeleteFileIdentity>,
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
    RemoveDeleteFile(DeleteFileIdentity),
}

impl Default for SnapshotDelta {
    fn default() -> Self {
        Self {
            ops: Vec::new(),
            added_data_file_paths: HashSet::new(),
            added_delete_files: HashSet::new(),
            removed_data_paths: HashSet::new(),
            removed_delete_files: HashSet::new(),
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
        self.ensure_can_add_data_path(data_file.file_path())?;
        self.push(DeltaOpKind::AddData(Arc::new(data_file)))?;
        Ok(self)
    }

    /// Append another delta's operation log to this delta, preserving operation
    /// order while re-validating path-level invariants.
    pub fn append_delta(&mut self, other: &SnapshotDelta) -> Result<&mut Self> {
        for op in &other.ops {
            match &op.kind {
                DeltaOpKind::AddData(data_file) => {
                    self.add_data_file(data_file.as_ref().clone())?;
                }
                DeltaOpKind::AddPositionDelete {
                    file,
                    referenced_data_files,
                } => {
                    self.add_position_delete_file(
                        file.as_ref().clone(),
                        referenced_data_files.iter().cloned(),
                    )?;
                }
                DeltaOpKind::RemoveDataFile(path) => {
                    self.remove_data_file(path.clone())?;
                }
                DeltaOpKind::RemoveDeleteFile(identity) => {
                    self.remove_delete_file(identity.clone())?;
                }
            }
        }
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

        self.ensure_can_add_delete_file(&delete_file)?;
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
        if self
            .added_delete_files
            .iter()
            .any(|identity| identity.file_path() == file_path)
        {
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

    /// Remove a delete manifest entry from the overlaid snapshot.
    pub fn remove_delete_file(
        &mut self,
        identity: DeleteFileIdentity,
    ) -> Result<&mut Self> {
        if identity.file_path().is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "snapshot delta remove delete requires a non-empty file path",
            ));
        }
        if self.removed_delete_files.contains(&identity) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "snapshot delta cannot remove an already removed delete file: {}",
                    identity.file_path()
                ),
            ));
        }
        self.push(DeltaOpKind::RemoveDeleteFile(identity))?;
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

    /// Return the currently visible transaction-local data files.
    #[must_use]
    pub fn added_data_files(&self) -> Vec<DataFile> {
        self.resolve()
            .added_data_files
            .into_iter()
            .map(|data_file| data_file.file.clone())
            .collect()
    }

    /// Return the currently visible transaction-local position delete files.
    ///
    /// Multi-target position delete files are expanded into one clone per
    /// referenced data file so callers can reason about file-scoped deletes
    /// without depending on the delta's internal representation.
    #[must_use]
    pub fn position_delete_files(&self) -> Vec<DataFile> {
        let resolved = self.resolve();
        let mut files = Vec::new();
        for delete_file in resolved.position_delete_files {
            for referenced_data_file in delete_file.referenced_data_files {
                let mut file = delete_file.file.clone();
                file.referenced_data_file = Some(referenced_data_file.to_owned());
                files.push(file);
            }
        }
        files
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

    fn ensure_can_add_data_path(&self, file_path: &str) -> Result<()> {
        if self.added_data_file_paths.contains(file_path) {
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

    fn ensure_can_add_delete_file(&self, delete_file: &DataFile) -> Result<()> {
        let identity = DeleteFileIdentity::from_data_file(delete_file);
        if self.added_delete_files.contains(&identity) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "duplicate delete file in snapshot delta: {}",
                    identity.file_path()
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
        let mut added_delete_files = HashSet::new();
        let mut removed_data_paths = HashSet::new();
        let mut removed_delete_files = HashSet::new();

        for op in &self.ops {
            Self::index_kind(
                &op.kind,
                &mut added_data_file_paths,
                &mut added_delete_files,
                &mut removed_data_paths,
                &mut removed_delete_files,
            );
        }

        self.added_data_file_paths = added_data_file_paths;
        self.added_delete_files = added_delete_files;
        self.removed_data_paths = removed_data_paths;
        self.removed_delete_files = removed_delete_files;
    }

    fn index_op(&mut self, kind: &DeltaOpKind) {
        Self::index_kind(
            kind,
            &mut self.added_data_file_paths,
            &mut self.added_delete_files,
            &mut self.removed_data_paths,
            &mut self.removed_delete_files,
        );
    }

    fn index_kind(
        kind: &DeltaOpKind,
        added_data_file_paths: &mut HashSet<String>,
        added_delete_files: &mut HashSet<DeleteFileIdentity>,
        removed_data_paths: &mut HashSet<String>,
        removed_delete_files: &mut HashSet<DeleteFileIdentity>,
    ) {
        match kind {
            DeltaOpKind::AddData(data_file) => {
                let file_path = data_file.file_path().to_owned();
                added_data_file_paths.insert(file_path);
            }
            DeltaOpKind::AddPositionDelete { file, .. } => {
                added_delete_files.insert(DeleteFileIdentity::from_data_file(file));
            }
            DeltaOpKind::RemoveDataFile(path) => {
                removed_data_paths.insert(path.clone());
            }
            DeltaOpKind::RemoveDeleteFile(identity) => {
                removed_delete_files.insert(identity.clone());
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
    removed_delete_files: HashSet<&'a DeleteFileIdentity>,
    added_file_paths: HashSet<&'a str>,
    position_delete_files: Vec<Option<PendingPositionDeleteResolution<'a>>>,
    position_delete_index: HashMap<DeleteFileIdentity, usize>,
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
                let identity = DeleteFileIdentity::from_data_file(file);
                let index = self.position_delete_files.len();
                self.position_delete_index.insert(identity, index);
                self.position_delete_files.push(Some(
                    PendingPositionDeleteResolution {
                        file: file.as_ref(),
                        referenced_data_files: referenced_data_files.as_slice(),
                        ordinal: op.ordinal,
                    },
                ));
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
            DeltaOpKind::RemoveDeleteFile(identity) => {
                if let Some(index) = self.position_delete_index.remove(identity) {
                    self.position_delete_files[index] = None;
                    self.added_file_paths.remove(identity.file_path());
                } else {
                    self.removed_delete_files.insert(identity);
                }
            }
        }
    }

    fn build(mut self) -> ResolvedSnapshotDelta<'a> {
        let added_data_files = self.added_data_files.into_iter().flatten().collect();
        let mut position_delete_files = Vec::new();
        for pending in self.position_delete_files.into_iter().flatten() {
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

        let mut removed_delete_files: Vec<&DeleteFileIdentity> =
            self.removed_delete_files.into_iter().collect();
        removed_delete_files.sort_unstable();

        let mut added_file_paths: Vec<&str> =
            self.added_file_paths.into_iter().collect();
        added_file_paths.sort_unstable();

        ResolvedSnapshotDelta {
            added_data_files,
            position_delete_files,
            removed_data_paths,
            removed_delete_files,
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

    #[test]
    fn resolved_removals_match_delete_file_identity() {
        let removed_delete =
            deletion_vector_file("delete-a.puffin", "data-a.parquet", 10, 100);
        let retained_delete =
            deletion_vector_file("delete-a.puffin", "data-a.parquet", 20, 100);
        let mut delta = SnapshotDelta::new();
        delta
            .remove_delete_file(DeleteFileIdentity::from_data_file(&removed_delete))
            .unwrap();
        let resolved = delta.resolve();

        assert!(resolved.has_removed_delete_file(&removed_delete));
        assert!(!resolved.has_removed_delete_file(&retained_delete));
    }

    #[test]
    fn resolved_removals_remove_dv_for_removed_data_file() {
        let removed_data = data_file("data-a.parquet");
        let dv_for_removed_data =
            deletion_vector_file("delete-a.puffin", "data-a.parquet", 10, 100);
        let dv_for_live_data =
            deletion_vector_file("delete-b.puffin", "data-b.parquet", 10, 100);
        let mut delta = SnapshotDelta::new();
        delta.remove_data_file(removed_data.file_path()).unwrap();
        let resolved = delta.resolve();

        assert!(resolved.removes_manifest_entry(
            ManifestContentType::Data,
            &manifest_entry(removed_data),
        ));
        assert!(resolved.removes_manifest_entry(
            ManifestContentType::Deletes,
            &manifest_entry(dv_for_removed_data),
        ));
        assert!(resolved.retains_manifest_entry(
            ManifestContentType::Deletes,
            &manifest_entry(dv_for_live_data),
        ));
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

    fn deletion_vector_file(
        path: &str,
        referenced_data_file: &str,
        offset: i64,
        size: i64,
    ) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_owned())
            .file_format(DataFileFormat::Puffin)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(100)
            .referenced_data_file(Some(referenced_data_file.to_owned()))
            .content_offset(Some(offset))
            .content_size_in_bytes(Some(size))
            .build()
            .unwrap()
    }

    fn manifest_entry(data_file: DataFile) -> ManifestEntry {
        ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .snapshot_id(1)
            .sequence_number(1)
            .file_sequence_number(1)
            .data_file(data_file)
            .build()
    }
}
