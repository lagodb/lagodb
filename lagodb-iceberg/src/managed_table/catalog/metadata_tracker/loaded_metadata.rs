//! A committed metadata snapshot combined with transaction-local file changes.

use std::collections::HashSet;
use std::sync::Arc;

use iceberg_lite::io::FileIO;
use iceberg_lite::overlay::SnapshotDelta;
use iceberg_lite::spec::{
    DataContentType, ManifestContentType, SchemaRef, TableMetadata,
};
use iceberg_lite::{Error, ErrorKind};
use pgrx::pg_sys;

use crate::error::IcebergResult;
use crate::managed_table::options::MANAGED_ROW_GROUP_MAX_ROWS;

const TOTAL_RECORDS: &str = "total-records";
const TOTAL_DATA_FILES: &str = "total-data-files";
const TOTAL_FILES_SIZE: &str = "total-files-size";
const TOTAL_DELETE_FILES: &str = "total-delete-files";

/// Outcome of a metadata read for one table inside the current transaction.
///
/// Pairs the resolved metadata location with the parsed `TableMetadata` so
/// callers do not have to perform the same `read_from` against `FileIO`
/// twice. Returned by [`super::TxMetadata::current_table_metadata`] and
/// [`super::TxMetadata::begin_table_modify`].
#[derive(Debug)]
pub struct LoadedTableMetadata {
    pub location: String,
    pub maintenance_due_at: Option<pg_sys::TimestampTz>,
    pub metadata: TableMetadata,
    pub delta: Option<Arc<SnapshotDelta>>,
}

/// Metadata context and an explicitly selected data baseline for one read.
/// When the PG snapshot has no metadata row in the active storage generation,
/// or this transaction rebuilt that local generation, `loaded` supplies the
/// current definition/storage context, but `snapshot_id` remains `None`.
/// The transaction-local delta is independent of that committed baseline.
pub(crate) struct LoadedTableRead {
    pub(crate) loaded: LoadedTableMetadata,
    pub(crate) read_schema: SchemaRef,
    pub(crate) snapshot_id: Option<i64>,
}

impl LoadedTableRead {
    pub(crate) fn current(loaded: LoadedTableMetadata) -> Self {
        Self {
            read_schema: Arc::clone(loaded.metadata.current_schema()),
            snapshot_id: loaded.metadata.current_snapshot_id(),
            loaded,
        }
    }

    /// Storage bytes this read observes. Without a committed baseline, the
    /// snapshot summary in `loaded` describes rows outside this read.
    pub(crate) fn storage_bytes(&self, file_io: &FileIO) -> IcebergResult<u64> {
        let committed = match self.snapshot_id {
            Some(_) => self.loaded.committed_stats()?,
            None => (0, 0),
        };
        Ok(self.loaded.stats_with_delta(committed, file_io)?.1)
    }

    /// Estimated units that file and row-group statistics keep or skip
    /// whole: every data file holds at least one row group of at most
    /// [`MANAGED_ROW_GROUP_MAX_ROWS`] rows.
    pub(crate) fn pruning_units(&self) -> IcebergResult<f64> {
        let (mut rows, mut files) = match self.snapshot_id {
            Some(_) => self.loaded.committed_layout()?,
            None => (0, 0),
        };
        if let Some(delta) = self.loaded.delta.as_ref() {
            let stats = delta.stats();
            if stats.truncates_base {
                rows = 0;
                files = 0;
            } else if !stats.removed_data_paths.is_empty() {
                // The committed rows still count the removed files, which only
                // their manifests tell apart. One unit prices statistics
                // pruning as skipping nothing.
                return Ok(1.0);
            }
            // Added files are counted through their rows.
            rows = rows.saturating_add(stats.added_data_records);
        }
        let row_groups = rows.div_ceil(MANAGED_ROW_GROUP_MAX_ROWS as u64);
        Ok(files.max(row_groups).max(1) as f64)
    }
}

impl LoadedTableMetadata {
    /// Return planner-facing relation statistics for the committed metadata
    /// plus this transaction's staged delta.
    ///
    /// Mirrors Iceberg snapshot-summary totals closely enough for PostgreSQL
    /// planner sizing: data-file appends add rows and bytes, delete-file
    /// appends add only bytes, and data-file removes subtract the committed
    /// file's rows and bytes.
    pub(crate) fn relation_stats(
        &self,
        file_io: &FileIO,
    ) -> IcebergResult<(u64, u64)> {
        self.stats_with_delta(self.committed_stats()?, file_io)
    }

    /// Committed rows and data files.
    fn committed_layout(&self) -> IcebergResult<(u64, u64)> {
        Ok((
            self.summary_total(TOTAL_RECORDS)?,
            self.summary_total(TOTAL_DATA_FILES)?,
        ))
    }

    fn committed_stats(&self) -> IcebergResult<(u64, u64)> {
        Ok((
            self.summary_total(TOTAL_RECORDS)?,
            self.summary_total(TOTAL_FILES_SIZE)?,
        ))
    }

    fn stats_with_delta(
        &self,
        (mut rows, mut bytes): (u64, u64),
        file_io: &FileIO,
    ) -> IcebergResult<(u64, u64)> {
        let Some(delta) = self.delta.as_ref() else {
            return Ok((rows, bytes));
        };

        let delta_stats = delta.stats();
        if delta_stats.truncates_base {
            rows = 0;
            bytes = 0;
        }
        rows = rows.saturating_add(delta_stats.added_data_records);
        bytes = bytes
            .saturating_add(delta_stats.added_data_file_bytes)
            .saturating_add(delta_stats.added_delete_file_bytes);

        if !delta_stats.truncates_base {
            self.subtract_removed_data_file_stats(
                file_io,
                &delta_stats.removed_data_paths,
                &mut rows,
                &mut bytes,
            )?;
        }

        Ok((rows, bytes))
    }

    pub(crate) fn is_partitioned(&self) -> bool {
        !self.metadata.default_partition_spec().is_unpartitioned()
    }

    /// Whether the captured snapshot may contain row-level delete files.
    ///
    /// A false result is exact and allows the manifest `total-records` value to
    /// serve as the live-row estimate. A true result is deliberately
    /// conservative when transaction-local removal of the last committed
    /// delete file cannot be proven from summary counters alone.
    pub(crate) fn may_have_row_deletes(&self) -> IcebergResult<bool> {
        let delta_stats = self.delta.as_ref().map(|delta| delta.stats());
        let base_was_replaced = delta_stats
            .as_ref()
            .is_some_and(|stats| stats.truncates_base);
        let base_may_have_deletes =
            !base_was_replaced && self.summary_total(TOTAL_DELETE_FILES)? != 0;
        Ok(base_may_have_deletes
            || delta_stats.is_some_and(|stats| stats.added_delete_file_bytes != 0))
    }

    /// A total from the current snapshot's summary, or 0 without a snapshot.
    ///
    /// LagoDB is the only writer of managed metadata: tables are created in an
    /// empty location and every snapshot goes through iceberg-lite, which
    /// records the totals from the first snapshot on. iceberg-lite skips a
    /// total only when the previous snapshot lacks it or its arithmetic
    /// underflows, so a missing or malformed total means the metadata is
    /// inconsistent or was changed outside LagoDB.
    fn summary_total(&self, key: &str) -> IcebergResult<u64> {
        let Some(snapshot) = self.metadata.current_snapshot() else {
            return Ok(0);
        };
        snapshot
            .summary()
            .additional_properties
            .get(key)
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Iceberg snapshot summary has no valid \"{key}\" total"),
                )
                .into()
            })
    }

    fn subtract_removed_data_file_stats(
        &self,
        file_io: &FileIO,
        removed_paths: &[String],
        rows: &mut u64,
        bytes: &mut u64,
    ) -> IcebergResult<()> {
        if removed_paths.is_empty() {
            return Ok(());
        }

        let Some(snapshot) = self.metadata.current_snapshot() else {
            return Ok(());
        };

        let mut remaining: HashSet<&str> =
            removed_paths.iter().map(String::as_str).collect();
        let manifest_list = snapshot.load_manifest_list(file_io, &self.metadata)?;
        for manifest_file in manifest_list.entries() {
            if remaining.is_empty() {
                break;
            }
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }

            let manifest = manifest_file.load_manifest(file_io)?;
            for entry in manifest.entries() {
                if entry.is_alive()
                    && entry.content_type() == DataContentType::Data
                    && remaining.remove(entry.file_path())
                {
                    *rows = rows.saturating_sub(entry.record_count());
                    *bytes = bytes.saturating_sub(entry.file_size_in_bytes());
                }
            }
        }

        Ok(())
    }
}
