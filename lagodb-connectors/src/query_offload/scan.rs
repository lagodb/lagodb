//! Statement-bound Parquet scan: a lazily listed, statement-stable inventory
//! and the task plans read from it.

use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::Range;
use std::rc::Rc;
use std::sync::{Mutex, MutexGuard};

use arrow_schema::SchemaRef;
use lagodb_arrow::scan::WeightedBinPacker;
use lagodb_core::runtime_api::TableScanTaskMetrics;
use pgrx::pg_sys;
use serde::{Deserialize, Serialize};

use crate::format::FormatKind;
use crate::format::parquet::{ParquetBoundPredicate, ParquetFooter};
use crate::storage::{ResolvedStorageLocation, StorageInput};

use super::BackendLocal;
use super::columns::ScanColumns;
use super::error::Error;

/// Parallel split defaults, matching Iceberg's `read.split.*` defaults.
const TARGET_SPLIT_BYTES: NonZeroU64 = NonZeroU64::new(128 * 1024 * 1024).unwrap();
const SPLIT_LOOKBACK: NonZeroUsize = NonZeroUsize::new(10).unwrap();
const OPEN_FILE_COST: u64 = 4 * 1024 * 1024;

struct InventoryState {
    location: ResolvedStorageLocation,
    input: Option<Rc<StorageInput>>,
}

pub(super) struct BoundScan {
    relation_oid: pg_sys::Oid,
    effective_user_oid: pg_sys::Oid,
    columns: ScanColumns,
    inventory: Mutex<BackendLocal<InventoryState>>,
    exact_rows: Mutex<Option<u64>>,
}

impl BoundScan {
    pub(super) fn new(
        relation_oid: pg_sys::Oid,
        effective_user_oid: pg_sys::Oid,
        location: ResolvedStorageLocation,
        columns: ScanColumns,
    ) -> Self {
        Self {
            relation_oid,
            effective_user_oid,
            columns,
            inventory: Mutex::new(BackendLocal::new(InventoryState {
                location,
                input: None,
            })),
            exact_rows: Mutex::new(None),
        }
    }

    pub(super) const fn relation_oid(&self) -> pg_sys::Oid {
        self.relation_oid
    }

    pub(super) const fn effective_user_oid(&self) -> pg_sys::Oid {
        self.effective_user_oid
    }

    pub(super) const fn schema(&self) -> &SchemaRef {
        self.columns.schema()
    }

    /// The membership listed on first use and reused by every task plan,
    /// rescan and worker of this statement.
    fn input(&self) -> Result<Rc<StorageInput>, Error> {
        let mut inventory = Self::lock(&self.inventory)?;
        let state = inventory.get_mut();
        if let Some(input) = &state.input {
            return Ok(Rc::clone(input));
        }
        let input = Rc::new(FormatKind::Parquet.input(&state.location)?);
        state.input = Some(Rc::clone(&input));
        Ok(input)
    }

    pub(super) fn plan_tasks(
        &self,
        projection: &[usize],
        filters: Box<[ParquetBoundPredicate]>,
    ) -> Result<PlannedTasks, Error> {
        let input = self.input()?;
        let files = input.member_count() as u64;
        Ok(PlannedTasks {
            metrics: TableScanTaskMetrics {
                planned_tasks: files,
                planned_files: files,
                planned_bytes: input.total_bytes(),
            },
            input: BackendLocal::new(input),
            columns: self.columns.project(projection)?,
            filters,
        })
    }

    /// Footer row counts answer an unfiltered, zero-column scan. The count
    /// is fixed by the statement-stable inventory, so it is computed once.
    pub(super) fn exact_rows(
        &self,
        planned: &PlannedTasks,
    ) -> Result<Option<u64>, Error> {
        if !planned.columns.is_empty() || !planned.filters.is_empty() {
            return Ok(None);
        }
        let mut cached = Self::lock(&self.exact_rows)?;
        if let Some(rows) = *cached {
            return Ok(Some(rows));
        }
        let input = planned.input.get();
        let mut rows = 0_u64;
        for index in 0..input.member_count() {
            pg_sys::check_for_interrupts!();
            let file = input.open_member(index)?;
            rows = rows.saturating_add(ParquetFooter::read(&file)?.num_rows()?);
        }
        *cached = Some(rows);
        Ok(Some(rows))
    }

    fn lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>, Error> {
        mutex
            .lock()
            .map_err(|_| Error::Invariant("a bound Parquet scan lock was poisoned"))
    }
}

/// One run's task plan: every inventory member, in inventory order.
pub(super) struct PlannedTasks {
    input: BackendLocal<Rc<StorageInput>>,
    columns: ScanColumns,
    filters: Box<[ParquetBoundPredicate]>,
    metrics: TableScanTaskMetrics,
}

impl PlannedTasks {
    pub(super) fn input(&self) -> &Rc<StorageInput> {
        self.input.get()
    }

    pub(super) const fn columns(&self) -> &ScanColumns {
        &self.columns
    }

    pub(super) fn filters(&self) -> &[ParquetBoundPredicate] {
        &self.filters
    }

    pub(super) const fn metrics(&self) -> TableScanTaskMetrics {
        self.metrics
    }

    /// Whole-file tasks for one serial stream.
    pub(super) fn whole_files(&self) -> Vec<TaskSplit> {
        (0..self.input().member_count())
            .map(|member| TaskSplit {
                member,
                range: None,
            })
            .collect()
    }

    /// Split files larger than the target size into byte ranges and pack the
    /// splits into weighted groups, one per parallel work id. Returns the
    /// groups and the split count.
    pub(super) fn worker_groups(&self) -> Result<(Vec<SplitGroup>, usize), Error> {
        let target = TARGET_SPLIT_BYTES.get();
        let mut splits = Vec::new();
        for (member, size) in self.input().member_sizes().enumerate() {
            if size <= target {
                splits.push((
                    TaskSplit {
                        member,
                        range: None,
                    },
                    size,
                ));
                continue;
            }
            let mut start = 0;
            while start < size {
                let end = start.saturating_add(target).min(size);
                splits.push((
                    TaskSplit {
                        member,
                        range: Some(start..end),
                    },
                    end - start,
                ));
                start = end;
            }
        }
        if u32::try_from(splits.len()).is_err() {
            return Err(Error::worker_payload("parallel split count exceeds u32"));
        }
        let mut packer = WeightedBinPacker::new(TARGET_SPLIT_BYTES, SPLIT_LOOKBACK);
        for (item, (_, bytes)) in splits.iter().enumerate() {
            packer.push(item as u32, (*bytes).max(OPEN_FILE_COST));
        }
        let mut groups = packer
            .finish()
            .into_iter()
            .map(|group| {
                group
                    .iter()
                    .map(|item| splits[*item as usize].0.clone())
                    .collect()
            })
            .collect::<Vec<SplitGroup>>();
        // The worker SPI requires at least one work id.
        if groups.is_empty() {
            groups.push(Box::default());
        }
        Ok((groups, splits.len()))
    }
}

/// The splits one parallel work id reads.
pub(super) type SplitGroup = Box<[TaskSplit]>;

/// One inventory member, optionally restricted to a byte range.
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct TaskSplit {
    pub(super) member: usize,
    pub(super) range: Option<Range<u64>>,
}
