//! Iceberg split weights over the shared bounded-lookback bin packer.

use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};

use iceberg_lite::scan::FileScanTask;
use iceberg_lite::spec::DataFileFormat;
use lagodb_arrow::scan::WeightedBinPacker;
use serde::{Deserialize, Serialize};

use super::super::ScanError;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum TaskGroupingError {
    #[error("{property}={value:?} is not an unsigned integer")]
    InvalidUnsignedInteger {
        property: &'static str,
        value: String,
    },
    #[error("{property} must be greater than zero")]
    MustBePositive { property: &'static str },
    #[error("{property}={value} exceeds this platform")]
    ExceedsPlatform { property: &'static str, value: u64 },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TaskGrouping {
    target_size: NonZeroU64,
    lookback: NonZeroUsize,
    open_file_cost: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct TaskGroupingConfig {
    target_size: Option<Box<str>>,
    lookback: Option<Box<str>>,
    open_file_cost: Option<Box<str>>,
}

impl TaskGroupingConfig {
    pub(crate) fn from_properties(properties: &HashMap<String, String>) -> Self {
        Self {
            target_size: properties
                .get(TaskGrouping::PROPERTY_TARGET_SIZE)
                .map(|value| value.as_str().into()),
            lookback: properties
                .get(TaskGrouping::PROPERTY_PLANNING_LOOKBACK)
                .map(|value| value.as_str().into()),
            open_file_cost: properties
                .get(TaskGrouping::PROPERTY_OPEN_FILE_COST)
                .map(|value| value.as_str().into()),
        }
    }

    pub(crate) fn resolve(&self) -> Result<TaskGrouping, ScanError> {
        Ok(TaskGrouping::from_values(
            self.target_size.as_deref(),
            self.lookback.as_deref(),
            self.open_file_cost.as_deref(),
        )?)
    }
}

impl TaskGrouping {
    pub(crate) const PROPERTY_TARGET_SIZE: &'static str = "read.split.target-size";
    pub(crate) const PROPERTY_PLANNING_LOOKBACK: &'static str =
        "read.split.planning-lookback";
    pub(crate) const PROPERTY_OPEN_FILE_COST: &'static str =
        "read.split.open-file-cost";

    pub(crate) const DEFAULT_TARGET_SIZE: u64 = 128 * 1024 * 1024;
    pub(crate) const DEFAULT_TARGET_SIZE_STR: &'static str = "134217728";

    pub(crate) const DEFAULT_PLANNING_LOOKBACK: usize = 10;
    pub(crate) const DEFAULT_PLANNING_LOOKBACK_STR: &'static str = "10";

    pub(crate) const DEFAULT_OPEN_FILE_COST: u64 = 4 * 1024 * 1024;
    pub(crate) const DEFAULT_OPEN_FILE_COST_STR: &'static str = "4194304";

    pub(crate) fn from_properties(
        properties: &HashMap<String, String>,
    ) -> Result<Self, TaskGroupingError> {
        Self::from_values(
            properties
                .get(Self::PROPERTY_TARGET_SIZE)
                .map(String::as_str),
            properties
                .get(Self::PROPERTY_PLANNING_LOOKBACK)
                .map(String::as_str),
            properties
                .get(Self::PROPERTY_OPEN_FILE_COST)
                .map(String::as_str),
        )
    }

    pub(crate) fn from_values(
        target_size: Option<&str>,
        lookback: Option<&str>,
        open_file_cost: Option<&str>,
    ) -> Result<Self, TaskGroupingError> {
        let target_size = Self::parse_property(
            target_size,
            Self::PROPERTY_TARGET_SIZE,
            Self::DEFAULT_TARGET_SIZE,
        )?;
        let target_size = NonZeroU64::new(target_size).ok_or(
            TaskGroupingError::MustBePositive {
                property: Self::PROPERTY_TARGET_SIZE,
            },
        )?;
        let lookback_u64 = Self::parse_property(
            lookback,
            Self::PROPERTY_PLANNING_LOOKBACK,
            Self::DEFAULT_PLANNING_LOOKBACK as u64,
        )?;
        let lookback = usize::try_from(lookback_u64).map_err(|_| {
            TaskGroupingError::ExceedsPlatform {
                property: Self::PROPERTY_PLANNING_LOOKBACK,
                value: lookback_u64,
            }
        })?;
        let lookback =
            NonZeroUsize::new(lookback).ok_or(TaskGroupingError::MustBePositive {
                property: Self::PROPERTY_PLANNING_LOOKBACK,
            })?;
        let open_file_cost = Self::parse_property(
            open_file_cost,
            Self::PROPERTY_OPEN_FILE_COST,
            Self::DEFAULT_OPEN_FILE_COST,
        )?;
        Ok(Self {
            target_size,
            lookback,
            open_file_cost,
        })
    }

    pub(crate) fn insert_properties(self, properties: &mut HashMap<String, String>) {
        properties.insert(
            Self::PROPERTY_TARGET_SIZE.to_owned(),
            self.target_size.get().to_string(),
        );
        properties.insert(
            Self::PROPERTY_PLANNING_LOOKBACK.to_owned(),
            self.lookback.get().to_string(),
        );
        properties.insert(
            Self::PROPERTY_OPEN_FILE_COST.to_owned(),
            self.open_file_cost.to_string(),
        );
    }

    fn parse_property(
        value: Option<&str>,
        name: &'static str,
        default: u64,
    ) -> Result<u64, TaskGroupingError> {
        value.map_or(Ok(default), |value| {
            value.parse::<u64>().map_err(|_| {
                TaskGroupingError::InvalidUnsignedInteger {
                    property: name,
                    value: value.to_owned(),
                }
            })
        })
    }

    pub(crate) fn group(
        self,
        tasks: &[FileScanTask],
    ) -> Result<GroupedTaskRanges, ScanError> {
        let mut packer = WeightedBinPacker::new(self.target_size, self.lookback);
        let mut ranges = Vec::new();
        self.for_each_range(tasks, |task, range| {
            let range_id = u32::try_from(ranges.len()).map_err(|_| {
                ScanError::WorkerPayload(
                    "Iceberg range inventory exceeds u32".to_owned(),
                )
            })?;
            let weight = self.range_weight(task, range.length);
            ranges.push(range);
            packer.push(range_id, weight);
            Ok(())
        })?;
        let mut groups = packer.finish();
        if groups.is_empty() {
            groups.push(Box::default());
        }
        Ok(GroupedTaskRanges {
            ranges: ranges.into_boxed_slice(),
            groups: groups.into_boxed_slice(),
        })
    }

    fn for_each_range(
        self,
        tasks: &[FileScanTask],
        mut visit: impl FnMut(&FileScanTask, TaskRange) -> Result<(), ScanError>,
    ) -> Result<(), ScanError> {
        for (file_id, task) in tasks.iter().enumerate() {
            let file_id = u32::try_from(file_id).map_err(|_| {
                ScanError::WorkerPayload(
                    "Iceberg file inventory exceeds u32".to_owned(),
                )
            })?;
            let effective_length = if task.length == 0 {
                task.file_size_in_bytes
            } else {
                task.length
            };
            if task.data_file_format != DataFileFormat::Parquet
                || effective_length <= self.target_size.get()
            {
                visit(
                    task,
                    TaskRange {
                        file_id,
                        start: task.start,
                        length: task.length,
                        record_count: task.record_count,
                        split: false,
                    },
                )?;
                continue;
            }
            // TODO(parquet-split-offsets): `plan_files()` discards
            // `DataFile::split_offsets`, so large Parquet files are sliced into
            // uniform `target_size` byte windows here and `iceberg-lite` assigns
            // each row group by its compressed byte midpoint. When a single file
            // barely exceeds `target_size`, the trailing window can own far fewer
            // row groups than the leading window. `iceberg-lite` already exposes
            // `TableScan::plan_files_with_data_files()` and `TaskRange` already
            // encodes arbitrary `(start, length)` spans; thread `split_offsets`
            // through native and columnar task planning so Parquet files with
            // recorded offsets emit row-group-aligned ranges before bin packing,
            // keeping fixed-size slicing as the fallback when offsets are absent.
            let end = task.start.checked_add(effective_length).ok_or_else(|| {
                ScanError::WorkerPayload(
                    "file task byte range overflows u64".to_owned(),
                )
            })?;
            let mut start = task.start;
            while start < end {
                let length = self.target_size.get().min(end - start);
                visit(
                    task,
                    TaskRange {
                        file_id,
                        start,
                        length,
                        record_count: None,
                        split: true,
                    },
                )?;
                start += length;
            }
        }
        Ok(())
    }

    fn range_weight(self, task: &FileScanTask, range_length: u64) -> u64 {
        let delete_bytes = task.deletes.iter().fold(0_u64, |total, delete| {
            let bytes = delete
                .content_size_in_bytes()
                .map(|bytes| bytes as u64)
                .unwrap_or(delete.file_size_in_bytes());
            total.saturating_add(bytes)
        });
        let data_bytes = if range_length == 0 {
            task.file_size_in_bytes
        } else {
            range_length
        };
        let content_weight = data_bytes.saturating_add(delete_bytes);
        let open_count = u64::try_from(task.deletes.len())
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        content_weight.max(open_count.saturating_mul(self.open_file_cost))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TaskRange {
    pub(super) file_id: u32,
    pub(super) start: u64,
    pub(super) length: u64,
    pub(super) record_count: Option<u64>,
    pub(super) split: bool,
}

pub(crate) struct GroupedTaskRanges {
    pub(super) ranges: Box<[TaskRange]>,
    pub(super) groups: Box<[Box<[u32]>]>,
}

impl GroupedTaskRanges {
    pub(crate) fn group_count(&self) -> usize {
        self.groups.len()
    }

    pub(crate) fn range_count(&self) -> usize {
        self.ranges.len()
    }
}
