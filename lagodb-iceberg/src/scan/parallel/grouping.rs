//! Iceberg split weights and bounded-lookback bin packing.

use std::collections::{HashMap, VecDeque};

use iceberg_lite::scan::FileScanTask;
use iceberg_lite::spec::DataFileFormat;
use serde::{Deserialize, Serialize};

use super::super::ScanError;

const TARGET_SIZE_PROPERTY: &str = "read.split.target-size";
const LOOKBACK_PROPERTY: &str = "read.split.planning-lookback";
const OPEN_FILE_COST_PROPERTY: &str = "read.split.open-file-cost";
const DEFAULT_TARGET_SIZE: u64 = 128 * 1024 * 1024;
const DEFAULT_LOOKBACK: usize = 10;
const DEFAULT_OPEN_FILE_COST: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub(crate) struct TaskGrouping {
    target_size: u64,
    lookback: usize,
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
                .get(TARGET_SIZE_PROPERTY)
                .map(|value| value.as_str().into()),
            lookback: properties
                .get(LOOKBACK_PROPERTY)
                .map(|value| value.as_str().into()),
            open_file_cost: properties
                .get(OPEN_FILE_COST_PROPERTY)
                .map(|value| value.as_str().into()),
        }
    }

    pub(crate) fn resolve(&self) -> Result<TaskGrouping, ScanError> {
        TaskGrouping::from_values(
            self.target_size.as_deref(),
            self.lookback.as_deref(),
            self.open_file_cost.as_deref(),
        )
    }
}

impl TaskGrouping {
    pub(crate) fn from_properties(
        properties: &HashMap<String, String>,
    ) -> Result<Self, ScanError> {
        Self::from_values(
            properties.get(TARGET_SIZE_PROPERTY).map(String::as_str),
            properties.get(LOOKBACK_PROPERTY).map(String::as_str),
            properties.get(OPEN_FILE_COST_PROPERTY).map(String::as_str),
        )
    }

    fn from_values(
        target_size: Option<&str>,
        lookback: Option<&str>,
        open_file_cost: Option<&str>,
    ) -> Result<Self, ScanError> {
        let target_size = Self::parse_property(
            target_size,
            TARGET_SIZE_PROPERTY,
            DEFAULT_TARGET_SIZE,
        )?;
        let lookback_u64 = Self::parse_property(
            lookback,
            LOOKBACK_PROPERTY,
            DEFAULT_LOOKBACK as u64,
        )?;
        let open_file_cost = Self::parse_property(
            open_file_cost,
            OPEN_FILE_COST_PROPERTY,
            DEFAULT_OPEN_FILE_COST,
        )?;
        let lookback = usize::try_from(lookback_u64).map_err(|_| {
            ScanError::ParallelScanConfiguration(format!(
                "{LOOKBACK_PROPERTY}={lookback_u64} exceeds this platform"
            ))
        })?;
        if target_size == 0 || lookback == 0 {
            return Err(ScanError::ParallelScanConfiguration(
                "split target size and planning lookback must be greater than zero"
                    .to_owned(),
            ));
        }
        Ok(Self {
            target_size,
            lookback,
            open_file_cost,
        })
    }

    fn parse_property(
        value: Option<&str>,
        name: &'static str,
        default: u64,
    ) -> Result<u64, ScanError> {
        value.map_or(Ok(default), |value| {
            value.parse::<u64>().map_err(|_| {
                ScanError::ParallelScanConfiguration(format!(
                    "{name}={value:?} is not an unsigned integer"
                ))
            })
        })
    }

    pub(crate) fn group(
        self,
        tasks: &[FileScanTask],
    ) -> Result<GroupedTaskRanges, ScanError> {
        let mut bins = VecDeque::<TaskBin>::new();
        let mut groups = Vec::new();
        let mut ranges = Vec::new();
        self.for_each_range(tasks, |task, range| {
            let range_id = u32::try_from(ranges.len()).map_err(|_| {
                ScanError::WorkerPayload(
                    "Iceberg range inventory exceeds u32".to_owned(),
                )
            })?;
            let weight = self.range_weight(task, range.length);
            ranges.push(range);
            if let Some(bin) = bins
                .iter_mut()
                .find(|bin| bin.weight.saturating_add(weight) <= self.target_size)
            {
                bin.push(range_id, weight);
                return Ok(());
            }
            let mut bin = TaskBin::default();
            bin.push(range_id, weight);
            bins.push_back(bin);
            if bins.len() > self.lookback {
                let largest = bins
                    .iter()
                    .enumerate()
                    .fold(None, |largest, (index, bin)| match largest {
                        Some((_, weight)) if weight >= bin.weight => largest,
                        _ => Some((index, bin.weight)),
                    })
                    .map(|(index, _)| index)
                    .expect("a bin was added immediately above");
                groups.push(
                    bins.remove(largest)
                        .expect("largest bin index came from this deque")
                        .tasks,
                );
            }
            Ok(())
        })?;
        groups.extend(bins.into_iter().map(|bin| bin.tasks));
        if groups.is_empty() {
            groups.push(Vec::new());
        }
        Ok(GroupedTaskRanges {
            ranges: ranges.into_boxed_slice(),
            groups: groups
                .into_iter()
                .map(Vec::into_boxed_slice)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
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
                || effective_length <= self.target_size
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
            let end = task.start.checked_add(effective_length).ok_or_else(|| {
                ScanError::WorkerPayload(
                    "file task byte range overflows u64".to_owned(),
                )
            })?;
            let mut start = task.start;
            while start < end {
                let length = self.target_size.min(end - start);
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
                .content_size_in_bytes
                .and_then(|bytes| u64::try_from(bytes).ok())
                .unwrap_or(delete.file_size_in_bytes);
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

#[derive(Default)]
struct TaskBin {
    weight: u64,
    tasks: Vec<u32>,
}

impl TaskBin {
    fn push(&mut self, range_id: u32, weight: u64) {
        self.weight = self.weight.saturating_add(weight);
        self.tasks.push(range_id);
    }
}
