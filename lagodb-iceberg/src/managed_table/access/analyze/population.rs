//! Stable Iceberg file population and ANALYZE read-plan construction.

use std::collections::HashSet;
use std::rc::Rc;

use iceberg_lite::arrow::SelectedRowsReadRequest;
use iceberg_lite::scan::FileScanTask;
use rand::Rng;

use super::cursor::{
    AnalyzeReadPlan, ExpectedCursor, ExpectedFile, ExpectedPositions,
};
use super::sampling::AnalyzeSampler;
use crate::error::{IcebergError, IcebergResult};

/// Immutable whole-snapshot population captured before sampling begins.
pub(crate) struct AnalyzePopulation {
    files: Box<[AnalyzeFile]>,
    zero_row_files: Box<[AnalyzeFile]>,
    pub(super) physical_rows: u64,
}

struct AnalyzeFile {
    task: FileScanTask,
    record_count: u64,
}

impl AnalyzePopulation {
    pub(super) fn try_new(tasks: Vec<FileScanTask>) -> IcebergResult<Self> {
        let mut files = Vec::with_capacity(tasks.len());
        let mut zero_row_files = Vec::new();
        let mut paths = HashSet::with_capacity(tasks.len());
        let mut physical_rows = 0_u64;

        for task in tasks {
            if !task.is_whole_file() {
                return Err(IcebergError::InvariantViolated(
                    "ANALYZE requires one whole-file task per Iceberg data file",
                ));
            }
            let record_count =
                task.record_count.ok_or(IcebergError::InvariantViolated(
                    "ANALYZE file task is missing its manifest record count",
                ))?;
            if !paths.insert(task.data_file_path.clone()) {
                return Err(IcebergError::InvariantViolated(
                    "ANALYZE planned duplicate data-file tasks",
                ));
            }
            if record_count == 0 {
                zero_row_files.push(AnalyzeFile { task, record_count });
                continue;
            }
            files.push(AnalyzeFile { task, record_count });
            physical_rows = physical_rows.checked_add(record_count).ok_or(
                IcebergError::InvariantViolated(
                    "Iceberg physical row population exceeds unsigned long range",
                ),
            )?;
        }

        Ok(Self {
            files: files.into_boxed_slice(),
            zero_row_files: zero_row_files.into_boxed_slice(),
            physical_rows,
        })
    }

    pub(super) fn locality_plan<R: Rng + ?Sized>(
        self,
        desired_candidates: u64,
        max_data_files: usize,
        rng: &mut R,
    ) -> IcebergResult<AnalyzePlannedSample> {
        let full_plan_file_count = self
            .files
            .len()
            .checked_add(self.zero_row_files.len())
            .ok_or(IcebergError::InvariantViolated(
                "ANALYZE data-file count overflowed",
            ))?;
        if desired_candidates == self.physical_rows
            && full_plan_file_count != 0
            && full_plan_file_count <= max_data_files
        {
            return Ok(AnalyzePlannedSample {
                read_plan: self.full_plan(),
                candidate_count: desired_candidates,
            });
        }
        let record_counts = self
            .files
            .iter()
            .map(|file| file.record_count)
            .collect::<Vec<_>>();
        let sampling = AnalyzeSampler::sample_population(
            &record_counts,
            self.physical_rows,
            desired_candidates,
            max_data_files,
            rng,
        )?;

        let mut requests = Vec::new();
        let mut expected_files = Vec::new();
        let mut samples = sampling.files.into_iter().peekable();

        for (file_index, file) in self.files.into_vec().into_iter().enumerate() {
            let Some(sample) = samples.peek() else {
                break;
            };
            if sample.file_index != file_index {
                continue;
            }
            let sample = samples.next().ok_or(IcebergError::InvariantViolated(
                "ANALYZE sampled file disappeared during plan construction",
            ))?;
            if sample.positions.is_empty() {
                continue;
            }
            let path: Rc<str> = Rc::from(file.task.data_file_path.as_str());
            let positions = sample
                .positions
                .iter()
                .map(|position| position.position)
                .collect::<Vec<_>>();
            expected_files.push(ExpectedFile {
                path,
                positions: ExpectedPositions::Selected(
                    sample.positions.into_boxed_slice(),
                ),
            });
            requests.push(SelectedRowsReadRequest::try_new(file.task, positions)?);
        }
        if samples.next().is_some() {
            return Err(IcebergError::InvariantViolated(
                "ANALYZE sampled file does not belong to the population",
            ));
        }

        Ok(AnalyzePlannedSample {
            read_plan: AnalyzeReadPlan::Selected {
                requests,
                expected: ExpectedCursor::new(expected_files),
            },
            candidate_count: sampling.observation_count,
        })
    }

    fn full_plan(self) -> AnalyzeReadPlan {
        let file_count = self.files.len() + self.zero_row_files.len();
        let mut tasks = Vec::with_capacity(file_count);
        let mut expected_files = Vec::with_capacity(file_count);
        for file in self.files.into_iter().chain(self.zero_row_files) {
            expected_files.push(ExpectedFile {
                path: Rc::from(file.task.data_file_path.as_str()),
                positions: ExpectedPositions::All(file.record_count),
            });
            tasks.push(file.task);
        }
        AnalyzeReadPlan::Full {
            tasks,
            expected: ExpectedCursor::new(expected_files),
        }
    }
}

pub(super) struct AnalyzePlannedSample {
    pub(super) read_plan: AnalyzeReadPlan,
    pub(super) candidate_count: u64,
}
