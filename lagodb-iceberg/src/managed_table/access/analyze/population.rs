//! Stable Iceberg file population and ANALYZE read-plan construction.

use std::collections::HashSet;

use iceberg_lite::arrow::SelectedRowsReadRequest;
use iceberg_lite::scan::FileScanTask;
use iceberg_lite::{Error, ErrorKind};
use rand::Rng;

use super::cursor::{AnalyzeReadPlan, ExpectedFile, ExpectedPositions};
use super::sampling::AnalyzeSampler;
use crate::error::IcebergResult;

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
    /// `tasks` come from iceberg-lite's `plan_files()`, which builds one
    /// whole-file task per live data file with its manifest record count.
    pub(super) fn try_new(tasks: Vec<FileScanTask>) -> IcebergResult<Self> {
        // The reader maps batches back to planned files by path.
        let has_duplicate_path = {
            let mut paths = HashSet::with_capacity(tasks.len());
            !tasks.iter().all(|task| paths.insert(task.data_file_path()))
        };
        if has_duplicate_path {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Iceberg snapshot lists a data file more than once",
            )
            .into());
        }

        let mut files = Vec::with_capacity(tasks.len());
        let mut zero_row_files = Vec::new();
        let mut physical_rows = 0_u64;
        for task in tasks {
            let record_count = task
                .record_count
                .expect("iceberg-lite plans whole-file tasks with record counts");
            if record_count == 0 {
                zero_row_files.push(AnalyzeFile { task, record_count });
                continue;
            }
            physical_rows = physical_rows.checked_add(record_count).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Iceberg manifest record counts exceed the unsigned long range",
                )
            })?;
            files.push(AnalyzeFile { task, record_count });
        }

        Ok(Self {
            files: files.into_boxed_slice(),
            zero_row_files: zero_row_files.into_boxed_slice(),
            physical_rows,
        })
    }

    /// Plan up to `requested_candidates` candidate rows. The candidate count
    /// is bounded by PostgreSQL's `targrows` (a C `int`) on PG17/18 and by the
    /// relation-local statistics budget (at most 300 * 10000) on PG16.
    pub(super) fn locality_plan<R: Rng + ?Sized>(
        self,
        requested_candidates: u64,
        max_data_files: usize,
        rng: &mut R,
    ) -> IcebergResult<AnalyzePlannedSample> {
        let file_count = self.files.len() + self.zero_row_files.len();
        let (read_plan, candidate_count) = if requested_candidates
            >= self.physical_rows
            && file_count != 0
            && file_count <= max_data_files
        {
            let physical_rows = self.physical_rows;
            (self.full_plan(), physical_rows)
        } else {
            self.selected_plan(requested_candidates, max_data_files, rng)?
        };
        Ok(AnalyzePlannedSample {
            read_plan,
            candidate_count: u32::try_from(candidate_count)
                .expect("PostgreSQL bounds ANALYZE sample rows by a C int"),
        })
    }

    fn selected_plan<R: Rng + ?Sized>(
        self,
        requested_candidates: u64,
        max_data_files: usize,
        rng: &mut R,
    ) -> IcebergResult<(AnalyzeReadPlan, u64)> {
        let record_counts = self
            .files
            .iter()
            .map(|file| file.record_count)
            .collect::<Vec<_>>();
        let sampling = AnalyzeSampler::sample_population(
            &record_counts,
            requested_candidates,
            max_data_files,
            rng,
        );

        let mut files = self.files.into_vec().into_iter().enumerate();
        let mut requests = Vec::with_capacity(sampling.files.len());
        let mut expected_files = Vec::with_capacity(sampling.files.len());
        for sample in sampling.files {
            let (_, file) = files
                .find(|(file_index, _)| *file_index == sample.file_index)
                .expect("the sampler returns ascending indexes into the population");
            let positions = sample
                .positions
                .iter()
                .map(|position| position.position)
                .collect::<Vec<_>>();
            expected_files.push(ExpectedFile {
                path: Box::from(file.task.data_file_path()),
                positions: ExpectedPositions::Selected(sample.positions.into_iter()),
            });
            requests.push(SelectedRowsReadRequest::try_new(file.task, positions)?);
        }

        Ok((
            AnalyzeReadPlan::Selected {
                requests,
                files: expected_files,
            },
            sampling.observation_count,
        ))
    }

    fn full_plan(self) -> AnalyzeReadPlan {
        let file_count = self.files.len() + self.zero_row_files.len();
        let mut tasks = Vec::with_capacity(file_count);
        let mut expected_files = Vec::with_capacity(file_count);
        for file in self.files.into_iter().chain(self.zero_row_files) {
            expected_files.push(ExpectedFile {
                path: Box::from(file.task.data_file_path()),
                positions: ExpectedPositions::All(0..file.record_count),
            });
            tasks.push(file.task);
        }
        AnalyzeReadPlan::Full {
            tasks,
            files: expected_files,
        }
    }
}

pub(super) struct AnalyzePlannedSample {
    pub(super) read_plan: AnalyzeReadPlan,
    pub(super) candidate_count: u32,
}
