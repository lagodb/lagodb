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

//! Validated, batch physical-position requests that retain Iceberg visibility.

use crate::scan::FileScanTask;
use crate::spec::DataFileFormat;
use crate::{Error, ErrorKind, Result};

/// Read selected original Parquet positions through the normal Iceberg scan
/// pipeline.
///
/// Unlike [`super::PhysicalRowReadRequest`], this request applies position
/// deletes, deletion vectors, equality deletes, schema transforms, partition
/// constants, and scan predicates. It is therefore suitable for statistical
/// sampling of the current logical snapshot, not `SnapshotAny` tuple fetches.
#[derive(Debug)]
pub struct SelectedRowsReadRequest {
    pub(super) task: FileScanTask,
    pub(super) positions: Box<[i64]>,
}

impl SelectedRowsReadRequest {
    /// Validate and construct a selected-row request for one whole-file task.
    ///
    /// # Errors
    ///
    /// Returns an error for split/non-Parquet tasks, missing manifest record
    /// counts, empty or non-increasing positions, out-of-bounds positions, or
    /// positions outside Iceberg's signed-long domain.
    pub fn try_new(task: FileScanTask, positions: Vec<u64>) -> Result<Self> {
        if task.data_file_format != DataFileFormat::Parquet {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "selected-row reads currently require Parquet data files",
            ));
        }
        if task.start != 0 || task.length != 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "selected-row reads require a whole-file scan task",
            ));
        }
        let record_count = task.record_count.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "selected-row scan task is missing its manifest record count",
            )
        })?;
        if positions.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "selected-row request must contain at least one position",
            ));
        }

        let mut converted = Vec::with_capacity(positions.len());
        let mut previous = None;
        for position in positions {
            if position >= record_count {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "selected row position exceeds the manifest record count",
                ));
            }
            if previous.is_some_and(|value| position <= value) {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "selected row positions must be strictly increasing",
                ));
            }
            converted.push(i64::try_from(position).map_err(|_| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "selected row position does not fit Iceberg long metadata column",
                )
            })?);
            previous = Some(position);
        }

        Ok(Self {
            task,
            positions: converted.into_boxed_slice(),
        })
    }

    /// Data-file path associated with this request.
    pub fn data_file_path(&self) -> &str {
        self.task.data_file_path()
    }

    /// Number of candidate physical rows in this request.
    pub fn len(&self) -> usize {
        self.positions.len()
    }

    /// Whether the request contains no candidates.
    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }
}
