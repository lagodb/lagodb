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

use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};

/// Default gap between byte ranges below which they are coalesced into a
/// single request in the upstream async reader.
const DEFAULT_RANGE_COALESCE_BYTES: u64 = 1024 * 1024;

/// Default maximum number of coalesced byte ranges fetched concurrently in the
/// upstream async reader.
const DEFAULT_RANGE_FETCH_CONCURRENCY: usize = 10;

/// Default number of bytes to prefetch when parsing Parquet footer metadata.
const DEFAULT_METADATA_SIZE_HINT: usize = 512 * 1024;

/// Options for tuning Parquet file I/O.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct ParquetReadOptions {
    /// Number of bytes to prefetch for parsing the Parquet metadata.
    pub(super) metadata_size_hint: Option<usize>,
    /// Gap threshold for merging nearby byte ranges into a single request.
    pub(super) range_coalesce_bytes: u64,
    /// Maximum number of merged byte ranges to fetch concurrently.
    pub(super) range_fetch_concurrency: usize,
    /// Policy for loading the Parquet column index.
    pub(super) column_index_policy: PageIndexPolicy,
    /// Policy for loading the Parquet offset index.
    pub(super) offset_index_policy: PageIndexPolicy,
}

impl Default for ParquetReadOptions {
    fn default() -> Self {
        Self {
            metadata_size_hint: Some(DEFAULT_METADATA_SIZE_HINT),
            range_coalesce_bytes: DEFAULT_RANGE_COALESCE_BYTES,
            range_fetch_concurrency: DEFAULT_RANGE_FETCH_CONCURRENCY,
            column_index_policy: PageIndexPolicy::Skip,
            offset_index_policy: PageIndexPolicy::Skip,
        }
    }
}

impl ParquetReadOptions {
    pub(super) fn with_index_policies(
        mut self,
        column_index_policy: PageIndexPolicy,
        offset_index_policy: PageIndexPolicy,
    ) -> Self {
        self.column_index_policy = column_index_policy;
        self.offset_index_policy = offset_index_policy;
        self
    }

    pub(super) fn apply_to_options(
        &self,
        options: ArrowReaderOptions,
    ) -> ArrowReaderOptions {
        options
            .with_column_index_policy(self.column_index_policy)
            .with_offset_index_policy(self.offset_index_policy)
    }

    pub(super) fn apply_to_metadata_reader(
        &self,
        reader: ParquetMetaDataReader,
    ) -> ParquetMetaDataReader {
        reader
            .with_column_index_policy(self.column_index_policy)
            .with_offset_index_policy(self.offset_index_policy)
    }
}
