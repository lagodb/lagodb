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
use parquet::file::metadata::PageIndexPolicy;

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
pub(super) struct ParquetReadOptions {
    /// Number of bytes to prefetch for parsing the Parquet metadata.
    pub(super) metadata_size_hint: Option<usize>,
    /// Gap threshold for merging nearby byte ranges into a single request.
    pub(super) range_coalesce_bytes: u64,
    /// Maximum number of merged byte ranges to fetch concurrently.
    pub(super) range_fetch_concurrency: usize,
    /// Whether to preload the page index when reading Parquet metadata.
    pub(super) preload_page_index: bool,
}

impl Default for ParquetReadOptions {
    fn default() -> Self {
        Self {
            metadata_size_hint: Some(DEFAULT_METADATA_SIZE_HINT),
            range_coalesce_bytes: DEFAULT_RANGE_COALESCE_BYTES,
            range_fetch_concurrency: DEFAULT_RANGE_FETCH_CONCURRENCY,
            preload_page_index: false,
        }
    }
}

impl ParquetReadOptions {
    pub(super) fn with_page_index(mut self, load: bool) -> Self {
        self.preload_page_index = load;
        self
    }

    pub(super) fn apply_to_options(
        &self,
        options: ArrowReaderOptions,
    ) -> ArrowReaderOptions {
        options.with_page_index_policy(PageIndexPolicy::from(self.preload_page_index))
    }
}
