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

use bytes::Bytes;
use parquet::file::reader::{ChunkReader, Length};

use crate::io::{FileMetadata, FileRead};

/// ArrowFileReader is a wrapper around a FileRead that implements parquet's ChunkReader.
pub struct ArrowFileReader<R: FileRead> {
    meta: FileMetadata,
    r: R,
}

impl<R: FileRead> ArrowFileReader<R> {
    /// Create a new ArrowFileReader
    pub fn new(meta: FileMetadata, r: R) -> Self {
        Self { meta, r }
    }
}

impl<R: FileRead> Length for ArrowFileReader<R> {
    fn len(&self) -> u64 {
        self.meta.size
    }
}

impl<R: FileRead> ChunkReader for ArrowFileReader<R> {
    type T = Box<dyn FileRead>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let mut reader = self
            .r
            .try_clone()
            .map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))?;
        reader
            .seek(std::io::SeekFrom::Start(start))
            .map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))?;
        Ok(reader)
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        self.r
            .read_range(start..start + length as u64)
            .map_err(|err| parquet::errors::ParquetError::External(Box::new(err)))
    }
}
