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

//! Scan metrics and I/O counting for Parquet data file reads.

use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::Result;
use crate::io::FileRead;
use crate::scan::ArrowRecordBatchIterator;

/// Wraps a [`FileRead`] to count bytes read via a shared atomic counter.
pub(crate) struct CountingFileRead {
    inner: Box<dyn FileRead>,
    bytes_read: Arc<AtomicU64>,
}

impl CountingFileRead {
    pub(crate) fn new(inner: Box<dyn FileRead>, bytes_read: Arc<AtomicU64>) -> Self {
        Self { inner, bytes_read }
    }
}

impl Read for CountingFileRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.bytes_read.fetch_add(read as u64, Ordering::Relaxed);
        Ok(read)
    }
}

impl Seek for CountingFileRead {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl FileRead for CountingFileRead {
    fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
        debug_assert!(range.end >= range.start);
        let len = range.end.saturating_sub(range.start);
        let bytes = self.inner.read_range(range)?;
        self.bytes_read.fetch_add(len, Ordering::Relaxed);
        Ok(bytes)
    }

    fn read_all(&self) -> Result<Bytes> {
        let bytes = self.inner.read_all()?;
        self.bytes_read
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }

    fn try_clone(&self) -> std::io::Result<Box<dyn FileRead>> {
        Ok(Box::new(Self::new(
            self.inner.try_clone()?,
            Arc::clone(&self.bytes_read),
        )))
    }
}

/// Metrics collected during an Iceberg scan.
#[derive(Clone, Debug)]
pub struct ScanMetrics {
    bytes_read: Arc<AtomicU64>,
}

impl ScanMetrics {
    pub(crate) fn new() -> Self {
        Self {
            bytes_read: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn bytes_read_counter(&self) -> &Arc<AtomicU64> {
        &self.bytes_read
    }

    /// Total bytes read from storage during this scan, including data files and delete files.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }
}

/// Result of [`ArrowReader::read_with_metrics`](super::ArrowReader::read_with_metrics),
/// containing the record batch iterator and metrics collected during the scan.
pub struct ScanResult {
    iterator: ArrowRecordBatchIterator,
    metrics: ScanMetrics,
}

impl ScanResult {
    pub(crate) fn new(
        iterator: ArrowRecordBatchIterator,
        metrics: ScanMetrics,
    ) -> Self {
        Self { iterator, metrics }
    }

    /// Consumes the result, returning only the record batch iterator.
    pub fn stream(self) -> ArrowRecordBatchIterator {
        self.iterator
    }

    /// Consumes the result, returning only the record batch iterator.
    pub fn iterator(self) -> ArrowRecordBatchIterator {
        self.iterator
    }

    /// Returns a reference to the scan metrics.
    pub fn metrics(&self) -> &ScanMetrics {
        &self.metrics
    }
}
