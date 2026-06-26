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

//! Writer adapter for Iceberg position delete files.

use arrow_array::RecordBatch;

use crate::spec::{DataContentType, DataFile, PartitionKey};
use crate::writer::file_writer::FileWriterBuilder;
use crate::writer::file_writer::location_generator::{
    FileNameGenerator, LocationGenerator,
};
use crate::writer::file_writer::rolling_writer::{
    RollingFileWriter, RollingFileWriterBuilder,
};
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Builder for [`PositionDeleteFileWriter`].
#[derive(Debug)]
pub struct PositionDeleteFileWriterBuilder<
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
> {
    inner: RollingFileWriterBuilder<B, L, F>,
    config: PositionDeleteWriterConfig,
}

impl<B, L, F> PositionDeleteFileWriterBuilder<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    /// Create a new position delete writer builder.
    pub fn new(
        inner: RollingFileWriterBuilder<B, L, F>,
        config: PositionDeleteWriterConfig,
    ) -> Self {
        Self { inner, config }
    }
}

/// Configuration for writing deletes that all reference one data file.
#[derive(Debug, Clone)]
pub struct PositionDeleteWriterConfig {
    referenced_data_file: String,
}

impl PositionDeleteWriterConfig {
    /// Create a config for position deletes scoped to one data file.
    pub fn new(referenced_data_file: impl Into<String>) -> Self {
        Self {
            referenced_data_file: referenced_data_file.into(),
        }
    }

    /// Return the referenced data file path.
    pub fn referenced_data_file(&self) -> &str {
        &self.referenced_data_file
    }
}

impl<B, L, F> IcebergWriterBuilder for PositionDeleteFileWriterBuilder<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    type R = PositionDeleteFileWriter<B, L, F>;

    fn build(&self, partition_key: Option<PartitionKey>) -> Result<Self::R> {
        Ok(PositionDeleteFileWriter {
            inner: Some(self.inner.build()),
            referenced_data_file: self.config.referenced_data_file.clone(),
            partition_key,
        })
    }
}

/// Writer used to write Iceberg position delete files.
#[derive(Debug)]
pub struct PositionDeleteFileWriter<
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
> {
    inner: Option<RollingFileWriter<B, L, F>>,
    referenced_data_file: String,
    partition_key: Option<PartitionKey>,
}

impl<B, L, F> IcebergWriter for PositionDeleteFileWriter<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    fn write(&mut self, batch: RecordBatch) -> Result<()> {
        if let Some(writer) = self.inner.as_mut() {
            writer.write(&self.partition_key, &batch)
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                "position delete inner writer has been closed",
            ))
        }
    }

    fn close(&mut self) -> Result<Vec<DataFile>> {
        if let Some(writer) = self.inner.take() {
            writer
                .close()?
                .into_iter()
                .map(|mut res| {
                    res.content(DataContentType::PositionDeletes);
                    res.equality_ids(None);
                    res.referenced_data_file(Some(self.referenced_data_file.clone()));
                    if let Some(pk) = self.partition_key.as_ref() {
                        res.partition(pk.data().clone());
                        res.partition_spec_id(pk.spec().spec_id());
                    }
                    res.build().map_err(|e| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("failed to build position delete file: {e}"),
                        )
                    })
                })
                .collect()
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                "position delete inner writer has been closed",
            ))
        }
    }
}
