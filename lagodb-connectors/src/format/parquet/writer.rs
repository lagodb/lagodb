//! Format-domain Parquet object writer shared by FDW and COPY adapters.
//!
//! Finalized prefix objects are uploaded immediately and registered for
//! best-effort transaction-abort deletion. This is raw external-file behavior,
//! not transactional table publication; providing MVCC membership would
//! require a manifest or catalog outside this writer.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{DEFAULT_MAX_ROW_GROUP_ROW_COUNT, WriterProperties};

use crate::error::ConnectorError;
use crate::format::{
    EmptyOutputPolicy, FileWriteProgress, ObjectFileEncoder,
    ObjectFileEncoderFactory, ObjectSetWriter, ParquetWriteCompression,
};
use crate::storage::{ObjectFileSuffix, ObjectOutput, StagedObjectWriter};

struct ParquetEncoderFactory {
    schema: Arc<Schema>,
    properties: Arc<WriterProperties>,
}

impl ParquetEncoderFactory {
    fn new(schema: Arc<Schema>, compression: ParquetWriteCompression) -> Self {
        let properties = Arc::new(
            WriterProperties::builder()
                .set_compression(Self::compression(compression))
                .set_max_row_group_row_count(Some(DEFAULT_MAX_ROW_GROUP_ROW_COUNT))
                .build(),
        );
        Self { schema, properties }
    }

    fn compression(value: ParquetWriteCompression) -> Compression {
        match value {
            ParquetWriteCompression::Uncompressed => Compression::UNCOMPRESSED,
            ParquetWriteCompression::Snappy => Compression::SNAPPY,
            ParquetWriteCompression::Gzip => Compression::GZIP(Default::default()),
            ParquetWriteCompression::Zstd => Compression::ZSTD(Default::default()),
        }
    }
}

impl ObjectFileEncoderFactory for ParquetEncoderFactory {
    type Input = RecordBatch;
    type Encoder = ParquetFileEncoder;

    fn file_suffix(&self) -> ObjectFileSuffix {
        ObjectFileSuffix::new("parquet")
    }

    fn open(
        &mut self,
        writer: StagedObjectWriter,
    ) -> Result<Self::Encoder, ConnectorError> {
        Ok(ParquetFileEncoder {
            writer: ArrowWriter::try_new(
                writer,
                Arc::clone(&self.schema),
                Some(self.properties.as_ref().clone()),
            )?,
        })
    }
}

struct ParquetFileEncoder {
    writer: ArrowWriter<StagedObjectWriter>,
}

impl ObjectFileEncoder for ParquetFileEncoder {
    type Input = RecordBatch;

    fn write(
        &mut self,
        batch: &Self::Input,
    ) -> Result<FileWriteProgress, ConnectorError> {
        self.writer.write(batch)?;
        let estimated = self
            .writer
            .bytes_written()
            .saturating_add(self.writer.in_progress_size());
        let estimated = u64::try_from(estimated)
            .expect("PostgreSQL is supported only on platforms where usize fits in u64");
        Ok(FileWriteProgress::new(estimated))
    }

    fn finish(self) -> Result<StagedObjectWriter, ConnectorError> {
        Ok(self.writer.into_inner()?)
    }
}

/// Incremental Parquet output for one exact object or rolling prefix.
pub(crate) struct ParquetObjectWriter {
    writer: Option<ObjectSetWriter<ParquetEncoderFactory>>,
}

impl ParquetObjectWriter {
    pub(crate) fn new(
        output: ObjectOutput,
        schema: Arc<Schema>,
        compression: ParquetWriteCompression,
    ) -> Self {
        Self {
            writer: Some(ObjectSetWriter::new(
                output,
                ParquetEncoderFactory::new(schema, compression),
            )),
        }
    }

    pub(crate) fn write_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(), ConnectorError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.writer
            .as_mut()
            .expect("the Parquet writer is not used after finish")
            .write(batch)
    }

    /// Close the final footer and upload it. COPY TO emits a valid empty
    /// Parquet container; FDW INSERT leaves an untouched prefix empty.
    pub(crate) fn finish(&mut self, emit_empty: bool) -> Result<(), ConnectorError> {
        let Some(writer) = self.writer.take() else {
            return Ok(());
        };
        let policy = if emit_empty {
            EmptyOutputPolicy::EmitFile
        } else {
            EmptyOutputPolicy::Skip
        };
        writer.finish(policy)
    }
}
