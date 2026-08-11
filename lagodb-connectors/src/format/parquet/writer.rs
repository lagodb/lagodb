//! Format-domain Parquet object writer shared by FDW and COPY adapters.
//!
//! # External-file transaction semantics
//!
//! Finalized objects are uploaded immediately, including each prefix object
//! rolled at [`TARGET_FILE_BYTES`]; visibility is not deferred until PostgreSQL
//! commit. Prefix scans list matching objects directly, so another backend can
//! observe a file written by a transaction that later aborts. Abort cleanup of
//! operation-unique prefix keys is best-effort garbage collection: it neither
//! provides MVCC nor retracts observations that already occurred.
//!
//! This is the intentional contract for a raw file-backed foreign table.
//! PostgreSQL's `file_fdw` is read-only; the analogous writable core behavior
//! is server-side `COPY TO` a file, which writes the external file during the
//! command and does not roll it back with the SQL transaction. PostgreSQL
//! transaction visibility would require a transactional membership catalog or
//! manifest. Deferring every upload instead would retain transaction-scale
//! staged files and move all object-store I/O to pre-commit. Neither behavior
//! is part of this connector's storage model.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{DEFAULT_MAX_ROW_GROUP_ROW_COUNT, WriterProperties};

use crate::error::ConnectorError;
use crate::format::ParquetWriteCompression;
use crate::storage::{
    ObjectLocationKind, ObjectOutput, StagedObjectUpload, StagedObjectWriter,
};

const TARGET_FILE_BYTES: usize = 256 * 1024 * 1024;

/// Rolling Parquet files for one exact object or operation-unique prefix.
///
/// This object owns format and upload lifecycle only. Row production remains
/// in the FDW/COPY adapters, which submit complete Arrow batches.
pub(crate) struct ParquetObjectWriter {
    output: ObjectOutput,
    schema: Arc<Schema>,
    properties: Arc<WriterProperties>,
    writer: Option<ArrowWriter<StagedObjectWriter>>,
    upload: Option<StagedObjectUpload>,
}

impl ParquetObjectWriter {
    pub(crate) fn new(
        output: ObjectOutput,
        schema: Arc<Schema>,
        compression: ParquetWriteCompression,
    ) -> Self {
        let properties = Arc::new(
            WriterProperties::builder()
                .set_compression(Self::compression(compression))
                .set_max_row_group_row_count(Some(DEFAULT_MAX_ROW_GROUP_ROW_COUNT))
                .build(),
        );
        Self {
            output,
            schema,
            properties,
            writer: None,
            upload: None,
        }
    }

    pub(crate) fn write_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(), ConnectorError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let prefix = self.output.kind() == ObjectLocationKind::Prefix;
        let writer = self.ensure_writer()?;
        writer.write(batch)?;
        if prefix
            && writer
                .bytes_written()
                .saturating_add(writer.in_progress_size())
                >= TARGET_FILE_BYTES
        {
            self.finish_file()?;
        }
        Ok(())
    }

    /// Close the final footer and upload it. COPY TO requests an empty file for
    /// a zero-row result; FDW INSERT leaves an untouched prefix empty.
    pub(crate) fn finish(&mut self, emit_empty: bool) -> Result<(), ConnectorError> {
        if emit_empty && self.writer.is_none() {
            self.ensure_writer()?;
        }
        self.finish_file()
    }

    fn ensure_writer(
        &mut self,
    ) -> Result<&mut ArrowWriter<StagedObjectWriter>, ConnectorError> {
        if self.writer.is_none() {
            let target = self.output.next_object()?;
            let (staging, upload) = StagedObjectUpload::start(target)?;
            self.writer = Some(ArrowWriter::try_new(
                staging,
                Arc::clone(&self.schema),
                Some(self.properties.as_ref().clone()),
            )?);
            self.upload = Some(upload);
        }
        Ok(self.writer.as_mut().expect("writer was initialized"))
    }

    fn finish_file(&mut self) -> Result<(), ConnectorError> {
        let Some(writer) = self.writer.take() else {
            return Ok(());
        };
        let staging = writer.into_inner()?;
        staging.finish_local()?;
        self.upload
            .take()
            .expect("every Parquet writer has one upload capability")
            .finish()?;
        Ok(())
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
