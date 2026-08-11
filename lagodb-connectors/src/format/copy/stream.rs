//! Object-backed byte adapters for PostgreSQL COPY.

use std::io::{self, Read, Write};

use pg_lakebase_core::copy::{CopyDataDestination, CopyDataSource, CopyError};
use pg_lakebase_storage::StorageFile;

use crate::error::ConnectorError;
use crate::format::{StreamCompression, StreamDecoder, StreamEncoder};
use crate::storage::{ObjectWriteTarget, StagedObjectUpload, StagedObjectWriter};

pub(super) struct ObjectCopySource {
    decoder: StreamDecoder<ObjectReader>,
}

impl ObjectCopySource {
    pub(super) fn new(
        file: StorageFile,
        compression: StreamCompression,
    ) -> Result<Self, CopyError> {
        let decoder = StreamDecoder::new(ObjectReader { file }, compression)
            .map_err(ConnectorError::copy_stream_io)?;
        Ok(Self { decoder })
    }
}

impl CopyDataSource for ObjectCopySource {
    fn read(
        &mut self,
        output: &mut [u8],
        min_read: usize,
    ) -> Result<usize, CopyError> {
        let read = self
            .decoder
            .read_at_least(output, min_read)
            .map_err(ConnectorError::copy_stream_io)?;
        Ok(read)
    }
}

struct ObjectReader {
    file: StorageFile,
}

impl Read for ObjectReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.file.read_into(output).map_err(io::Error::other)
    }
}

pub(super) struct ObjectCopyDestination {
    encoder: StreamEncoder<StagedObjectWriter>,
    upload: StagedObjectUpload,
}

impl ObjectCopyDestination {
    pub(super) fn new(
        target: ObjectWriteTarget,
        compression: StreamCompression,
    ) -> Result<Self, CopyError> {
        let (writer, upload) =
            StagedObjectUpload::start(target).map_err(CopyError::storage)?;
        let encoder = StreamEncoder::new(writer, compression)
            .map_err(ConnectorError::copy_stream_io)?;
        Ok(Self { encoder, upload })
    }

    pub(super) fn finish(self) -> Result<(), CopyError> {
        let Self { encoder, upload } = self;
        let writer = encoder.finish().map_err(ConnectorError::copy_stream_io)?;
        writer.finish_local().map_err(CopyError::storage)?;
        upload.finish().map_err(CopyError::storage)
    }
}

impl CopyDataDestination for ObjectCopyDestination {
    fn write_row(&mut self, data: &[u8]) -> Result<(), CopyError> {
        self.encoder
            .write_all(data)
            .map_err(ConnectorError::copy_stream_io)?;
        // PostgreSQL's COPY_CALLBACK destination omits the text/CSV row
        // terminator that its file and frontend destinations append. Lago COPY
        // TO supports only those two line-oriented formats, so restore their
        // platform-independent object framing before compression.
        self.encoder
            .write_all(b"\n")
            .map_err(ConnectorError::copy_stream_io)?;
        Ok(())
    }
}
