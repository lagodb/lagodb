//! Shared row-delimited object encoder for stream formats.

use std::io::Write;

use crate::error::ConnectorError;
use crate::storage::{ObjectFileSuffix, StagedObjectWriter};

use super::{
    FileWriteProgress, ObjectFileEncoder, ObjectFileEncoderFactory,
    StreamCompression, StreamEncoder,
};

#[derive(Clone, Copy)]
pub(crate) enum StreamFormat {
    Text,
    Csv,
    Json,
}

pub(crate) struct StreamEncoderFactory {
    compression: StreamCompression,
    suffix: ObjectFileSuffix,
    header: Option<Box<[u8]>>,
}

impl StreamEncoderFactory {
    pub(crate) fn new(format: StreamFormat, compression: StreamCompression) -> Self {
        let suffix = match (format, compression) {
            (StreamFormat::Text, StreamCompression::None) => "text",
            (StreamFormat::Text, StreamCompression::Gzip) => "text.gz",
            (StreamFormat::Text, StreamCompression::Zstd) => "text.zst",
            (StreamFormat::Csv, StreamCompression::None) => "csv",
            (StreamFormat::Csv, StreamCompression::Gzip) => "csv.gz",
            (StreamFormat::Csv, StreamCompression::Zstd) => "csv.zst",
            (StreamFormat::Json, StreamCompression::None) => "json",
            (StreamFormat::Json, StreamCompression::Gzip) => "json.gz",
            (StreamFormat::Json, StreamCompression::Zstd) => "json.zst",
        };
        Self {
            compression,
            suffix: ObjectFileSuffix::new(suffix),
            header: None,
        }
    }

    pub(crate) fn set_header(&mut self, header: Box<[u8]>) {
        self.header = Some(header);
    }
}

impl ObjectFileEncoderFactory for StreamEncoderFactory {
    type Input = [u8];
    type Encoder = StreamFileEncoder;

    fn file_suffix(&self) -> ObjectFileSuffix {
        self.suffix
    }

    fn open(
        &mut self,
        writer: StagedObjectWriter,
    ) -> Result<Self::Encoder, ConnectorError> {
        let mut encoder = StreamEncoder::new(writer, self.compression)
            .map_err(ConnectorError::copy_stream_io)?;
        if let Some(header) = &self.header {
            encoder
                .write_all(header)
                .and_then(|()| encoder.write_all(b"\n"))
                .map_err(ConnectorError::copy_stream_io)?;
        }
        Ok(StreamFileEncoder { encoder })
    }
}

pub(crate) struct StreamFileEncoder {
    encoder: StreamEncoder<StagedObjectWriter>,
}

impl ObjectFileEncoder for StreamFileEncoder {
    type Input = [u8];

    fn write(
        &mut self,
        data: &Self::Input,
    ) -> Result<FileWriteProgress, ConnectorError> {
        self.encoder
            .write_all(data)
            .and_then(|()| self.encoder.write_all(b"\n"))
            .map_err(ConnectorError::copy_stream_io)?;
        Ok(FileWriteProgress::new(
            self.encoder.writer().bytes_written(),
        ))
    }

    fn finish(self) -> Result<StagedObjectWriter, ConnectorError> {
        self.encoder
            .finish()
            .map_err(ConnectorError::copy_stream_io)
    }
}
