//! Shared row-delimited object encoder for stream formats.

use std::io::Write;

use crate::error::ConnectorError;
use crate::storage::{ObjectFileSuffix, StagedObjectWriter};

use super::{
    FileWriteProgress, FormatKind, ObjectFileEncoder, ObjectFileEncoderFactory,
    StreamCompression, StreamEncoder,
};

pub(crate) struct StreamEncoderFactory {
    compression: StreamCompression,
    suffix: ObjectFileSuffix,
    header: Option<Box<[u8]>>,
}

impl StreamEncoderFactory {
    pub(crate) fn new(format: FormatKind, compression: StreamCompression) -> Self {
        let suffix = match (format, compression) {
            (FormatKind::Text, StreamCompression::None) => "text",
            (FormatKind::Text, StreamCompression::Gzip) => "text.gz",
            (FormatKind::Text, StreamCompression::Zstd) => "text.zst",
            (FormatKind::Csv, StreamCompression::None) => "csv",
            (FormatKind::Csv, StreamCompression::Gzip) => "csv.gz",
            (FormatKind::Csv, StreamCompression::Zstd) => "csv.zst",
            (FormatKind::Json, StreamCompression::None) => "json",
            (FormatKind::Json, StreamCompression::Gzip) => "json.gz",
            (FormatKind::Json, StreamCompression::Zstd) => "json.zst",
            (FormatKind::Avro | FormatKind::Parquet, _) => {
                unreachable!("container formats do not use the stream encoder")
            }
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
