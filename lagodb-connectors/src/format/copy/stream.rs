//! Object-backed byte adapters for PostgreSQL COPY.

use std::io::{self, Read, Write};
use std::mem;

use pg_lakebase_core::copy::{CopyDataDestination, CopyDataSource, CopyError};
use pg_lakebase_storage::StorageFile;

use crate::error::ConnectorError;
use crate::format::{
    EmptyOutputPolicy, FileWriteProgress, FormatKind, ObjectFileEncoder,
    ObjectFileEncoderFactory, ObjectSetWriter, StreamCompression, StreamDecoder,
    StreamEncoder,
};
use crate::storage::{
    ObjectFileSuffix, ObjectOutput, StagedObjectWriter,
};

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

struct StreamEncoderFactory {
    compression: StreamCompression,
    suffix: ObjectFileSuffix,
    header: Option<Box<[u8]>>,
}

impl StreamEncoderFactory {
    fn new(format: FormatKind, compression: StreamCompression) -> Self {
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

    fn set_header(&mut self, header: &[u8]) {
        self.header = Some(header.into());
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

struct StreamFileEncoder {
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

enum StreamDestinationState {
    AwaitingHeader {
        output: ObjectOutput,
        factory: StreamEncoderFactory,
    },
    Writing(ObjectSetWriter<StreamEncoderFactory>),
    Transitioning,
}

pub(super) struct ObjectCopyDestination {
    state: StreamDestinationState,
}

impl ObjectCopyDestination {
    pub(super) fn new(
        output: ObjectOutput,
        compression: StreamCompression,
        format: FormatKind,
        header: bool,
    ) -> Self {
        let factory = StreamEncoderFactory::new(format, compression);
        let state = if header {
            StreamDestinationState::AwaitingHeader { output, factory }
        } else {
            StreamDestinationState::Writing(ObjectSetWriter::new(output, factory))
        };
        Self { state }
    }

    pub(super) fn finish(self) -> Result<(), CopyError> {
        let StreamDestinationState::Writing(writer) = self.state else {
            unreachable!("PostgreSQL emits the requested COPY header before completion")
        };
        writer
            .finish(EmptyOutputPolicy::EmitFile)
            .map_err(CopyError::from)
    }
}

impl CopyDataDestination for ObjectCopyDestination {
    fn write_row(&mut self, data: &[u8]) -> Result<(), CopyError> {
        if let StreamDestinationState::Writing(writer) = &mut self.state {
            return writer.write(data).map_err(CopyError::from);
        }
        let StreamDestinationState::AwaitingHeader {
            output,
            mut factory,
        } = mem::replace(
            &mut self.state,
            StreamDestinationState::Transitioning,
        ) else {
            unreachable!("the transition state never escapes header handling")
        };
        factory.set_header(data);
        self.state = StreamDestinationState::Writing(ObjectSetWriter::new(
            output, factory,
        ));
        Ok(())
    }
}
