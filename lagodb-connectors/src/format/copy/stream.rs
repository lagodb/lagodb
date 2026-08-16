//! Object-backed byte adapters for PostgreSQL COPY.

use std::io::{self, Read};
use std::mem;

use pg_lakebase_core::copy::{CopyDataDestination, CopyDataSource, CopyError};
use pg_lakebase_storage::StorageFile;

use crate::error::ConnectorError;
use crate::format::{
    EmptyOutputPolicy, FormatKind, ObjectSetWriter, StreamCompression, StreamDecoder,
    StreamEncoderFactory,
};
use crate::storage::ObjectOutput;

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

enum StreamDestinationState {
    AwaitingHeader {
        output: ObjectOutput,
        factory: StreamEncoderFactory,
    },
    Writing(Box<ObjectSetWriter<StreamEncoderFactory>>),
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
            StreamDestinationState::Writing(Box::new(ObjectSetWriter::new(
                output, factory,
            )))
        };
        Self { state }
    }

    pub(super) fn finish(self) -> Result<(), CopyError> {
        let StreamDestinationState::Writing(writer) = self.state else {
            unreachable!(
                "PostgreSQL emits the requested COPY header before completion"
            )
        };
        (*writer)
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
        } = mem::replace(&mut self.state, StreamDestinationState::Transitioning)
        else {
            unreachable!("the transition state never escapes header handling")
        };
        factory.set_header(data.into());
        self.state = StreamDestinationState::Writing(Box::new(ObjectSetWriter::new(
            output, factory,
        )));
        Ok(())
    }
}
