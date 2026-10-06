//! Object-backed byte adapters for PostgreSQL COPY.

use lagodb_core::copy::{
    CopyColumnLayout, CopyContext, CopyDataDestination, CopyDataSource, CopyError,
    CopyOutputProgress,
};
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::format::{
    EmptyOutputPolicy, ObjectSetWriter, StreamCompression, StreamDecoder,
    StreamEncoderFactory,
};
use crate::storage::{InputFile, ObjectOutput};

use super::super::delimited::DelimitedFormat;
use super::{FormatCopyDestination, FormatCopyOutput, ResolvedCopyFormat};

pub(super) struct StreamCopySource {
    decoder: StreamDecoder<InputFile>,
}

impl StreamCopySource {
    pub(super) fn new(
        file: InputFile,
        compression: StreamCompression,
    ) -> Result<Self, CopyError> {
        let decoder = StreamDecoder::new(file, compression)
            .map_err(ConnectorError::copy_stream_io)?;
        Ok(Self { decoder })
    }
}

impl CopyDataSource for StreamCopySource {
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

enum StreamDestinationState {
    Uninitialized {
        output: ObjectOutput,
        factory: StreamEncoderFactory,
    },
    AwaitingHeader {
        output: ObjectOutput,
        factory: StreamEncoderFactory,
        progress: CopyOutputProgress,
    },
    Writing(Box<ObjectSetWriter<StreamEncoderFactory>>),
}

pub(super) struct StreamCopyDestination {
    state: Option<StreamDestinationState>,
    completed_bytes: u64,
    format: DelimitedFormat,
}

impl StreamCopyDestination {
    pub(super) fn new(
        output: ObjectOutput,
        compression: StreamCompression,
        format: DelimitedFormat,
    ) -> Self {
        let factory = StreamEncoderFactory::new(format.stream(), compression);
        Self {
            state: Some(StreamDestinationState::Uninitialized { output, factory }),
            completed_bytes: 0,
            format,
        }
    }
}

impl CopyDataDestination for StreamCopyDestination {
    fn initialize(
        &mut self,
        _layout: &CopyColumnLayout,
        has_header: bool,
        progress: CopyOutputProgress,
    ) -> Result<(), CopyError> {
        let Some(StreamDestinationState::Uninitialized { output, factory }) =
            self.state.take()
        else {
            unreachable!("COPY initializes its destination exactly once")
        };
        self.state = Some(if has_header {
            StreamDestinationState::AwaitingHeader {
                output,
                factory,
                progress,
            }
        } else {
            let mut writer = ObjectSetWriter::new(output, factory)?;
            writer.set_copy_progress(progress);
            StreamDestinationState::Writing(Box::new(writer))
        });
        Ok(())
    }

    fn write_row(&mut self, data: &[u8]) -> Result<(), CopyError> {
        if let Some(StreamDestinationState::Writing(writer)) = &mut self.state {
            return writer.write(data).map_err(CopyError::from);
        }
        let Some(StreamDestinationState::AwaitingHeader {
            output,
            mut factory,
            progress,
        }) = self.state.take()
        else {
            unreachable!("COPY writes rows only while its destination is active")
        };
        factory.set_header(data.into());
        let mut writer = ObjectSetWriter::new(output, factory)?;
        writer.set_copy_progress(progress);
        self.state = Some(StreamDestinationState::Writing(Box::new(writer)));
        Ok(())
    }

    fn bytes_produced(&self) -> u64 {
        match &self.state {
            Some(StreamDestinationState::Writing(writer)) => writer.bytes_written(),
            _ => self.completed_bytes,
        }
    }

    fn finish(&mut self) -> Result<(), CopyError> {
        let Some(StreamDestinationState::Writing(writer)) = self.state.take() else {
            unreachable!(
                "PostgreSQL emits the requested COPY header before completion"
            );
        };
        self.completed_bytes = (*writer)
            .finish_with_bytes(EmptyOutputPolicy::EmitFile)
            .map_err(CopyError::from)?;
        Ok(())
    }

    fn abort(&mut self) {
        self.state = None;
    }
}

impl FormatCopyDestination for StreamCopyDestination {
    fn output(&mut self) -> FormatCopyOutput<'_> {
        FormatCopyOutput::Bytes(self)
    }

    fn postgres_options(&self, context: &CopyContext<'_>) -> *mut pg_sys::List {
        ResolvedCopyFormat::postgres_options(context, Some(self.format))
    }
}
