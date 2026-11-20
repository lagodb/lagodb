//! PostgreSQL COPY-backed Text/CSV Foreign Table scans.

use std::io;

use lagodb_core::copy::{CopyDataSource, CopyError};
use lagodb_core::fdw::{
    CopyDocumentSource, CopyFromScan, ReScanForeignScanContext, ScanSlotWriter,
    StartForeignScanContext,
};
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::fdw::LagodbConnectors;
use crate::format::{StreamCompressionOptions, StreamDecoder};
use crate::storage::{InputFile, InputFiles};

use super::FormatScanState;

pub(super) struct DelimitedScanState {
    decoder: CopyFromScan,
}

impl DelimitedScanState {
    pub(super) fn begin(
        context: StartForeignScanContext<'_, LagodbConnectors>,
        files: InputFiles,
        compression: StreamCompressionOptions,
        postgres_options: *mut pg_sys::List,
    ) -> Result<Self, ConnectorError> {
        let source = DelimitedObjectSource::new(files, compression);
        // SAFETY: the executor owns the relation, expression context, and
        // PostgreSQL option list for this scan lifetime; the boxed source is
        // retained by CopyFromScan for every synchronous parser invocation.
        let decoder = unsafe {
            CopyFromScan::begin(
                context.relation.as_raw(),
                context.econtext,
                postgres_options,
                Box::new(source),
            )
        }?;
        Ok(Self { decoder })
    }
}

impl FormatScanState for DelimitedScanState {
    fn next_slot(
        &mut self,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ConnectorError> {
        Ok(self.decoder.next_slot(output)?)
    }

    fn rescan(
        &mut self,
        context: ReScanForeignScanContext<'_, LagodbConnectors>,
    ) -> Result<(), ConnectorError> {
        Ok(self.decoder.rescan(context.econtext)?)
    }

    fn end(&mut self) -> Result<(), ConnectorError> {
        self.decoder.end();
        Ok(())
    }
}

struct DelimitedObjectSource {
    files: InputFiles,
    compression: StreamCompressionOptions,
    decoder: Option<StreamDecoder<InputFile>>,
}

impl DelimitedObjectSource {
    fn new(files: InputFiles, compression: StreamCompressionOptions) -> Self {
        Self {
            files,
            compression,
            decoder: None,
        }
    }
}

impl CopyDocumentSource for DelimitedObjectSource {
    fn copy_data_source(&mut self) -> &mut dyn CopyDataSource {
        self
    }

    fn next_document(&mut self) -> Result<bool, CopyError> {
        self.decoder = None;
        let Some(file) = self.files.next() else {
            return Ok(false);
        };
        let file = file?;
        let compression = self.compression.for_file(&file);
        let decoder = StreamDecoder::new(file, compression)
            .map_err(ConnectorError::stream_io)?;
        self.decoder = Some(decoder);
        Ok(true)
    }

    fn reset(&mut self) -> Result<(), CopyError> {
        self.decoder = None;
        self.files.reset();
        Ok(())
    }
}

impl CopyDataSource for DelimitedObjectSource {
    fn read(
        &mut self,
        output: &mut [u8],
        min_read: usize,
    ) -> Result<usize, CopyError> {
        let decoder = self.decoder.as_mut().ok_or_else(|| {
            ConnectorError::stream_io(io::Error::other(
                "COPY parser requested bytes without an active object",
            ))
        })?;
        let read = decoder
            .read_at_least(output, min_read)
            .map_err(ConnectorError::stream_io)?;
        Ok(read)
    }
}
