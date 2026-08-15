//! PostgreSQL COPY-backed Text/CSV Foreign Table scans.

use std::io::{self, Read};

use pg_lakebase_core::copy::{
    CopyDataSource, CopyDocumentSource, CopyError, CopyFromScan,
};
use pg_lakebase_core::fdw::{
    BeginForeignScanContext, ForeignPathBuilder, ForeignPathContext, ForeignPathKeys,
    ForeignPathSpec, ForeignPlanContext, ForeignPlanSpec, ForeignRelSize,
    ForeignRelSizeContext, ReScanForeignScanContext, ScanProjectionPolicy,
    ScanSlotWriter,
};
use pg_lakebase_storage::StorageFile;
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::fdw::Lakebase;

use super::scan::{FormatScanPlanner, FormatScanPrivate, FormatScanState};
use super::{FormatKind, StreamCompression, StreamDecoder};
use crate::storage::ObjectFiles;

const DEFAULT_ESTIMATED_ROWS: f64 = 1_000.0;
const DEFAULT_ESTIMATED_WIDTH: i32 = 32;

pub(super) struct DelimitedScanPlanner {
    kind: FormatKind,
}

impl DelimitedScanPlanner {
    pub(super) const fn new(kind: FormatKind) -> Self {
        Self { kind }
    }
}

impl FormatScanPlanner for DelimitedScanPlanner {
    fn estimate(
        &mut self,
        _context: &ForeignRelSizeContext<'_>,
    ) -> Result<ForeignRelSize, ConnectorError> {
        Ok(ForeignRelSize::new(
            DEFAULT_ESTIMATED_ROWS,
            DEFAULT_ESTIMATED_WIDTH,
        ))
    }

    fn build_paths(
        &self,
        _context: &ForeignPathContext<'_>,
        paths: &mut ForeignPathBuilder<FormatScanPrivate>,
    ) -> Result<(), ConnectorError> {
        paths.push(ForeignPathSpec::new(
            DEFAULT_ESTIMATED_ROWS,
            0.0,
            DEFAULT_ESTIMATED_ROWS,
            FormatScanPrivate::new(self.kind),
        ));
        Ok(())
    }

    fn supports_pathkeys(
        &self,
        _context: &ForeignPathContext<'_>,
        _pathkeys: &mut ForeignPathKeys,
    ) -> Result<bool, ConnectorError> {
        Ok(false)
    }

    fn build_plan(
        &mut self,
        context: &ForeignPlanContext<'_, Lakebase>,
    ) -> Result<ForeignPlanSpec<FormatScanPrivate>, ConnectorError> {
        let mut plan = ForeignPlanSpec::new(context.path_private().to_owned());
        plan.projection_policy = ScanProjectionPolicy::RequireRelationShape;
        Ok(plan)
    }
}

pub(super) struct DelimitedScanState {
    decoder: CopyFromScan,
}

impl DelimitedScanState {
    pub(super) fn begin(
        context: BeginForeignScanContext<'_, Lakebase>,
        files: ObjectFiles,
        compression: StreamCompression,
        postgres_options: *mut pg_sys::List,
    ) -> Result<Self, ConnectorError> {
        let source = DelimitedObjectSource::new(files, compression);
        // SAFETY: the executor owns the relation, expression context, and
        // PostgreSQL option list for this scan lifetime; the boxed source is
        // retained by CopyFromScan until its callback guard is removed.
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
        context: ReScanForeignScanContext<'_, Lakebase>,
    ) -> Result<(), ConnectorError> {
        Ok(self.decoder.rescan(context.econtext)?)
    }

    fn end(&mut self) -> Result<(), ConnectorError> {
        Ok(self.decoder.end()?)
    }
}

struct DelimitedObjectSource {
    files: ObjectFiles,
    compression: StreamCompression,
    decoder: Option<StreamDecoder<ObjectReader>>,
}

impl DelimitedObjectSource {
    fn new(files: ObjectFiles, compression: StreamCompression) -> Self {
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
        let file = file.map_err(ConnectorError::from)?;
        let decoder = StreamDecoder::new(ObjectReader { file }, self.compression)
            .map_err(ConnectorError::copy_stream_io)?;
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
            ConnectorError::copy_stream_io(io::Error::other(
                "COPY parser requested bytes without an active object",
            ))
        })?;
        let read = decoder
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
