//! COPY capabilities owned by concrete object formats.
//!
//! The connector COPY consumer only coordinates PostgreSQL's COPY driver.
//! Format resolution, object adapters, and native-format completion remain in
//! this module so adding a format does not grow COPY orchestration.

mod canonical_csv;
mod stream;

use pg_lakebase_core::copy::{
    CopyColumnLayout, CopyContext, CopyDataDestination, CopyDataSource, CopyError,
    CopyOptionView,
};
use pg_lakebase_core::storage::foreign::StorageManager;
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::storage::{
    ObjectInput, ObjectOutput, ObjectUri, ObjectWriteTarget, StorageTarget,
};

use super::parquet::{ParquetCopyDestination, ParquetCopySource};
use super::{
    AvroWriteCompression, FormatKind, ParquetWriteCompression, StreamCompression,
};

pub(super) use canonical_csv::{CanonicalCsv, CanonicalCsvRow};

/// COPY source constructed by one resolved format.
pub(crate) trait FormatCopySource {
    fn source(&mut self) -> &mut dyn CopyDataSource;

    fn postgres_options(&self, context: &CopyContext<'_>) -> *mut pg_sys::List {
        CanonicalCsv::postgres_options(context)
    }
}

/// COPY destination constructed by one resolved format.
pub(crate) trait FormatCopyDestination {
    fn destination(&mut self) -> &mut dyn CopyDataDestination;

    fn postgres_options(&self, context: &CopyContext<'_>) -> *mut pg_sys::List {
        CanonicalCsv::postgres_options(context)
    }

    fn finish(self: Box<Self>) -> Result<(), CopyError>;
}

/// COPY-specific format state.
///
/// Container reads discover their compression from object metadata. Container
/// writes retain their validated output codec. Stream formats retain the
/// explicit or suffix-derived stream codec.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResolvedCopyFormat {
    Text(StreamCompression),
    Csv(StreamCompression),
    Json(StreamCompression),
    AvroRead,
    AvroWrite(AvroWriteCompression),
    ParquetRead,
    ParquetWrite(ParquetWriteCompression),
}

impl ResolvedCopyFormat {
    pub(crate) fn resolve(
        options: CopyOptionView<'_>,
        kind: FormatKind,
        object: &ObjectUri,
        copy_from: bool,
        explicit_compression: Option<&str>,
    ) -> Result<Self, ConnectorError> {
        Self::validate_options(options, kind)?;
        let suffix = StreamCompression::from_suffix(object.key());
        match kind {
            FormatKind::Text | FormatKind::Csv | FormatKind::Json => {
                let compression = match explicit_compression {
                    Some(value) => StreamCompression::parse(value).ok_or_else(|| {
                        ConnectorError::invalid_copy_option(
                            "compression",
                            "must be none, gzip, or zstd for a stream format",
                        )
                    })?,
                    None => suffix.unwrap_or(StreamCompression::None),
                };
                Ok(match kind {
                    FormatKind::Text => Self::Text(compression),
                    FormatKind::Csv => Self::Csv(compression),
                    FormatKind::Json => Self::Json(compression),
                    FormatKind::Avro | FormatKind::Parquet => unreachable!(),
                })
            }
            FormatKind::Avro if copy_from => {
                Self::reject_container_read_compression(explicit_compression, suffix)?;
                Ok(Self::AvroRead)
            }
            FormatKind::Avro => {
                Self::reject_container_suffix(suffix)?;
                Ok(Self::AvroWrite(match explicit_compression {
                    Some(value) => AvroWriteCompression::parse(value).ok_or_else(|| {
                        ConnectorError::invalid_copy_option(
                            "compression",
                            "must be none, deflate, snappy, or zstd for avro COPY TO",
                        )
                    })?,
                    None => AvroWriteCompression::default(),
                }))
            }
            FormatKind::Parquet if copy_from => {
                Self::reject_container_read_compression(explicit_compression, suffix)?;
                Ok(Self::ParquetRead)
            }
            FormatKind::Parquet => {
                Self::reject_container_suffix(suffix)?;
                Ok(Self::ParquetWrite(match explicit_compression {
                    Some(value) => ParquetWriteCompression::parse(value).ok_or_else(|| {
                        ConnectorError::invalid_copy_option(
                            "compression",
                            "must be none, snappy, gzip, or zstd for parquet COPY TO",
                        )
                    })?,
                    None => ParquetWriteCompression::default(),
                }))
            }
        }
    }

    pub(crate) fn open_source(
        self,
        target: &StorageTarget,
        column_layout: impl FnOnce() -> Result<CopyColumnLayout, CopyError>,
    ) -> Result<Box<dyn FormatCopySource>, CopyError> {
        match self {
            Self::Text(compression) => {
                Self::open_stream_source(target, compression, FormatKind::Text)
            }
            Self::Csv(compression) => {
                Self::open_stream_source(target, compression, FormatKind::Csv)
            }
            Self::ParquetRead => {
                let manager = StorageManager::from_pg_gucs()?;
                let files = ObjectInput::resolve(target, &manager, FormatKind::Parquet)?.open();
                Ok(Box::new(ParquetCopySource::new(files, &column_layout()?)?))
            }
            Self::Json(_) | Self::AvroRead | Self::AvroWrite(_) | Self::ParquetWrite(_) => {
                Err(ConnectorError::copy_not_implemented(self.kind()).into())
            }
        }
    }

    pub(crate) fn open_destination(
        self,
        target: &StorageTarget,
    ) -> Result<Box<dyn FormatCopyDestination>, CopyError> {
        match self {
            Self::Text(compression) => {
                Self::open_stream_destination(target, compression, FormatKind::Text)
            }
            Self::Csv(compression) => {
                Self::open_stream_destination(target, compression, FormatKind::Csv)
            }
            Self::ParquetWrite(compression) => {
                let manager = StorageManager::from_pg_gucs()?;
                let output = ObjectOutput::resolve(target, &manager, FormatKind::Parquet)?;
                Ok(Box::new(ParquetCopyDestination::new(output, compression)))
            }
            Self::Json(_) | Self::AvroRead | Self::AvroWrite(_) | Self::ParquetRead => {
                Err(ConnectorError::copy_not_implemented(self.kind()).into())
            }
        }
    }

    fn open_stream_source(
        target: &StorageTarget,
        compression: StreamCompression,
        format: FormatKind,
    ) -> Result<Box<dyn FormatCopySource>, CopyError> {
        let object = target.acquire_object_access_from_pg_gucs()?;
        let source = stream::ObjectCopySource::new(
            object.open().map_err(CopyError::storage)?,
            compression,
        )?;
        Ok(Box::new(StreamCopySource {
            source,
            format,
        }))
    }

    fn open_stream_destination(
        target: &StorageTarget,
        compression: StreamCompression,
        format: FormatKind,
    ) -> Result<Box<dyn FormatCopyDestination>, CopyError> {
        let destination = stream::ObjectCopyDestination::new(
            ObjectWriteTarget::exact(target.acquire_object_access_from_pg_gucs()?),
            compression,
        )?;
        Ok(Box::new(StreamCopyDestination {
            destination,
            format,
        }))
    }

    fn reject_container_read_compression(
        explicit: Option<&str>,
        suffix: Option<StreamCompression>,
    ) -> Result<(), ConnectorError> {
        if explicit.is_some() || suffix.is_some() {
            return Err(ConnectorError::invalid_copy_option(
                "compression",
                "must be omitted when reading Parquet or Avro; the container records its codec",
            ));
        }
        Ok(())
    }

    fn reject_container_suffix(suffix: Option<StreamCompression>) -> Result<(), ConnectorError> {
        if suffix.is_some() {
            return Err(ConnectorError::invalid_copy_option(
                "compression",
                "a stream-compression suffix is not valid for Parquet or Avro",
            ));
        }
        Ok(())
    }

    fn validate_options(
        options: CopyOptionView<'_>,
        kind: FormatKind,
    ) -> Result<(), ConnectorError> {
        if matches!(kind, FormatKind::Text | FormatKind::Csv) {
            return Ok(());
        }
        CanonicalCsv::reject_user_overrides(options)
    }

    pub(crate) const fn kind(self) -> FormatKind {
        match self {
            Self::Text(_) => FormatKind::Text,
            Self::Csv(_) => FormatKind::Csv,
            Self::Json(_) => FormatKind::Json,
            Self::AvroRead | Self::AvroWrite(_) => FormatKind::Avro,
            Self::ParquetRead | Self::ParquetWrite(_) => FormatKind::Parquet,
        }
    }
}

struct StreamCopySource {
    source: stream::ObjectCopySource,
    format: FormatKind,
}

impl CopyDataSource for StreamCopySource {
    fn read(&mut self, output: &mut [u8], min_read: usize) -> Result<usize, CopyError> {
        self.source.read(output, min_read)
    }
}

impl FormatCopySource for StreamCopySource {
    fn source(&mut self) -> &mut dyn CopyDataSource {
        &mut self.source
    }

    fn postgres_options(&self, context: &CopyContext<'_>) -> *mut pg_sys::List {
        postgres_options(context, self.format)
    }
}

struct StreamCopyDestination {
    destination: stream::ObjectCopyDestination,
    format: FormatKind,
}

impl CopyDataDestination for StreamCopyDestination {
    fn write_row(&mut self, data: &[u8]) -> Result<(), CopyError> {
        self.destination.write_row(data)
    }
}

impl FormatCopyDestination for StreamCopyDestination {
    fn destination(&mut self) -> &mut dyn CopyDataDestination {
        &mut self.destination
    }

    fn postgres_options(&self, context: &CopyContext<'_>) -> *mut pg_sys::List {
        postgres_options(context, self.format)
    }

    fn finish(self: Box<Self>) -> Result<(), CopyError> {
        let Self { destination, .. } = *self;
        destination.finish()
    }
}

fn postgres_options(context: &CopyContext<'_>, format: FormatKind) -> *mut pg_sys::List {
    let options = context.statement().option_view().without_names(&[
        b"storage_server".as_slice(),
        b"format".as_slice(),
        b"compression".as_slice(),
    ]);
    let value = match format {
        FormatKind::Text => c"text",
        FormatKind::Csv => c"csv",
        FormatKind::Json | FormatKind::Avro | FormatKind::Parquet => {
            unreachable!("only PostgreSQL text and csv formats use this COPY bridge")
        }
    };
    unsafe {
        let option = pg_sys::makeDefElem(
            pg_sys::pstrdup(c"format".as_ptr()),
            pg_sys::makeString(pg_sys::pstrdup(value.as_ptr())).cast(),
            -1,
        );
        pg_sys::lappend(options, option.cast())
    }
}
