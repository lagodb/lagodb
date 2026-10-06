//! COPY format factories. Transport and PG options are selected without I/O;
//! core drivers validate the statement before constructing an input reader.

mod json;
mod options;
mod stream;

use lagodb_core::copy::{
    CopyColumnLayout, CopyContext, CopyDataDestination, CopyDataSource,
    CopyDatumSource, CopyError, CopyTupleDestination,
};
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::gucs::WriteConfig;
use crate::storage::{ObjectLocationKind, ResolvedStorageLocation};

use super::avro::{AvroCopyDestination, AvroCopySource};
use super::delimited::DelimitedFormat;
use super::parquet::{ParquetCopyDestination, ParquetCopySource};
use super::{FormatKind, StreamCompression};

use json::{JsonCopyDestination, JsonCopySource};
pub(crate) use options::ResolvedCopyFormat;
use stream::{StreamCopyDestination, StreamCopySource};

/// Input transport selected before PostgreSQL initializes the COPY state.
pub(crate) enum FormatCopyInput {
    Bytes,
    Datums,
}

pub(crate) enum FormatCopyOutput<'a> {
    Bytes(&'a mut dyn CopyDataDestination),
    Tuples(&'a mut dyn CopyTupleDestination),
}

/// COPY destination constructed by one resolved format.
pub(crate) trait FormatCopyDestination {
    fn output(&mut self) -> FormatCopyOutput<'_>;

    fn postgres_options(&self, context: &CopyContext<'_>) -> *mut pg_sys::List {
        ResolvedCopyFormat::postgres_options(context, None)
    }
}

impl ResolvedCopyFormat {
    pub(crate) const fn input(self) -> FormatCopyInput {
        match self {
            Self::Text(_) | Self::Csv(_) => FormatCopyInput::Bytes,
            _ => FormatCopyInput::Datums,
        }
    }

    pub(crate) fn input_options(
        self,
        context: &CopyContext<'_>,
    ) -> *mut pg_sys::List {
        let stream = match self {
            Self::Text(_) => Some(DelimitedFormat::Text),
            Self::Csv(_) => Some(DelimitedFormat::Csv),
            _ => None,
        };
        Self::postgres_options(context, stream)
    }

    pub(crate) fn open_byte_source(
        self,
        location: &ResolvedStorageLocation,
    ) -> Result<Box<dyn CopyDataSource>, CopyError> {
        let (compression, format) = match self {
            Self::Text(compression) => (compression, DelimitedFormat::Text),
            Self::Csv(compression) => (compression, DelimitedFormat::Csv),
            _ => unreachable!("only text and CSV use the COPY byte driver"),
        };
        let kind = format.kind();
        if kind.location_kind(location.path())? != ObjectLocationKind::Exact {
            return Err(ConnectorError::copy_from_exact_only(kind).into());
        }
        Ok(Box::new(StreamCopySource::new(
            location.open_file()?,
            compression,
        )?))
    }

    pub(crate) fn open_datum_source(
        self,
        location: &ResolvedStorageLocation,
        layout: &CopyColumnLayout,
    ) -> Result<Box<dyn CopyDatumSource>, CopyError> {
        match self {
            Self::ParquetRead => {
                let files = FormatKind::Parquet.input(location)?.open();
                Ok(Box::new(ParquetCopySource::new(files, layout)?))
            }
            Self::AvroRead => {
                if FormatKind::Avro.location_kind(location.path())?
                    != ObjectLocationKind::Exact
                {
                    return Err(ConnectorError::copy_from_exact_only(
                        FormatKind::Avro,
                    )
                    .into());
                }
                let files = FormatKind::Avro.input(location)?.open();
                Ok(Box::new(AvroCopySource::new(files, layout)?))
            }
            Self::Json(compression) => {
                if FormatKind::Json.location_kind(location.path())?
                    != ObjectLocationKind::Exact
                {
                    return Err(ConnectorError::copy_from_exact_only(
                        FormatKind::Json,
                    )
                    .into());
                }
                let files = FormatKind::Json.input(location)?.open();
                Ok(Box::new(JsonCopySource::new(files, layout, compression)?))
            }
            Self::AvroWrite(_) | Self::ParquetWrite(_) => {
                Err(ConnectorError::copy_not_implemented(self.kind()).into())
            }
            Self::Text(_) | Self::Csv(_) => {
                unreachable!("text and CSV use the COPY byte driver")
            }
        }
    }

    pub(crate) fn open_destination(
        self,
        location: &ResolvedStorageLocation,
    ) -> Result<Box<dyn FormatCopyDestination>, CopyError> {
        match self {
            Self::Text(compression) => Self::open_stream_destination(
                location,
                compression,
                DelimitedFormat::Text,
            ),
            Self::Csv(compression) => Self::open_stream_destination(
                location,
                compression,
                DelimitedFormat::Csv,
            ),
            Self::ParquetWrite(compression) => {
                let output = FormatKind::Parquet.output(location, || {
                    WriteConfig::from_guc().target_file_bytes()
                })?;
                Ok(Box::new(ParquetCopyDestination::new(output, compression)))
            }
            Self::AvroWrite(compression) => {
                let output = FormatKind::Avro.output(location, || {
                    WriteConfig::from_guc().target_file_bytes()
                })?;
                Ok(Box::new(AvroCopyDestination::new(output, compression)))
            }
            Self::Json(compression) => {
                let output = FormatKind::Json.output(location, || {
                    WriteConfig::from_guc().target_file_bytes()
                })?;
                Ok(Box::new(JsonCopyDestination::new(output, compression)))
            }
            Self::AvroRead | Self::ParquetRead => {
                Err(ConnectorError::copy_not_implemented(self.kind()).into())
            }
        }
    }

    fn open_stream_destination(
        location: &ResolvedStorageLocation,
        compression: StreamCompression,
        format: DelimitedFormat,
    ) -> Result<Box<dyn FormatCopyDestination>, CopyError> {
        let output = format
            .kind()
            .output(location, || WriteConfig::from_guc().target_file_bytes())?;
        Ok(Box::new(StreamCopyDestination::new(
            output,
            compression,
            format,
        )))
    }
}
