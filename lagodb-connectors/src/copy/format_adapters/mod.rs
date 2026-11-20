//! Format-specific PostgreSQL COPY adapters over shared format components.
//!
//! COPY direction, transport, PG option translation, and execution state stay
//! here. File codecs remain in `format` for both FDW and COPY consumers.
//! Format selection performs no I/O; core drivers validate the statement before
//! a source or destination opens its object.

mod avro;
mod json;
mod parquet;
mod stream;

use lagodb_core::copy::{
    CopyColumnLayout, CopyContext, CopyDataDestination, CopyDatumSource, CopyError,
    CopyOnError, CopyOptionView, CopyTupleDestination,
};
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::format::{
    AvroWriteCompression, DelimitedFormat, FormatKind, ParquetWriteCompression,
    StreamCompression,
};
use crate::gucs::WriteConfig;
use crate::storage::{ObjectLocationKind, ResolvedStorageLocation, StoragePath};

pub(super) use stream::StreamCopySource;

use avro::{AvroCopyDestination, AvroCopySource};
use json::{JsonCopyDestination, JsonCopySource};
use parquet::{ParquetCopyDestination, ParquetCopySource};
use stream::StreamCopyDestination;

const CONNECTOR_OPTION_NAMES: [&[u8]; 3] = [b"server", b"format", b"compression"];
const NATIVE_INVALID_OPTION_NAMES: [&[u8]; 10] = [
    b"delimiter",
    b"null",
    b"default",
    b"header",
    b"quote",
    b"escape",
    b"encoding",
    b"force_quote",
    b"force_not_null",
    b"force_null",
];

/// One COPY statement's format, resolved for its direction.
pub(crate) enum CopyFormat {
    From(CopyFromFormat),
    To(CopyToFormat),
}

/// COPY FROM format state, split by the core driver that executes it.
/// Container reads discover their codec from object metadata; stream formats
/// retain the explicit or suffix-derived codec.
#[derive(Clone, Copy)]
pub(crate) enum CopyFromFormat {
    /// PostgreSQL parses text and CSV through the byte COPY driver.
    Delimited(DelimitedFormat, StreamCompression),
    /// Connector codecs produce Datums for the typed COPY driver.
    Typed(TypedCopyFromFormat),
}

#[derive(Clone, Copy)]
pub(crate) enum TypedCopyFromFormat {
    Json(StreamCompression),
    Avro,
    Parquet,
}

/// COPY TO format state with the validated output codec.
#[derive(Clone, Copy)]
pub(crate) enum CopyToFormat {
    Delimited(DelimitedFormat, StreamCompression),
    Json(StreamCompression),
    Avro(AvroWriteCompression),
    Parquet(ParquetWriteCompression),
}

pub(crate) enum FormatCopyOutput<'a> {
    Bytes(&'a mut dyn CopyDataDestination),
    Tuples(&'a mut dyn CopyTupleDestination),
}

/// COPY TO destination constructed by one resolved format.
pub(crate) trait FormatCopyDestination {
    fn output(&mut self) -> FormatCopyOutput<'_>;

    fn postgres_options(&self, context: &CopyContext<'_>) -> *mut pg_sys::List {
        CopyFormat::postgres_options(context, None)
    }
}

impl CopyFormat {
    pub(crate) fn resolve(
        options: CopyOptionView<'_>,
        kind: FormatKind,
        object: &StoragePath,
        copy_from: bool,
        explicit_compression: Option<&str>,
    ) -> Result<Self, ConnectorError> {
        Self::validate_options(options, kind)?;
        let suffix = StreamCompression::from_suffix(object.key());
        if copy_from {
            CopyFromFormat::resolve(kind, explicit_compression, suffix)
                .map(Self::From)
        } else {
            CopyToFormat::resolve(kind, explicit_compression, suffix).map(Self::To)
        }
    }

    fn resolve_stream_compression(
        explicit: Option<&str>,
        suffix: Option<StreamCompression>,
    ) -> Result<StreamCompression, ConnectorError> {
        match explicit {
            Some(value) => StreamCompression::parse(value).ok_or_else(|| {
                ConnectorError::invalid_option(
                    "compression",
                    "must be none, gzip, or zstd for a stream format",
                )
            }),
            None => Ok(suffix.unwrap_or(StreamCompression::None)),
        }
    }

    fn validate_options(
        options: CopyOptionView<'_>,
        kind: FormatKind,
    ) -> Result<(), ConnectorError> {
        if matches!(kind, FormatKind::Text | FormatKind::Csv) {
            return Ok(());
        }
        for option in options.iter() {
            if NATIVE_INVALID_OPTION_NAMES
                .iter()
                .any(|candidate| *candidate == option.name().to_bytes())
            {
                let name = option.name().to_str().map_err(|_| {
                    ConnectorError::invalid_option(
                        "COPY option",
                        "must be valid UTF-8",
                    )
                })?;
                return Err(ConnectorError::invalid_option(
                    name,
                    "is only valid for text or csv",
                ));
            }
        }
        Ok(())
    }

    /// PostgreSQL COPY options without connector-owned names. Byte formats
    /// name their PostgreSQL format; native formats use PG's default options.
    fn postgres_options(
        context: &CopyContext<'_>,
        delimited: Option<DelimitedFormat>,
    ) -> *mut pg_sys::List {
        let options = context
            .statement()
            .option_view()
            .without_names(&CONNECTOR_OPTION_NAMES);
        let Some(delimited) = delimited else {
            return options;
        };
        let value = match delimited {
            DelimitedFormat::Text => c"text",
            DelimitedFormat::Csv => c"csv",
        };
        // SAFETY: COPY retains this PG-owned option list through its driver.
        unsafe {
            let option = pg_sys::makeDefElem(
                pg_sys::pstrdup(c"format".as_ptr()),
                pg_sys::makeString(pg_sys::pstrdup(value.as_ptr())).cast(),
                -1,
            );
            pg_sys::lappend(options, option.cast())
        }
    }
}

impl CopyFromFormat {
    fn resolve(
        kind: FormatKind,
        explicit_compression: Option<&str>,
        suffix: Option<StreamCompression>,
    ) -> Result<Self, ConnectorError> {
        let stream =
            || CopyFormat::resolve_stream_compression(explicit_compression, suffix);
        match kind {
            FormatKind::Text => Ok(Self::Delimited(DelimitedFormat::Text, stream()?)),
            FormatKind::Csv => Ok(Self::Delimited(DelimitedFormat::Csv, stream()?)),
            FormatKind::Json => Ok(Self::Typed(TypedCopyFromFormat::Json(stream()?))),
            FormatKind::Avro | FormatKind::Parquet
                if explicit_compression.is_some() || suffix.is_some() =>
            {
                Err(ConnectorError::invalid_option(
                    "compression",
                    "must be omitted when reading Parquet or Avro; the container records its codec",
                ))
            }
            FormatKind::Avro => Ok(Self::Typed(TypedCopyFromFormat::Avro)),
            FormatKind::Parquet => Ok(Self::Typed(TypedCopyFromFormat::Parquet)),
        }
    }

    pub(crate) const fn kind(self) -> FormatKind {
        match self {
            Self::Delimited(delimited, _) => delimited.kind(),
            Self::Typed(typed) => typed.kind(),
        }
    }

    pub(crate) fn postgres_options(
        self,
        context: &CopyContext<'_>,
    ) -> *mut pg_sys::List {
        let delimited = match self {
            Self::Delimited(delimited, _) => Some(delimited),
            Self::Typed(_) => None,
        };
        CopyFormat::postgres_options(context, delimited)
    }

    /// Parquet COPY FROM reads every matching object under a prefix. The other
    /// formats accept one exact object only.
    pub(crate) fn validate_location(
        self,
        location: &ResolvedStorageLocation,
    ) -> Result<(), ConnectorError> {
        let kind = self.kind();
        let location_kind = kind.resolved_location_kind(location)?;
        if kind != FormatKind::Parquet && location_kind != ObjectLocationKind::Exact {
            return Err(ConnectorError::copy_from_exact_only(kind));
        }
        Ok(())
    }
}

impl TypedCopyFromFormat {
    const fn kind(self) -> FormatKind {
        match self {
            Self::Json(_) => FormatKind::Json,
            Self::Avro => FormatKind::Avro,
            Self::Parquet => FormatKind::Parquet,
        }
    }

    /// JSON converts text through PostgreSQL's soft-error input functions, as
    /// text COPY does. Avro and Parquet coerce typed datums through functions
    /// whose errors are hard, so they cannot skip a row safely.
    pub(crate) fn validate_on_error(
        self,
        on_error: CopyOnError,
    ) -> Result<(), ConnectorError> {
        match (self, on_error) {
            (Self::Json(_), _) | (Self::Avro | Self::Parquet, CopyOnError::Stop) => {
                Ok(())
            }
            (Self::Avro | Self::Parquet, CopyOnError::Ignore) => {
                Err(ConnectorError::copy_on_error_unsupported(self.kind()))
            }
        }
    }

    /// Opens the Datum source for PostgreSQL's accepted column layout. The
    /// caller has validated `ON_ERROR` and the location policy.
    pub(crate) fn open_source(
        self,
        location: &ResolvedStorageLocation,
        layout: &CopyColumnLayout,
    ) -> Result<Box<dyn CopyDatumSource>, CopyError> {
        Ok(match self {
            Self::Json(compression) => {
                Box::new(JsonCopySource::open(location, layout, compression)?)
            }
            Self::Avro => Box::new(AvroCopySource::open(location, layout)?),
            Self::Parquet => Box::new(ParquetCopySource::open(location, layout)?),
        })
    }
}

impl CopyToFormat {
    fn resolve(
        kind: FormatKind,
        explicit_compression: Option<&str>,
        suffix: Option<StreamCompression>,
    ) -> Result<Self, ConnectorError> {
        let stream =
            || CopyFormat::resolve_stream_compression(explicit_compression, suffix);
        match kind {
            FormatKind::Text => Ok(Self::Delimited(DelimitedFormat::Text, stream()?)),
            FormatKind::Csv => Ok(Self::Delimited(DelimitedFormat::Csv, stream()?)),
            FormatKind::Json => Ok(Self::Json(stream()?)),
            FormatKind::Avro | FormatKind::Parquet if suffix.is_some() => {
                Err(ConnectorError::invalid_option(
                    "compression",
                    "a stream-compression suffix is not valid for Parquet or Avro",
                ))
            }
            FormatKind::Avro => Ok(Self::Avro(match explicit_compression {
                Some(value) => {
                    AvroWriteCompression::parse(value).ok_or_else(|| {
                        ConnectorError::invalid_option(
                            "compression",
                            "must be none, deflate, snappy, or zstd for avro COPY TO",
                        )
                    })?
                }
                None => AvroWriteCompression::default(),
            })),
            FormatKind::Parquet => Ok(Self::Parquet(match explicit_compression {
                Some(value) => {
                    ParquetWriteCompression::parse(value).ok_or_else(|| {
                        ConnectorError::invalid_option(
                            "compression",
                            "must be none, snappy, gzip, or zstd for parquet COPY TO",
                        )
                    })?
                }
                None => ParquetWriteCompression::default(),
            })),
        }
    }

    const fn kind(self) -> FormatKind {
        match self {
            Self::Delimited(delimited, _) => delimited.kind(),
            Self::Json(_) => FormatKind::Json,
            Self::Avro(_) => FormatKind::Avro,
            Self::Parquet(_) => FormatKind::Parquet,
        }
    }

    pub(crate) fn open_destination(
        self,
        location: &ResolvedStorageLocation,
    ) -> Result<Box<dyn FormatCopyDestination>, ConnectorError> {
        let output = self
            .kind()
            .output(location, || WriteConfig::from_guc().target_file_bytes())?;
        Ok(match self {
            Self::Delimited(delimited, compression) => {
                Box::new(StreamCopyDestination::new(output, compression, delimited))
            }
            Self::Json(compression) => {
                Box::new(JsonCopyDestination::new(output, compression))
            }
            Self::Avro(compression) => {
                Box::new(AvroCopyDestination::new(output, compression))
            }
            Self::Parquet(compression) => {
                Box::new(ParquetCopyDestination::new(output, compression))
            }
        })
    }
}
