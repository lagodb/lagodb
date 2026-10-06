//! COPY format options and PostgreSQL option translation.

use lagodb_core::copy::{CopyContext, CopyOptionView};
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::storage::StoragePath;

use super::super::delimited::DelimitedFormat;
use super::super::{
    AvroWriteCompression, FormatKind, ParquetWriteCompression, StreamCompression,
};

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
        object: &StoragePath,
        copy_from: bool,
        explicit_compression: Option<&str>,
    ) -> Result<Self, ConnectorError> {
        Self::validate_options(options, kind)?;
        let suffix = StreamCompression::from_suffix(object.key());
        match kind {
            FormatKind::Text => {
                let compression =
                    Self::resolve_stream_compression(explicit_compression, suffix)?;
                Ok(Self::Text(compression))
            }
            FormatKind::Csv => {
                let compression =
                    Self::resolve_stream_compression(explicit_compression, suffix)?;
                Ok(Self::Csv(compression))
            }
            FormatKind::Json => {
                let compression =
                    Self::resolve_stream_compression(explicit_compression, suffix)?;
                Ok(Self::Json(compression))
            }
            FormatKind::Avro if copy_from => {
                Self::reject_container_read_compression(
                    explicit_compression,
                    suffix,
                )?;
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
                Self::reject_container_read_compression(
                    explicit_compression,
                    suffix,
                )?;
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

    fn resolve_stream_compression(
        explicit: Option<&str>,
        suffix: Option<StreamCompression>,
    ) -> Result<StreamCompression, ConnectorError> {
        match explicit {
            Some(value) => StreamCompression::parse(value).ok_or_else(|| {
                ConnectorError::invalid_copy_option(
                    "compression",
                    "must be none, gzip, or zstd for a stream format",
                )
            }),
            None => Ok(suffix.unwrap_or(StreamCompression::None)),
        }
    }

    fn reject_container_suffix(
        suffix: Option<StreamCompression>,
    ) -> Result<(), ConnectorError> {
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
        for option in options.iter() {
            if NATIVE_INVALID_OPTION_NAMES
                .iter()
                .any(|candidate| *candidate == option.name().to_bytes())
            {
                let name = option.name().to_str().map_err(|_| {
                    ConnectorError::invalid_copy_option(
                        "COPY option",
                        "must be valid UTF-8",
                    )
                })?;
                return Err(ConnectorError::invalid_copy_option(
                    name,
                    "is only valid for text or csv",
                ));
            }
        }
        Ok(())
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

    pub(super) fn postgres_options(
        context: &CopyContext<'_>,
        stream: Option<DelimitedFormat>,
    ) -> *mut pg_sys::List {
        let options = context
            .statement()
            .option_view()
            .without_names(&CONNECTOR_OPTION_NAMES);
        let Some(stream) = stream else {
            return options;
        };
        let value = match stream {
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
