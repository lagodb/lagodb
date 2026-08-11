//! PostgreSQL COPY text-format object and its validated options.

use crate::error::ConnectorError;

use super::delimited::{DelimitedOptions, DelimitedOptionsBuilder};
use super::{
    FormatKind, FormatObject, FormatOption, FormatReader, FormatWriter,
    StreamCompression,
};

/// Text-format processor.
pub(crate) struct TextFormat {
    // Validated once here; the text scan/write implementation will consume
    // this configuration when that existing capability skeleton is filled in.
    _options: TextOptions,
    pub(super) _compression: StreamCompression,
}

#[derive(Debug)]
struct TextOptions {
    _delimiter: u8,
    _null_marker: Box<str>,
    _encoding: Option<Box<str>>,
}

impl TextFormat {
    pub(crate) fn resolve(
        compression: StreamCompression,
        options: &[FormatOption<'_>],
    ) -> Result<Self, ConnectorError> {
        let mut builder = DelimitedOptionsBuilder::default();
        for option in options.iter().copied() {
            if !builder.consume(option)? {
                return Err(ConnectorError::invalid_option(
                    option.name(),
                    "is not valid for text",
                ));
            }
        }
        let DelimitedOptions {
            delimiter,
            null_marker,
            encoding,
        } = builder.resolve("\t", "\\N")?;
        if b"\\.abcdefghijklmnopqrstuvwxyz0123456789".contains(&delimiter) {
            return Err(ConnectorError::invalid_option(
                "delimiter",
                "is not valid for PostgreSQL COPY TEXT",
            ));
        }
        Ok(Self {
            _options: TextOptions {
                _delimiter: delimiter,
                _null_marker: null_marker,
                _encoding: encoding,
            },
            _compression: compression,
        })
    }
}

impl FormatObject for TextFormat {
    fn kind(&self) -> FormatKind {
        FormatKind::Text
    }
}

impl FormatReader for TextFormat {}

impl FormatWriter for TextFormat {}
