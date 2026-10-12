//! PostgreSQL COPY text-format object and its validated options.

use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::storage::InputFile;

use super::delimited::{DelimitedOptions, DelimitedOptionsBuilder};
use super::delimited_schema::DelimitedSchemaReader;
use super::{
    FormatKind, FormatObject, FormatOption, FormatSchemaReader, InferredSchema,
    StreamCompressionOptions,
};

/// Text-format processor.
pub(crate) struct TextFormat {
    pub(super) options: TextOptions,
    pub(super) compression: StreamCompressionOptions,
}

#[derive(Debug)]
pub(crate) struct TextOptions(pub(super) DelimitedOptions);

impl TextOptions {
    pub(crate) fn postgres_options(
        &self,
    ) -> Result<*mut pg_sys::List, ConnectorError> {
        self.0
            .append_postgres_output_options(std::ptr::null_mut(), FormatKind::Text)
    }
}

impl TextFormat {
    pub(crate) fn into_parts(self) -> (TextOptions, StreamCompressionOptions) {
        (self.options, self.compression)
    }

    pub(crate) fn resolve(
        compression: StreamCompressionOptions,
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
            options: TextOptions(DelimitedOptions {
                delimiter,
                null_marker,
                encoding,
            }),
            compression,
        })
    }
}

impl FormatObject for TextFormat {
    fn kind(&self) -> FormatKind {
        FormatKind::Text
    }
}

impl FormatSchemaReader for TextFormat {
    fn infer_schema(
        &self,
        file: &mut InputFile,
    ) -> Result<InferredSchema, ConnectorError> {
        let compression = self.compression.for_file(file);
        // SAFETY: postgres_options returns a PostgreSQL-owned COPY
        // option list in the current context, which outlives inference.
        unsafe {
            DelimitedSchemaReader::new(
                FormatKind::Text,
                false,
                self.options.postgres_options()?,
            )
        }
        .infer(file, compression)
    }
}
