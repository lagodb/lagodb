//! COPY options owned by the connector consumer.
//!
//! PostgreSQL COPY options are deliberately not decoded here. They remain in
//! the original `CopyStmt` so the consumer can strip connector-owned options
//! and pass the remaining options to the PostgreSQL COPY bridge. This module parses
//! the connector-owned option names; the selected format validates its COPY
//! compression and PostgreSQL-option semantics.

use lagodb_core::copy::{CopyEndpoint, CopyOptionView, CopyStatement};

use crate::error::ConnectorError;
use crate::format::{FormatKind, ResolvedCopyFormat};
use crate::storage::StoragePath;

pub(crate) struct CopyCommandOptions {
    pub(crate) server: Option<Box<str>>,
    pub(crate) format: ResolvedCopyFormat,
}

impl CopyCommandOptions {
    /// Claim native local formats while explicit PostgreSQL formats retain
    /// the table consumer or PostgreSQL's standard COPY path.
    pub(super) fn uses_native_file_format(statement: &CopyStatement<'_>) -> bool {
        if statement.endpoint() != CopyEndpoint::ServerFile {
            return false;
        }
        let kind = match statement.option_view().get("format") {
            Some(format) => format.value_str().ok().and_then(FormatKind::parse),
            None => {
                let filename = statement
                    .filename()
                    .expect("server-file COPY has a filename");
                // Routing only inspects the suffix. Consumption reports an
                // invalid UTF-8 path instead of falling through to text COPY.
                FormatKind::infer_from_key(&filename.to_string_lossy())
            }
        };
        matches!(
            kind,
            Some(FormatKind::Json | FormatKind::Avro | FormatKind::Parquet)
        )
    }

    pub(crate) fn from_statement(
        statement: &CopyStatement<'_>,
        object: &StoragePath,
    ) -> Result<Self, ConnectorError> {
        let provider = ProviderOptions::parse(statement.option_view())?;
        let format = provider.format.map_or_else(
            || Self::infer_format(object.key()),
            Ok::<FormatKind, ConnectorError>,
        )?;
        let format = ResolvedCopyFormat::resolve(
            statement.option_view(),
            format,
            object,
            statement.is_from(),
            provider.compression.as_deref(),
        )?;
        Ok(Self {
            server: provider.server,
            format,
        })
    }

    fn infer_format(key: &str) -> Result<FormatKind, ConnectorError> {
        FormatKind::infer_from_key(key).ok_or_else(|| {
            ConnectorError::invalid_copy_option(
                "format",
                "cannot be inferred from the object suffix; specify format explicitly",
            )
        })
    }
}

#[derive(Default)]
struct ProviderOptions {
    server: Option<Box<str>>,
    format: Option<FormatKind>,
    compression: Option<Box<str>>,
}

impl ProviderOptions {
    fn parse(view: CopyOptionView<'_>) -> Result<Self, ConnectorError> {
        let mut options = Self::default();
        for option in view.iter() {
            let name = option.name().to_bytes();
            match name {
                b"server" => {
                    if options.server.is_some() {
                        return Err(ConnectorError::invalid_copy_option(
                            "server",
                            "must not be specified more than once",
                        ));
                    }
                    let value = option.value_str().map_err(|_| {
                        ConnectorError::invalid_copy_option(
                            "server",
                            "must be valid UTF-8",
                        )
                    })?;
                    if value.is_empty() {
                        return Err(ConnectorError::invalid_copy_option(
                            "server",
                            "must not be empty",
                        ));
                    }
                    options.server = Some(value.into());
                }
                b"format" => {
                    if options.format.is_some() {
                        return Err(ConnectorError::invalid_copy_option(
                            "format",
                            "must not be specified more than once",
                        ));
                    }
                    let value = option.value_str().map_err(|_| {
                        ConnectorError::invalid_copy_option(
                            "format",
                            "must be valid UTF-8",
                        )
                    })?;
                    options.format = Some(
                        FormatKind::parse(value)
                            .ok_or_else(|| ConnectorError::invalid_format(value))?,
                    );
                }
                b"compression" => {
                    if options.compression.is_some() {
                        return Err(ConnectorError::invalid_copy_option(
                            "compression",
                            "must not be specified more than once",
                        ));
                    }
                    let value = option.value_str().map_err(|_| {
                        ConnectorError::invalid_copy_option(
                            "compression",
                            "must be valid UTF-8",
                        )
                    })?;
                    options.compression = Some(value.into());
                }
                // All other options belong to PostgreSQL COPY. Keeping them
                // in the raw statement preserves PostgreSQL's option
                // validation and row semantics in the core bridge.
                _ => {}
            }
        }
        Ok(options)
    }
}
