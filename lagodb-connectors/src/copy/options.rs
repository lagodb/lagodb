//! COPY options owned by the object-storage consumer.
//!
//! PostgreSQL COPY options are deliberately not decoded here. They remain in
//! the original `CopyStmt` so the consumer can strip connector-owned options
//! and pass the remaining options to the PG17 COPY bridge. This module parses
//! the connector-owned option names; the selected format validates its COPY
//! compression and PostgreSQL-option semantics.

use pg_lakebase_core::copy::{CopyOptionView, CopyStatement};

use crate::error::ConnectorError;
use crate::format::{FormatKind, ResolvedCopyFormat};
use crate::storage::ObjectUri;

pub(crate) struct CopyCommandOptions {
    storage_server: Option<Box<str>>,
    format: ResolvedCopyFormat,
}

pub(crate) struct ResolvedCopyOptions {
    pub(crate) storage_server: Option<Box<str>>,
    pub(crate) format: ResolvedCopyFormat,
}

impl CopyCommandOptions {
    pub(crate) fn from_statement(
        statement: &CopyStatement<'_>,
        object: &ObjectUri,
    ) -> Result<Self, ConnectorError> {
        let provider = ProviderOptions::parse(statement.option_view())?;
        let format = provider.format.map_or_else(
            || infer_format(object.key()),
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
            storage_server: provider.storage_server,
            format,
        })
    }

    pub(crate) fn into_resolved(self) -> ResolvedCopyOptions {
        ResolvedCopyOptions {
            storage_server: self.storage_server,
            format: self.format,
        }
    }
}

#[derive(Default)]
struct ProviderOptions {
    storage_server: Option<Box<str>>,
    format: Option<FormatKind>,
    compression: Option<Box<str>>,
}

impl ProviderOptions {
    fn parse(view: CopyOptionView<'_>) -> Result<Self, ConnectorError> {
        let mut options = Self::default();
        for option in view.iter() {
            let name = option.name().to_bytes();
            match name {
                b"storage_server" => {
                    if options.storage_server.is_some() {
                        return Err(ConnectorError::invalid_copy_option(
                            "storage_server",
                            "must not be specified more than once",
                        ));
                    }
                    let value = option.value_str().map_err(|_| {
                        ConnectorError::invalid_copy_option(
                            "storage_server",
                            "must be valid UTF-8",
                        )
                    })?;
                    if value.is_empty() {
                        return Err(ConnectorError::invalid_copy_option(
                            "storage_server",
                            "must not be empty",
                        ));
                    }
                    options.storage_server = Some(value.into());
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

fn infer_format(key: &str) -> Result<FormatKind, ConnectorError> {
    FormatKind::infer_from_key(key).ok_or_else(|| {
        ConnectorError::invalid_copy_option(
            "format",
            "cannot be inferred from the object suffix; specify format explicitly",
        )
    })
}
