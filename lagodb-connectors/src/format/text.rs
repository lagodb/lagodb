//! PostgreSQL COPY text-format object and its validated options.

use crate::storage::InputFile;
use lagodb_core::fdw::{ColumnRequirements, StartForeignScanContext};
use lagodb_core::handles::RelationHandle;
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::fdw::LagodbConnectors;

use super::delimited::{DelimitedFormat, DelimitedOptions, DelimitedOptionsBuilder};
use super::delimited_scan::{DelimitedScanPlanner, DelimitedScanState};
use super::delimited_schema::DelimitedSchemaReader;
use super::delimited_write::DelimitedWriteState;
use super::{
    FormatKind, FormatObject, FormatOption, FormatReader, FormatScanPlanner,
    FormatScanState, FormatSchemaReader, FormatWriteState, FormatWriter,
    InferredSchema, StreamCompressionOptions,
};
use crate::storage::{ObjectFiles, ObjectOutput};

/// Text-format processor.
pub(crate) struct TextFormat {
    pub(super) options: TextOptions,
    pub(super) compression: StreamCompressionOptions,
}

#[derive(Debug)]
pub(super) struct TextOptions(pub(super) DelimitedOptions);

impl TextOptions {
    pub(super) fn postgres_options(
        &self,
        relation: &RelationHandle<'_>,
        requirements: &ColumnRequirements,
    ) -> Result<*mut pg_sys::List, ConnectorError> {
        self.0.append_postgres_options(
            std::ptr::null_mut(),
            FormatKind::Text,
            relation,
            requirements,
        )
    }

    pub(super) fn postgres_output_options(
        &self,
    ) -> Result<*mut pg_sys::List, ConnectorError> {
        self.0
            .append_postgres_output_options(std::ptr::null_mut(), FormatKind::Text)
    }
}

impl TextFormat {
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
        // SAFETY: postgres_output_options returns a PostgreSQL-owned COPY
        // option list in the current context, which outlives inference.
        unsafe {
            DelimitedSchemaReader::new(
                FormatKind::Text,
                false,
                self.options.postgres_output_options()?,
            )
        }
        .infer(file, compression)
    }
}

impl FormatReader for TextFormat {
    fn planner(self: Box<Self>) -> Box<dyn FormatScanPlanner> {
        Box::new(DelimitedScanPlanner::new(FormatKind::Text))
    }

    fn begin(
        self: Box<Self>,
        context: StartForeignScanContext<'_, LagodbConnectors>,
        files: ObjectFiles,
    ) -> Result<Box<dyn FormatScanState>, ConnectorError> {
        let Self {
            options,
            compression,
        } = *self;
        let postgres_options =
            options.postgres_options(&context.relation, context.required_columns)?;
        Ok(Box::new(DelimitedScanState::begin(
            context,
            files,
            compression,
            postgres_options,
        )?))
    }
}

impl FormatWriter for TextFormat {
    fn begin(
        self: Box<Self>,
        relation: &RelationHandle<'_>,
        output: ObjectOutput,
    ) -> Result<Box<dyn FormatWriteState>, ConnectorError> {
        let Self {
            options,
            compression,
        } = *self;
        let postgres_options = options.postgres_output_options()?;
        Ok(Box::new(DelimitedWriteState::begin(
            relation,
            output,
            DelimitedFormat::Text,
            compression.for_output(),
            postgres_options,
            false,
        )?))
    }
}
