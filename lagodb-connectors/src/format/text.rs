//! PostgreSQL COPY text-format object and its validated options.

use pg_lakebase_core::fdw::{
    BeginForeignScanContext, ColumnRequirements, ForeignInsertBeginContext,
    ForeignModifyBeginContext, ForeignModifyCapabilities, ForeignModifyOperation,
    ForeignModifyPlanContext, ForeignModifyPlanSpec, ForeignModifyRelationContext,
};
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_storage::StorageFile;
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::fdw::Lakebase;

use super::delimited::{DelimitedOptions, DelimitedOptionsBuilder};
use super::delimited_schema::DelimitedSchemaReader;
use super::{
    FormatKind, FormatObject, FormatOption, FormatReader, FormatScanPlanner,
    FormatScanState, FormatSchemaReader, FormatWritePrivate, FormatWriteState,
    FormatWriter, InferredSchema, StreamCompression,
};
use crate::storage::ObjectOutput;

/// Text-format processor.
pub(crate) struct TextFormat {
    pub(super) options: TextOptions,
    pub(super) compression: StreamCompression,
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
        file: &mut StorageFile,
    ) -> Result<InferredSchema, ConnectorError> {
        // SAFETY: postgres_output_options returns a PostgreSQL-owned COPY
        // option list in the current context, which outlives inference.
        unsafe {
            DelimitedSchemaReader::new(
                FormatKind::Text,
                false,
                self.options.postgres_output_options()?,
            )
        }
        .infer(file, self.compression)
    }
}

impl FormatReader for TextFormat {
    fn planner(self: Box<Self>) -> Box<dyn FormatScanPlanner> {
        Box::new(super::delimited_scan::DelimitedScanPlanner::new(
            FormatKind::Text,
        ))
    }

    fn begin(
        self: Box<Self>,
        context: BeginForeignScanContext<'_, Lakebase>,
        files: crate::storage::ObjectFiles,
    ) -> Result<Box<dyn FormatScanState>, ConnectorError> {
        let Self {
            options,
            compression,
        } = *self;
        let postgres_options =
            options.postgres_options(&context.relation, context.required_columns)?;
        Ok(Box::new(super::delimited_scan::DelimitedScanState::begin(
            context,
            files,
            compression,
            postgres_options,
        )?))
    }
}

impl FormatWriter for TextFormat {
    fn capabilities(
        &self,
        _context: &ForeignModifyRelationContext<'_>,
    ) -> Result<ForeignModifyCapabilities, ConnectorError> {
        Ok(ForeignModifyCapabilities::new(true, false, false))
    }

    fn plan_modify(
        &self,
        context: &ForeignModifyPlanContext<'_>,
    ) -> Result<ForeignModifyPlanSpec<FormatWritePrivate>, ConnectorError> {
        if context.operation() != ForeignModifyOperation::Insert {
            return Err(ConnectorError::modify_not_implemented(FormatKind::Text));
        }
        Ok(ForeignModifyPlanSpec::new(FormatWritePrivate::new(
            FormatKind::Text,
        )))
    }

    fn begin_modify(
        self: Box<Self>,
        context: ForeignModifyBeginContext<'_, FormatWritePrivate>,
        output: ObjectOutput,
    ) -> Result<Box<dyn FormatWriteState>, ConnectorError> {
        if context.operation() != ForeignModifyOperation::Insert {
            return Err(ConnectorError::modify_not_implemented(FormatKind::Text));
        }
        let Self {
            options,
            compression,
        } = *self;
        let postgres_options = options.postgres_output_options()?;
        Ok(Box::new(
            super::delimited_write::DelimitedWriteState::begin(
                context.relation(),
                output,
                FormatKind::Text,
                compression,
                postgres_options,
                false,
            )?,
        ))
    }

    fn begin_insert(
        self: Box<Self>,
        context: &mut ForeignInsertBeginContext<'_>,
        output: ObjectOutput,
    ) -> Result<Box<dyn FormatWriteState>, ConnectorError> {
        let Self {
            options,
            compression,
        } = *self;
        let postgres_options = options.postgres_output_options()?;
        Ok(Box::new(
            super::delimited_write::DelimitedWriteState::begin(
                context.relation(),
                output,
                FormatKind::Text,
                compression,
                postgres_options,
                false,
            )?,
        ))
    }
}
