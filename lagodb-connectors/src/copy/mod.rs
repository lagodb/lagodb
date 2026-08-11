//! Object-URI COPY consumer for LagoDB connectors.
//!
//! PostgreSQL text and CSV objects use the core COPY drivers directly. Native
//! formats supply an internal canonical CSV bridge so PostgreSQL retains COPY
//! relation/query semantics while the connector owns object encoding; routing
//! an object URI must never silently execute PostgreSQL's local-file COPY path
//! with the wrong format semantics.
//!
//! A native format decides whether a selected-format suffix denotes an exact
//! object or a prefix. PostgreSQL text/CSV retain their existing exact-object
//! behavior.

mod options;

use pg_lakebase_core::copy::{
    CopyCompletion, CopyContext, CopyError, CopyFromDriver, CopyFromSpec,
    CopyToDriver, CopyToSpec,
};
use pg_lakebase_core::hooks::{CopyConsumer, CopyRoute, register_copy_consumer};

use crate::error::ConnectorError;
use crate::format::{FormatCopyDestination, FormatCopySource};
use crate::storage::{ObjectUri, StorageTarget};

use self::options::ResolvedCopyOptions;

pub(crate) struct ConnectorCopyConsumer;

impl CopyConsumer for ConnectorCopyConsumer {
    fn name(&self) -> &'static str {
        "lagodb-connectors.object-copy"
    }

    fn route(&self, context: &CopyContext<'_>) -> Result<CopyRoute, CopyError> {
        let statement = context.statement();
        let Some(filename) = statement.filename() else {
            return Ok(CopyRoute::PassThrough);
        };
        if statement.is_program() {
            return Ok(CopyRoute::PassThrough);
        }
        Ok(if ObjectUri::is_supported_prefix(filename.to_bytes()) {
            CopyRoute::Consumed
        } else {
            CopyRoute::PassThrough
        })
    }

    fn consume(
        &self,
        context: &mut CopyContext<'_>,
    ) -> Result<CopyCompletion, CopyError> {
        self.consume_inner(context)
    }
}

impl ConnectorCopyConsumer {
    fn consume_inner(
        &self,
        context: &mut CopyContext<'_>,
    ) -> Result<CopyCompletion, CopyError> {
        let statement = context.statement();
        let filename = statement.filename().ok_or_else(|| {
            ConnectorError::invalid_object_uri("object URI is required")
        })?;
        let filename = filename
            .to_str()
            .map_err(|_| ConnectorError::invalid_object_uri("must be valid UTF-8"))?;
        let object = ObjectUri::parse(filename)?;
        let options =
            options::CopyCommandOptions::from_statement(statement, &object)?;
        let options = options.into_resolved();
        if statement.is_from() {
            self.copy_from(context, object, options)
        } else {
            self.copy_to(context, object, options)
        }
    }

    fn copy_from(
        &self,
        context: &mut CopyContext<'_>,
        object: ObjectUri,
        options: ResolvedCopyOptions,
    ) -> Result<CopyCompletion, CopyError> {
        let parse_state = context.parse_state();
        let preparation = context.prepare_from(&parse_state)?;
        let target = StorageTarget::resolve(object, options.storage_server.as_deref())?;
        let mut source = options.format.open_source(&target, || {
            preparation.column_layout(context.statement())
        })?;
        let pg_options = source.postgres_options(context);
        let spec = unsafe {
            CopyFromSpec::new(
                context.statement(),
                &parse_state,
                preparation,
                pg_options,
                source.source(),
            )
        };
        let processed = unsafe { CopyFromDriver::begin(spec)? }.execute()?;
        parse_state.dispose()?;
        Ok(CopyCompletion::new(processed))
    }

    fn copy_to(
        &self,
        context: &mut CopyContext<'_>,
        object: ObjectUri,
        options: ResolvedCopyOptions,
    ) -> Result<CopyCompletion, CopyError> {
        let parse_state = context.parse_state();
        let preparation = context.prepare_to(&parse_state)?;
        let target = StorageTarget::resolve(object, options.storage_server.as_deref())?;

        let mut destination = options.format.open_destination(&target)?;
        let pg_options = destination.postgres_options(context);
        let spec = unsafe {
            CopyToSpec::new(
                context.statement(),
                &parse_state,
                preparation,
                pg_options,
                destination.destination(),
            )
        };
        let processed = unsafe { CopyToDriver::begin(spec)? }.execute()?;
        parse_state.dispose()?;
        destination.finish()?;
        Ok(CopyCompletion::new(processed))
    }
}

pub(crate) fn register() {
    register_copy_consumer(Box::new(ConnectorCopyConsumer));
}
