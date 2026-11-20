//! Object-URI and native local-file COPY consumer for LagoDB connectors.
//!
//! PostgreSQL text and CSV objects use the byte COPY drivers. Native formats
//! bind directly to Datum/slot drivers so PostgreSQL retains COPY executor
//! semantics without a canonical-CSV round trip. Routing an object URI must
//! never silently execute PostgreSQL's local-file COPY path with the wrong
//! format semantics.
//!
//! Every writable format uses the same exact-object versus rolling-prefix
//! output contract. Format adapters retain ownership of encoding and safe
//! split boundaries.

mod format_adapters;
mod options;

use lagodb_core::copy::{
    CopyCompletion, CopyContext, CopyEndpoint, CopyError, CopyFromDriver,
    CopyFromSpec, CopyToDriver, CopyToSpec, TypedCopyFromDriver, TypedCopyFromSpec,
    TypedCopyToDriver, TypedCopyToSpec,
};
use lagodb_core::hooks::{CopyConsumer, CopyRoute, register_copy_consumer};

use crate::access::ConnectorAccess;
use crate::error::ConnectorError;
use crate::storage::{ObjectUri, StoragePath};

use format_adapters::{
    CopyFormat, CopyFromFormat, CopyToFormat, FormatCopyOutput, StreamCopySource,
};
use options::CopyCommandOptions;

pub(crate) struct ConnectorCopyConsumer;

impl CopyConsumer for ConnectorCopyConsumer {
    fn name(&self) -> &'static str {
        "lagodb-connectors.object-copy"
    }

    fn route(&self, context: &CopyContext<'_>) -> Result<CopyRoute, CopyError> {
        let statement = context.statement();
        if CopyCommandOptions::uses_native_file_format(statement) {
            return Ok(CopyRoute::Consumed);
        }
        if statement.endpoint() != CopyEndpoint::ExternalUri {
            return Ok(CopyRoute::PassThrough);
        }
        let filename = statement
            .filename()
            .expect("external COPY URI has a filename");
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
        let statement = context.statement();
        let filename = statement.filename().ok_or_else(|| {
            ConnectorError::invalid_path("a file path or object URI is required")
        })?;
        let filename = filename
            .to_str()
            .map_err(|_| ConnectorError::invalid_object_uri("must be valid UTF-8"))?;
        let object = StoragePath::parse(filename)?;
        let options = CopyCommandOptions::from_statement(statement, &object)?;
        let server = options.server.as_deref();
        match options.format {
            CopyFormat::From(format) => {
                self.copy_from(context, object, server, format)
            }
            CopyFormat::To(format) => self.copy_to(context, object, server, format),
        }
    }
}

impl ConnectorCopyConsumer {
    fn copy_from(
        &self,
        context: &mut CopyContext<'_>,
        object: StoragePath,
        server: Option<&str>,
        format: CopyFromFormat,
    ) -> Result<CopyCompletion, CopyError> {
        let parse_state = context.parse_state();
        let preparation = context.prepare_from(&parse_state)?;
        let location = ConnectorAccess::resolve(object, server)?;
        let pg_options = format.postgres_options(context);
        // Core drivers validate PostgreSQL options before a source opens, so
        // location policy is checked inside the open callbacks.
        let processed = match format {
            CopyFromFormat::Delimited(_, compression) => {
                let spec = unsafe {
                    CopyFromSpec::new(
                        context.statement(),
                        &parse_state,
                        preparation,
                        pg_options,
                    )
                };
                unsafe { CopyFromDriver::begin(spec)? }.execute(|| {
                    format.validate_location(&location)?;
                    Ok(Box::new(StreamCopySource::open(&location, compression)?))
                })?
            }
            CopyFromFormat::Typed(typed) => {
                let spec = unsafe {
                    TypedCopyFromSpec::new(
                        context.statement(),
                        &parse_state,
                        preparation,
                        pg_options,
                    )
                };
                TypedCopyFromDriver::begin(spec)?.execute(|layout, on_error| {
                    typed.validate_on_error(on_error)?;
                    format.validate_location(&location)?;
                    typed.open_source(&location, layout)
                })?
            }
        };
        parse_state.dispose()?;
        Ok(CopyCompletion::new(processed))
    }

    fn copy_to(
        &self,
        context: &mut CopyContext<'_>,
        object: StoragePath,
        server: Option<&str>,
        format: CopyToFormat,
    ) -> Result<CopyCompletion, CopyError> {
        let parse_state = context.parse_state();
        let preparation = context.prepare_to(&parse_state)?;
        let location = ConnectorAccess::resolve(object, server)?;

        let mut destination = format.open_destination(&location)?;
        let pg_options = destination.postgres_options(context);
        let processed = match destination.output() {
            FormatCopyOutput::Bytes(destination) => {
                let spec = unsafe {
                    CopyToSpec::new(
                        context.statement(),
                        &parse_state,
                        preparation,
                        pg_options,
                        destination,
                    )
                };
                unsafe { CopyToDriver::begin(spec)? }.execute()?
            }
            FormatCopyOutput::Tuples(destination) => {
                let spec = unsafe {
                    TypedCopyToSpec::new(
                        context.statement(),
                        &parse_state,
                        preparation,
                        pg_options,
                        destination,
                    )
                };
                TypedCopyToDriver::begin(spec)?.execute()?
            }
        };
        parse_state.dispose()?;
        Ok(CopyCompletion::new(processed))
    }
}

pub(crate) fn register() {
    register_copy_consumer(Box::new(ConnectorCopyConsumer));
}
