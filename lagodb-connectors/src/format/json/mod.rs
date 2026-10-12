//! Newline-delimited JSON object format.

mod record;
mod scalar;
mod schema;
mod stream;

use std::io::BufReader;

use crate::error::ConnectorError;
use crate::gucs::ReadConfig;
use crate::storage::InputFile;

use super::{
    FormatKind, FormatObject, FormatOption, FormatSchemaReader, InferredSchema,
    StreamCompressionOptions, StreamDecoder,
};

pub(crate) use record::{JsonColumnPlan, JsonInputValue, JsonRecordDecoder};
pub(crate) use stream::JsonRecordStream;

/// JSON-format processor. Every non-empty line is one JSON object.
pub(crate) struct JsonFormat {
    compression: StreamCompressionOptions,
}

impl JsonFormat {
    pub(crate) const fn compression(&self) -> StreamCompressionOptions {
        self.compression
    }

    pub(crate) fn resolve(
        compression: StreamCompressionOptions,
        options: &[FormatOption<'_>],
    ) -> Result<Self, ConnectorError> {
        if let Some(option) = options.first() {
            return Err(ConnectorError::invalid_option(
                option.name(),
                "is not valid for json",
            ));
        }
        Ok(Self { compression })
    }
}

impl FormatObject for JsonFormat {
    fn kind(&self) -> FormatKind {
        FormatKind::Json
    }
}

impl FormatSchemaReader for JsonFormat {
    fn infer_schema(
        &self,
        file: &mut InputFile,
    ) -> Result<InferredSchema, ConnectorError> {
        let compression = self.compression.for_file(file);
        let input =
            StreamDecoder::new(file, compression).map_err(ConnectorError::json_io)?;
        schema::JsonSchemaAccumulator::default().read(
            BufReader::new(input),
            ReadConfig::from_guc().json_max_record_bytes(),
        )
    }
}
