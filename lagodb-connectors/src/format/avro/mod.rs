//! Avro object-container format support.

mod read;
mod value_kind;
mod write;

use std::io::BufReader;

use apache_avro::{Reader, Schema};

use crate::error::ConnectorError;
use crate::storage::InputFile;

use super::{
    AvroWriteCompression, FormatKind, FormatObject, FormatOption, FormatSchemaReader,
    InferredColumn, InferredSchema,
};

pub(crate) use read::AvroReadColumn;
use value_kind::AvroValueKind;
pub(crate) use write::{AvroDatumRow, AvroObjectWriter, AvroWritePlan};

/// Avro-format processor.
pub(crate) struct AvroFormat {
    write_compression: AvroWriteCompression,
}

impl AvroFormat {
    pub(crate) const fn write_compression(&self) -> AvroWriteCompression {
        self.write_compression
    }

    pub(crate) fn resolve(
        write_compression: AvroWriteCompression,
        options: &[FormatOption<'_>],
    ) -> Result<Self, ConnectorError> {
        if let Some(option) = options.first() {
            return Err(ConnectorError::unknown_option(
                option.name(),
                "is not valid for avro",
            ));
        }
        Ok(Self { write_compression })
    }
}

impl FormatObject for AvroFormat {
    fn kind(&self) -> FormatKind {
        FormatKind::Avro
    }
}

impl FormatSchemaReader for AvroFormat {
    fn infer_schema(
        &self,
        file: &mut InputFile,
    ) -> Result<InferredSchema, ConnectorError> {
        // OCF headers contain byte-wise varints even though record payloads
        // are buffered internally by apache-avro.
        let reader = Reader::new(BufReader::new(file))?;
        let Schema::Record(record) = reader.writer_schema() else {
            return Err(ConnectorError::invalid_object_schema(
                self.kind(),
                "the Avro container writer schema must be a record",
            ));
        };
        let columns = record
            .fields
            .iter()
            .map(|field| {
                let postgres_type = AvroValueKind::from_schema(&field.schema)
                    .map_err(|error| match error {
                        ConnectorError::InvalidObjectSchema { format, reason } => {
                            ConnectorError::invalid_object_schema(
                                format,
                                format!("column {:?}: {reason}", field.name),
                            )
                        }
                        error => error,
                    })?
                    .postgres_type();
                Ok(InferredColumn::new(&field.name, postgres_type))
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        InferredSchema::new(self.kind(), columns)
    }
}
