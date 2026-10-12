//! Parquet footer access and schema inference.

use arrow_schema::Schema;
use bytes::Bytes;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaDataReader;

use crate::error::ConnectorError;
use crate::format::{FormatKind, InferredColumn, InferredSchema, PostgresType};
use crate::storage::InputFile;

use super::type_mapping::ParquetTypeMapping;

const INITIAL_FOOTER_READ: u64 = 64 * 1024;

pub(super) struct ParquetSchema;

impl ParquetSchema {
    fn read(file: &mut InputFile) -> Result<Schema, ConnectorError> {
        let file_size = file.size();
        if file_size == 0 {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Parquet,
                "the object is empty",
            ));
        }
        let mut required = file_size.min(INITIAL_FOOTER_READ);
        let mut metadata_reader = ParquetMetaDataReader::new();
        loop {
            let read_len = u32::try_from(required).map_err(|_| {
                ConnectorError::invalid_object_schema(
                    FormatKind::Parquet,
                    "the Parquet footer exceeds the supported read size",
                )
            })?;
            let tail = Bytes::from(file.read_at(file_size - required, read_len)?);
            match metadata_reader.try_parse_sized(&tail, file_size) {
                Ok(()) => break,
                Err(ParquetError::NeedMoreData(needed)) => {
                    let needed = u64::try_from(needed).map_err(|_| {
                        ConnectorError::invalid_object_schema(
                            FormatKind::Parquet,
                            "the Parquet footer size cannot be represented",
                        )
                    })?;
                    if needed <= required || needed > file_size {
                        return Err(ConnectorError::invalid_object_schema(
                            FormatKind::Parquet,
                            "the Parquet footer references bytes outside the object",
                        ));
                    }
                    required = needed;
                }
                Err(error) => return Err(error.into()),
            }
        }
        let metadata = metadata_reader.finish()?;
        let file_metadata = metadata.file_metadata();
        Ok(parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
        )?)
    }

    pub(super) fn infer(
        file: &mut InputFile,
    ) -> Result<InferredSchema, ConnectorError> {
        let schema = Self::read(file)?;
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                let ty = ParquetTypeMapping::canonical(field)?;
                Ok(InferredColumn::new(
                    field.name(),
                    PostgresType::with_typmod(FormatKind::Parquet, ty.oid, ty.typmod),
                ))
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        InferredSchema::new(FormatKind::Parquet, columns)
    }
}
