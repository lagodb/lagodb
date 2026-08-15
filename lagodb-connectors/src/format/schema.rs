//! Format-neutral schema description for DDL and COPY cold paths.

use std::collections::HashSet;
use std::ffi::CString;
use std::io::{self, Read};

use arrow_schema::{DataType, Schema, TimeUnit};
use pg_lakebase_core::copy::{CopyDataSource, CopyError};
use pg_lakebase_storage::StorageFile;
use pgrx::pg_sys;

use crate::error::ConnectorError;

use super::{FormatKind, FormatObject, StreamCompression, StreamDecoder};

pub(crate) const SCHEMA_SAMPLE_RECORDS: usize = 100;

pub(crate) trait FormatSchemaReader: FormatObject {
    fn infer_schema(
        &self,
        file: &mut StorageFile,
    ) -> Result<InferredSchema, ConnectorError>;
}

pub(crate) struct StorageFileReader<'a> {
    file: &'a mut StorageFile,
}

impl<'a> StorageFileReader<'a> {
    pub(crate) fn new(file: &'a mut StorageFile) -> Self {
        Self { file }
    }
}

impl Read for StorageFileReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.file.read_into(output).map_err(io::Error::other)
    }
}

/// A compressed object source for one cold-path PostgreSQL COPY parser.
pub(crate) struct StorageFileCopySource<'a> {
    decoder: StreamDecoder<StorageFileReader<'a>>,
}

impl<'a> StorageFileCopySource<'a> {
    pub(crate) fn new(
        file: &'a mut StorageFile,
        compression: StreamCompression,
    ) -> Result<Self, ConnectorError> {
        let decoder = StreamDecoder::new(StorageFileReader::new(file), compression)
            .map_err(ConnectorError::copy_stream_io)?;
        Ok(Self { decoder })
    }
}

impl CopyDataSource for StorageFileCopySource<'_> {
    fn read(
        &mut self,
        output: &mut [u8],
        min_read: usize,
    ) -> Result<usize, CopyError> {
        self.decoder
            .read_at_least(output, min_read)
            .map_err(ConnectorError::copy_stream_io)
            .map_err(CopyError::from)
    }
}

#[derive(Debug)]
pub(crate) struct InferredSchema {
    columns: Vec<InferredColumn>,
}

impl InferredSchema {
    pub(crate) fn new(
        format: FormatKind,
        columns: Vec<InferredColumn>,
    ) -> Result<Self, ConnectorError> {
        if columns.is_empty() {
            return Err(ConnectorError::invalid_object_schema(
                format,
                "the object does not describe any columns",
            ));
        }
        Self::validate_column_names(&columns)?;
        Ok(Self { columns })
    }

    pub(crate) fn from_arrow(
        format: FormatKind,
        schema: &Schema,
    ) -> Result<Self, ConnectorError> {
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                let postgres_type = PostgresType::from_arrow(
                    format,
                    field.name(),
                    field.data_type(),
                )?;
                Ok(InferredColumn::new(field.name(), postgres_type))
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        Self::new(format, columns)
    }

    pub(crate) fn into_pg_list(self) -> Result<*mut pg_sys::List, ConnectorError> {
        let mut columns = std::ptr::null_mut();
        for column in self.columns {
            // SAFETY: InferredSchema validates names before this PostgreSQL
            // DDL conversion and ownership moves here unchanged.
            let name = unsafe { CString::from_vec_unchecked(column.name.into_vec()) };
            let definition = unsafe {
                pg_sys::makeColumnDef(
                    name.as_ptr(),
                    column.postgres_type.oid,
                    column.postgres_type.typmod,
                    pg_sys::InvalidOid,
                )
            };
            columns = unsafe { pg_sys::lappend(columns, definition.cast()) };
        }
        Ok(columns)
    }

    fn validate_column_names(
        columns: &[InferredColumn],
    ) -> Result<(), ConnectorError> {
        let mut names = HashSet::with_capacity(columns.len());
        for column in columns {
            let name = column.name.as_ref();
            if name.is_empty() {
                return Err(ConnectorError::invalid_object_schema(
                    column.format,
                    "a column name is empty",
                ));
            }
            if name.len() >= pg_sys::NAMEDATALEN as usize {
                return Err(ConnectorError::invalid_object_schema(
                    column.format,
                    format!(
                        "column name {:?} exceeds PostgreSQL's {}-byte identifier limit",
                        String::from_utf8_lossy(name),
                        pg_sys::NAMEDATALEN - 1,
                    ),
                ));
            }
            if name.contains(&0) {
                return Err(ConnectorError::invalid_object_schema(
                    column.format,
                    "a column name contains a NUL byte",
                ));
            }
            if !names.insert(name) {
                return Err(ConnectorError::invalid_object_schema(
                    column.format,
                    "column names must be unique",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct InferredColumn {
    name: Box<[u8]>,
    postgres_type: PostgresType,
    format: FormatKind,
}

impl InferredColumn {
    pub(crate) fn new(name: &str, postgres_type: PostgresType) -> Self {
        Self {
            name: name.as_bytes().into(),
            postgres_type,
            format: postgres_type.format,
        }
    }

    pub(crate) fn from_bytes(name: Box<[u8]>, postgres_type: PostgresType) -> Self {
        Self {
            name,
            postgres_type,
            format: postgres_type.format,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PostgresType {
    oid: pg_sys::Oid,
    typmod: i32,
    format: FormatKind,
}

impl PostgresType {
    pub(crate) const fn new(format: FormatKind, oid: pg_sys::Oid) -> Self {
        Self {
            oid,
            typmod: -1,
            format,
        }
    }

    pub(crate) const fn with_typmod(
        format: FormatKind,
        oid: pg_sys::Oid,
        typmod: i32,
    ) -> Self {
        Self {
            oid,
            typmod,
            format,
        }
    }

    pub(crate) fn array(self, field_name: &str) -> Result<Self, ConnectorError> {
        let oid = unsafe { pg_sys::get_array_type(self.oid) };
        if oid == pg_sys::InvalidOid {
            return Err(ConnectorError::invalid_object_schema(
                self.format,
                format!(
                    "column {field_name:?} has an element type without a PostgreSQL array type"
                ),
            ));
        }
        Ok(Self::new(self.format, oid))
    }

    pub(crate) fn numeric(format: FormatKind, precision: i32, scale: i32) -> Self {
        let typmod = ((precision << 16) | (scale & 0x7ff))
            + i32::try_from(std::mem::size_of::<i32>())
                .expect("i32 size fits in i32");
        Self::with_typmod(format, pg_sys::NUMERICOID, typmod)
    }

    fn from_arrow(
        format: FormatKind,
        field_name: &str,
        data_type: &DataType,
    ) -> Result<Self, ConnectorError> {
        let plain = |oid| Ok(Self::new(format, oid));
        match data_type {
            DataType::Boolean => plain(pg_sys::BOOLOID),
            DataType::Int32 => plain(pg_sys::INT4OID),
            DataType::Int64 => plain(pg_sys::INT8OID),
            DataType::Float32 => plain(pg_sys::FLOAT4OID),
            DataType::Float64 => plain(pg_sys::FLOAT8OID),
            DataType::Decimal128(precision, scale)
                if (1..=38).contains(precision)
                    && (0..=38).contains(scale)
                    && *scale as u8 <= *precision =>
            {
                Ok(Self::numeric(
                    format,
                    i32::from(*precision),
                    i32::from(*scale),
                ))
            }
            DataType::Timestamp(
                TimeUnit::Microsecond | TimeUnit::Nanosecond,
                timezone,
            ) => plain(if timezone.is_some() {
                pg_sys::TIMESTAMPTZOID
            } else {
                pg_sys::TIMESTAMPOID
            }),
            DataType::Date32 => plain(pg_sys::DATEOID),
            DataType::Time64(TimeUnit::Microsecond) => plain(pg_sys::TIMEOID),
            DataType::Binary | DataType::LargeBinary => plain(pg_sys::BYTEAOID),
            DataType::FixedSizeBinary(width) if *width > 0 => plain(pg_sys::BYTEAOID),
            DataType::Utf8 | DataType::LargeUtf8 => plain(pg_sys::TEXTOID),
            DataType::List(field) => match field.data_type() {
                DataType::Boolean => {
                    Self::new(format, pg_sys::BOOLOID).array(field_name)
                }
                DataType::Int32 => {
                    Self::new(format, pg_sys::INT4OID).array(field_name)
                }
                DataType::Int64 => {
                    Self::new(format, pg_sys::INT8OID).array(field_name)
                }
                DataType::Float32 => {
                    Self::new(format, pg_sys::FLOAT4OID).array(field_name)
                }
                DataType::Float64 => {
                    Self::new(format, pg_sys::FLOAT8OID).array(field_name)
                }
                DataType::Utf8 | DataType::LargeUtf8 => {
                    Self::new(format, pg_sys::TEXTOID).array(field_name)
                }
                _ => Err(ConnectorError::invalid_object_schema(
                    format,
                    format!(
                        "column {field_name:?} uses unsupported source type {data_type}"
                    ),
                )),
            },
            _ => Err(ConnectorError::invalid_object_schema(
                format,
                format!(
                    "column {field_name:?} uses unsupported source type {data_type}"
                ),
            )),
        }
    }
}
