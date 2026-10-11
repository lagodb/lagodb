//! Format-neutral schema description for DDL and COPY cold paths.

use std::collections::HashSet;
use std::ffi::CString;

use crate::storage::InputFile;
use pgrx::pg_sys;

use crate::error::ConnectorError;

use super::{FormatKind, FormatObject};

pub(crate) const SCHEMA_SAMPLE_RECORDS: usize = 100;

pub(crate) trait FormatSchemaReader: FormatObject {
    fn infer_schema(
        &self,
        file: &mut InputFile,
    ) -> Result<InferredSchema, ConnectorError>;
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

    pub(crate) fn numeric(format: FormatKind, precision: i32, scale: i32) -> Self {
        let typmod = ((precision << 16) | (scale & 0x7ff))
            + i32::try_from(std::mem::size_of::<i32>())
                .expect("i32 size fits in i32");
        Self::with_typmod(format, pg_sys::NUMERICOID, typmod)
    }
}
