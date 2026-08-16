//! Cold-path schema inference for PostgreSQL COPY text and CSV input.

use std::ffi::CStr;

use pg_lakebase_core::copy::{
    CopyRawFieldReader, CopyRawRecord, CopyTextInputValidator,
};
use pg_lakebase_storage::StorageFile;
use pgrx::pg_sys;

use crate::error::ConnectorError;

use super::{
    FormatKind, InferredColumn, InferredSchema, PostgresType, SCHEMA_SAMPLE_RECORDS,
    StorageFileCopySource, StreamCompression,
};

pub(super) struct DelimitedSchemaReader {
    format: FormatKind,
    has_header: bool,
    options: *mut pg_sys::List,
}

impl DelimitedSchemaReader {
    /// # Safety
    ///
    /// `options` must be PostgreSQL `NIL` or a valid COPY option list that
    /// remains live through [`Self::infer`].
    pub(super) const unsafe fn new(
        format: FormatKind,
        has_header: bool,
        options: *mut pg_sys::List,
    ) -> Self {
        Self {
            format,
            has_header,
            options,
        }
    }

    pub(super) fn infer(
        self,
        file: &mut StorageFile,
        compression: StreamCompression,
    ) -> Result<InferredSchema, ConnectorError> {
        let mut source = StorageFileCopySource::new(file, compression)?;
        // SAFETY: required by `new`; `self` retains the same options pointer
        // and the reader is finished before this method returns.
        let mut reader =
            unsafe { CopyRawFieldReader::begin(self.options, &mut source) }
                .map_err(ConnectorError::from)?;
        let result = self.read_records(&mut reader);
        let finish = reader.finish().map_err(ConnectorError::from);
        match (result, finish) {
            (Ok(schema), Ok(())) => Ok(schema),
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        }
    }

    fn read_records(
        self,
        reader: &mut CopyRawFieldReader<'_>,
    ) -> Result<InferredSchema, ConnectorError> {
        let mut validators = TypeValidators::new()?;
        let first = reader.next_record().map_err(ConnectorError::from)?;
        let Some(first) = first else {
            return InferredSchema::new(self.format, Vec::new());
        };

        let mut accumulator = if self.has_header {
            DelimitedSchemaAccumulator::from_header(self.format, &first)?
        } else {
            DelimitedSchemaAccumulator::from_data(
                self.format,
                &first,
                &mut validators,
            )?
        };

        let mut sampled = usize::from(!self.has_header);
        while sampled < SCHEMA_SAMPLE_RECORDS {
            let Some(record) = reader.next_record().map_err(ConnectorError::from)?
            else {
                break;
            };
            accumulator.observe(&record, &mut validators)?;
            sampled += 1;
        }
        accumulator.into_schema()
    }
}

struct DelimitedSchemaAccumulator {
    format: FormatKind,
    columns: Vec<DelimitedColumn>,
}

impl DelimitedSchemaAccumulator {
    fn from_header(
        format: FormatKind,
        record: &CopyRawRecord<'_>,
    ) -> Result<Self, ConnectorError> {
        let columns = record
            .fields()
            .map(|name| {
                DelimitedColumn::new(
                    name.map_or_else(Box::<[u8]>::default, |value| {
                        value.to_bytes().into()
                    }),
                )
            })
            .collect::<Vec<_>>();
        if columns.is_empty() {
            return Err(ConnectorError::invalid_object_schema(
                format,
                "the CSV header has no fields",
            ));
        }
        Ok(Self { format, columns })
    }

    fn from_data(
        format: FormatKind,
        record: &CopyRawRecord<'_>,
        validators: &mut TypeValidators,
    ) -> Result<Self, ConnectorError> {
        let mut columns = Vec::with_capacity(record.len());
        for (index, value) in record.fields().enumerate() {
            let mut column = DelimitedColumn::new(
                format!("column{}", index + 1)
                    .into_bytes()
                    .into_boxed_slice(),
            );
            column.observe(value, validators)?;
            columns.push(column);
        }
        Ok(Self { format, columns })
    }

    fn observe(
        &mut self,
        record: &CopyRawRecord<'_>,
        validators: &mut TypeValidators,
    ) -> Result<(), ConnectorError> {
        if record.len() != self.columns.len() {
            return Err(ConnectorError::invalid_object_schema(
                self.format,
                "sampled records have inconsistent field counts",
            ));
        }
        for (column, value) in self.columns.iter_mut().zip(record.fields()) {
            column.observe(value, validators)?;
        }
        Ok(())
    }

    fn into_schema(self) -> Result<InferredSchema, ConnectorError> {
        let columns = self
            .columns
            .into_iter()
            .map(|column| {
                let postgres_type = column.type_state.postgres_type(self.format);
                InferredColumn::from_bytes(column.name, postgres_type)
            })
            .collect();
        InferredSchema::new(self.format, columns)
    }
}

struct DelimitedColumn {
    name: Box<[u8]>,
    type_state: DelimitedTypeState,
}

impl DelimitedColumn {
    fn new(name: Box<[u8]>) -> Self {
        Self {
            name,
            type_state: DelimitedTypeState::default(),
        }
    }

    fn observe(
        &mut self,
        value: Option<&CStr>,
        validators: &mut TypeValidators,
    ) -> Result<(), ConnectorError> {
        let Some(value) = value else {
            return Ok(());
        };
        self.type_state.observe(value, validators)
    }
}

#[derive(Clone, Copy)]
struct DelimitedTypeState {
    candidates: TypeCandidates,
    saw_value: bool,
}

impl Default for DelimitedTypeState {
    fn default() -> Self {
        Self {
            candidates: TypeCandidates::ALL,
            saw_value: false,
        }
    }
}

impl DelimitedTypeState {
    fn observe(
        &mut self,
        value: &CStr,
        validators: &mut TypeValidators,
    ) -> Result<(), ConnectorError> {
        self.saw_value = true;
        self.candidates.retain(value, validators)
    }

    fn postgres_type(self, format: FormatKind) -> PostgresType {
        let oid = if !self.saw_value {
            pg_sys::TEXTOID
        } else if self.candidates.contains(TypeCandidates::INT8) {
            pg_sys::INT8OID
        } else if self.candidates.contains(TypeCandidates::NUMERIC) {
            pg_sys::NUMERICOID
        } else if self.candidates.contains(TypeCandidates::FLOAT8) {
            pg_sys::FLOAT8OID
        } else if self.candidates.contains(TypeCandidates::BOOLEAN) {
            pg_sys::BOOLOID
        } else {
            pg_sys::TEXTOID
        };
        PostgresType::new(format, oid)
    }
}

#[derive(Clone, Copy)]
struct TypeCandidates(u8);

impl TypeCandidates {
    const BOOLEAN: u8 = 1;
    const INT8: u8 = 1 << 1;
    const NUMERIC: u8 = 1 << 2;
    const FLOAT8: u8 = 1 << 3;
    const ALL: Self = Self(Self::BOOLEAN | Self::INT8 | Self::NUMERIC | Self::FLOAT8);

    const fn contains(self, candidate: u8) -> bool {
        self.0 & candidate != 0
    }

    fn retain(
        &mut self,
        value: &CStr,
        validators: &mut TypeValidators,
    ) -> Result<(), ConnectorError> {
        for (candidate, validator) in validators.iter_mut() {
            if self.contains(candidate)
                && !validator.accepts(value).map_err(ConnectorError::from)?
            {
                self.0 &= !candidate;
            }
        }
        Ok(())
    }
}

struct TypeValidators {
    boolean: CopyTextInputValidator,
    int8: CopyTextInputValidator,
    numeric: CopyTextInputValidator,
    float8: CopyTextInputValidator,
}

impl TypeValidators {
    fn new() -> Result<Self, ConnectorError> {
        Ok(Self {
            boolean: CopyTextInputValidator::new(pg_sys::BOOLOID)
                .map_err(ConnectorError::from)?,
            int8: CopyTextInputValidator::new(pg_sys::INT8OID)
                .map_err(ConnectorError::from)?,
            numeric: CopyTextInputValidator::new(pg_sys::NUMERICOID)
                .map_err(ConnectorError::from)?,
            float8: CopyTextInputValidator::new(pg_sys::FLOAT8OID)
                .map_err(ConnectorError::from)?,
        })
    }

    fn iter_mut(&mut self) -> [(u8, &mut CopyTextInputValidator); 4] {
        [
            (TypeCandidates::BOOLEAN, &mut self.boolean),
            (TypeCandidates::INT8, &mut self.int8),
            (TypeCandidates::NUMERIC, &mut self.numeric),
            (TypeCandidates::FLOAT8, &mut self.float8),
        ]
    }
}
