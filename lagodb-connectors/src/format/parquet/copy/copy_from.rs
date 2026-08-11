//! Parquet-to-canonical-CSV source for PostgreSQL COPY FROM.

use std::ffi::CStr;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use arrow_schema::Schema;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use pg_arrow_conv::{ColumnReader, PgColumnType, resolve_column_rule};
use pg_lakebase_core::copy::{
    CopyColumnLayout, CopyDataSource, CopyError,
};
use pg_lakebase_core::diag::PgReportError;
use pg_lakebase_core::tuple::{ColumnDatumCodec, ColumnDatumTarget};
use pgrx::memcxt::PgMemoryContexts;
use pgrx::{PgTryBuilder, pg_sys};

use crate::error::ConnectorError;
use crate::format::{FormatKind, ParquetObjectReader};
use crate::storage::ObjectFiles;

use super::super::super::copy::{CanonicalCsv, FormatCopySource};

const PARQUET_BATCH_SIZE: usize = 8_192;
const BRIDGE_BUFFER_TARGET: usize = 256 * 1024;

struct CopyColumnPlan {
    source: usize,
    rule: pg_arrow_conv::ColumnRule,
    codec: ColumnDatumCodec,
    output_function: pg_sys::Oid,
}

struct BoundCopyBatch {
    columns: Box<[ColumnReader]>,
    rows: usize,
}

struct CopyColumnBindings {
    projection_roots: Box<[usize]>,
    columns: Box<[CopyColumnPlan]>,
}

pub(super) struct ParquetCopySource {
    files: ObjectFiles,
    expected_schema: Option<Arc<Schema>>,
    projection_roots: Box<[usize]>,
    columns: Box<[CopyColumnPlan]>,
    reader: Option<ParquetRecordBatchReader>,
    batch: Option<BoundCopyBatch>,
    row: usize,
    bytes: Vec<u8>,
    position: usize,
    datum_context: PgMemoryContexts,
}

impl ParquetCopySource {
    pub(super) fn new(
        mut files: ObjectFiles,
        layout: &CopyColumnLayout,
    ) -> Result<Self, CopyError> {
        let Some(first) = files.next() else {
            return Ok(Self {
                files,
                expected_schema: None,
                projection_roots: Box::new([]),
                columns: Box::new([]),
                reader: None,
                batch: None,
                row: 0,
                bytes: Vec::new(),
                position: 0,
                datum_context: PgMemoryContexts::new(
                    "lagodb parquet copy from bridge",
                ),
            });
        };
        let first = first?;
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(ParquetObjectReader::new(first))
                .map_err(ConnectorError::from)?;
        let expected_schema = builder.schema().clone();
        let bindings = Self::bind_columns(&expected_schema, layout)?;
        let reader = Self::build_reader(builder, &bindings.projection_roots)
            .map_err(CopyError::from)?;
        Ok(Self {
            files,
            expected_schema: Some(expected_schema),
            projection_roots: bindings.projection_roots,
            columns: bindings.columns,
            reader: Some(reader),
            batch: None,
            row: 0,
            bytes: Vec::with_capacity(BRIDGE_BUFFER_TARGET),
            position: 0,
            datum_context: PgMemoryContexts::new("lagodb parquet copy from bridge"),
        })
    }

    fn bind_columns(
        schema: &Arc<Schema>,
        layout: &CopyColumnLayout,
    ) -> Result<CopyColumnBindings, CopyError> {
        let mut roots = Vec::with_capacity(layout.len());
        let mut pending = Vec::with_capacity(layout.len());
        let bind = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                for column in layout.columns() {
                    let name = column.name().to_str().map_err(|_| {
                        ConnectorError::invalid_object_schema(
                            FormatKind::Parquet,
                            "COPY column names must be valid UTF-8 for Parquet",
                        )
                    })?;
                    let source = schema.index_of(name).map_err(|_| {
                        ConnectorError::invalid_object_schema(
                            FormatKind::Parquet,
                            format!(
                                "COPY target column {:?} is missing from the Parquet schema",
                                name
                            ),
                        )
                    })?;
                    let pg = PgColumnType::from_pg_type(column.type_oid()).ok_or_else(|| {
                        ConnectorError::invalid_object_schema(
                            FormatKind::Parquet,
                            format!(
                                "PostgreSQL type OID {} has no Arrow conversion",
                                column.type_oid()
                            ),
                        )
                    })?;
                    let rule = resolve_column_rule(schema.field(source).data_type(), pg)?;
                    let codec = ColumnDatumCodec::bind(ColumnDatumTarget::from_oid(
                        column.type_oid(),
                    ))?;
                    let mut output_function = pg_sys::InvalidOid;
                    let mut variable_length = false;
                    pg_sys::getTypeOutputInfo(
                        column.type_oid(),
                        &mut output_function,
                        &mut variable_length,
                    );
                    roots.push(source);
                    pending.push((source, rule, codec, output_function));
                }
                Ok::<(), ConnectorError>(())
            }))
            .catch_others(|error| Err(ConnectorError::Postgres(PgReportError::from_caught(error))))
            .execute()
        };
        bind.map_err(CopyError::from)?;

        roots.sort_unstable();
        roots.dedup();
        let columns = pending
            .into_iter()
            .map(|(source, rule, codec, output_function)| CopyColumnPlan {
                source: roots
                    .binary_search(&source)
                    .expect("projected Parquet source was retained"),
                rule,
                codec,
                output_function,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(CopyColumnBindings {
            projection_roots: roots.into_boxed_slice(),
            columns,
        })
    }

    fn build_reader(
        builder: ParquetRecordBatchReaderBuilder<ParquetObjectReader>,
        roots: &[usize],
    ) -> Result<ParquetRecordBatchReader, ConnectorError> {
        let projection =
            ProjectionMask::roots(builder.parquet_schema(), roots.iter().copied());
        builder
            .with_projection(projection)
            .with_batch_size(PARQUET_BATCH_SIZE)
            .build()
            .map_err(ConnectorError::from)
    }

    fn open_next_reader(&mut self) -> Result<bool, ConnectorError> {
        let Some(file) = self.files.next() else {
            return Ok(false);
        };
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(ParquetObjectReader::new(file?))
                .map_err(ConnectorError::from)?;
        let expected_schema = self
            .expected_schema
            .as_ref()
            .expect("a non-empty Parquet input has a bound schema");
        if builder.schema().fields() != expected_schema.fields() {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Parquet,
                "objects under one prefix do not share the same Arrow schema",
            ));
        }
        self.reader = Some(Self::build_reader(builder, &self.projection_roots)?);
        Ok(true)
    }

    fn bind_batch(
        &self,
        batch: arrow_array::RecordBatch,
    ) -> Result<BoundCopyBatch, ConnectorError> {
        let columns = self
            .columns
            .iter()
            .map(|plan| {
                ColumnReader::bind(&plan.rule, batch.column(plan.source).as_ref())
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(ConnectorError::from)?;
        Ok(BoundCopyBatch {
            columns: columns.into_boxed_slice(),
            rows: batch.num_rows(),
        })
    }

    fn next_batch(&mut self) -> Result<bool, ConnectorError> {
        loop {
            if let Some(reader) = self.reader.as_mut()
                && let Some(batch) = reader.next()
            {
                self.batch = Some(self.bind_batch(batch?)?);
                self.row = 0;
                return Ok(true);
            }
            self.reader = None;
            if !self.open_next_reader()? {
                return Ok(false);
            }
        }
    }

    fn fill_bytes(&mut self) -> Result<bool, CopyError> {
        self.bytes.clear();
        self.position = 0;
        let datum_context = self.datum_context.value();
        let result = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                while self.bytes.len() < BRIDGE_BUFFER_TARGET {
                    if self
                        .batch
                        .as_ref()
                        .is_none_or(|batch| self.row >= batch.rows)
                    {
                        self.batch = None;
                        if !self.next_batch()? {
                            return Ok(false);
                        }
                    }
                    // Output functions can allocate varlena text. Keep that
                    // allocation strictly row-scoped rather than retaining a
                    // bridge-buffer-sized group of temporary values.
                    pg_sys::MemoryContextReset(datum_context);
                    PgMemoryContexts::For(datum_context).switch_to(|_| {
                        let batch = self.batch.as_ref().expect("batch was loaded");
                        for (index, (plan, column)) in
                            self.columns.iter().zip(batch.columns.iter()).enumerate()
                        {
                            if index > 0 {
                                self.bytes.push(b',');
                            }
                            match column.read_datum_unchecked(self.row, plan.codec)? {
                                None => {
                                    self.bytes.extend_from_slice(CanonicalCsv::NULL)
                                }
                                Some(datum) => {
                                    // Intentional CSV bridge; see canonical_csv's
                                    // accepted native-format performance trade-off.
                                    let value = pg_sys::OidOutputFunctionCall(
                                        plan.output_function,
                                        datum,
                                    );
                                    let value = CStr::from_ptr(value);
                                    CanonicalCsv::write_field(
                                        &mut self.bytes,
                                        value.to_bytes(),
                                    );
                                }
                            }
                        }
                        self.bytes.push(b'\n');
                        Ok::<(), ConnectorError>(())
                    })?;
                    self.row += 1;
                }
                Ok::<bool, ConnectorError>(true)
            }))
            .catch_others(|error| {
                Err(ConnectorError::Postgres(PgReportError::from_caught(error)))
            })
            .execute()
        };
        unsafe { self.datum_context.reset() };
        let more = result.map_err(CopyError::from)?;
        Ok(more || !self.bytes.is_empty())
    }
}

impl CopyDataSource for ParquetCopySource {
    fn read(
        &mut self,
        output: &mut [u8],
        min_read: usize,
    ) -> Result<usize, CopyError> {
        let mut written = 0;
        let target = min_read.max(1).min(output.len());
        while written < target {
            if self.position == self.bytes.len() && !self.fill_bytes()? {
                break;
            }
            let available = &self.bytes[self.position..];
            let count = available.len().min(output.len() - written);
            output[written..written + count].copy_from_slice(&available[..count]);
            self.position += count;
            written += count;
            if written == output.len() {
                break;
            }
        }
        Ok(written)
    }
}

impl FormatCopySource for ParquetCopySource {
    fn source(&mut self) -> &mut dyn CopyDataSource {
        self
    }
}
