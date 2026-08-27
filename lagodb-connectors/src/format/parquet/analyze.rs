//! Parquet-backed PostgreSQL `ANALYZE` sampling.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use lagodb_core::fdw::{
    ForeignAnalyzeSupport, ForeignSampleContext, ForeignSampleStatistics,
    ForeignTableMaintenanceError,
};
use lagodb_core::handles::HeapTupleGuard;
use lagodb_core::tuple::{ColumnDatumCodec, ColumnDatumTarget};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use pg_arrow_conv::{ColumnReader, ColumnRule, PgColumnType, resolve_column_rule};
use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::format::{FormatAnalyzer, FormatKind};
use crate::storage::ObjectFiles;

use super::reader::ParquetObjectReader;

const PARQUET_BATCH_SIZE: usize = 8_192;
const MAX_VALID_BLOCK_NUMBER: u64 = pg_sys::BlockNumber::MAX as u64 - 1;

pub(super) struct ParquetAnalyze;

impl FormatAnalyzer for ParquetAnalyze {
    fn support(&self, total_bytes: u64) -> ForeignAnalyzeSupport {
        let pages = total_bytes.div_ceil(pg_sys::BLCKSZ as u64);
        ForeignAnalyzeSupport::new(pages.min(MAX_VALID_BLOCK_NUMBER) as _)
    }

    fn acquire_sample_rows(
        self: Box<Self>,
        context: &mut ForeignSampleContext<'_>,
        files: ObjectFiles,
    ) -> Result<ForeignSampleStatistics, ForeignTableMaintenanceError> {
        let mut analyzer = ParquetAnalyzer::bind(context, files)
            .map_err(ForeignTableMaintenanceError::from)?;
        analyzer.sample(context)
    }
}

struct AnalyzeColumn {
    source: usize,
    destination: usize,
    rule: ColumnRule,
    codec: ColumnDatumCodec,
}

struct AnalyzeBindings {
    roots: Box<[usize]>,
    columns: Box<[AnalyzeColumn]>,
}

struct BoundBatch {
    columns: Box<[ColumnReader]>,
    rows: usize,
}

struct ParquetAnalyzer {
    files: ObjectFiles,
    reader: Option<ParquetRecordBatchReader>,
    expected_schema: Option<Arc<Schema>>,
    roots: Box<[usize]>,
    columns: Box<[AnalyzeColumn]>,
    values: Box<[pg_sys::Datum]>,
    nulls: Box<[bool]>,
    tuple_desc: pg_sys::TupleDesc,
    datum_context: PgMemoryContexts,
}

impl ParquetAnalyzer {
    fn bind(
        context: &ForeignSampleContext<'_>,
        mut files: ObjectFiles,
    ) -> Result<Self, ConnectorError> {
        let natts = context.relation().natts();
        let Some(file) = files.next() else {
            return Ok(Self {
                files,
                reader: None,
                expected_schema: None,
                roots: Box::new([]),
                columns: Box::new([]),
                values: vec![pg_sys::Datum::from(0); natts].into_boxed_slice(),
                nulls: vec![true; natts].into_boxed_slice(),
                tuple_desc: context.relation().tuple_desc(),
                datum_context: PgMemoryContexts::new("lagodb parquet ANALYZE datum"),
            });
        };
        let builder = ParquetRecordBatchReaderBuilder::try_new(
            ParquetObjectReader::new(file?),
        )?;
        let expected_schema = builder.schema().clone();
        let bindings = Self::bind_columns(context, &expected_schema)?;
        let reader = Self::build_reader(builder, &bindings.roots)?;
        Ok(Self {
            files,
            reader: Some(reader),
            expected_schema: Some(expected_schema),
            roots: bindings.roots,
            columns: bindings.columns,
            values: vec![pg_sys::Datum::from(0); natts].into_boxed_slice(),
            nulls: vec![true; natts].into_boxed_slice(),
            tuple_desc: context.relation().tuple_desc(),
            datum_context: PgMemoryContexts::new("lagodb parquet ANALYZE datum"),
        })
    }

    fn bind_columns(
        context: &ForeignSampleContext<'_>,
        schema: &Schema,
    ) -> Result<AnalyzeBindings, ConnectorError> {
        let live = context.relation().live_columns();
        let mut roots = Vec::with_capacity(live.len());
        let mut pending = Vec::with_capacity(live.len());
        for column in live.iter() {
            let name = column.name().to_str().map_err(|_| {
                ConnectorError::invalid_object_schema(
                    FormatKind::Parquet,
                    "PostgreSQL column names must be valid UTF-8 for Parquet",
                )
            })?;
            let source = schema.index_of(name).map_err(|_| {
                ConnectorError::invalid_object_schema(
                    FormatKind::Parquet,
                    format!("column {name:?} is missing from the Parquet schema"),
                )
            })?;
            let target_oid = column.type_oid();
            let pg_type =
                PgColumnType::from_pg_type(target_oid).ok_or_else(|| {
                    ConnectorError::invalid_object_schema(
                        FormatKind::Parquet,
                        format!(
                            "PostgreSQL type OID {target_oid} has no Arrow conversion"
                        ),
                    )
                })?;
            let rule =
                resolve_column_rule(schema.field(source).data_type(), pg_type)?;
            let codec =
                ColumnDatumCodec::bind(ColumnDatumTarget::from_oid(target_oid))?;
            roots.push(source);
            pending.push((source, (column.attno() - 1) as usize, rule, codec));
        }
        roots.sort_unstable();
        roots.dedup();
        let columns = pending
            .into_iter()
            .map(|(source, destination, rule, codec)| AnalyzeColumn {
                source: roots
                    .binary_search(&source)
                    .expect("projected ANALYZE source was retained"),
                destination,
                rule,
                codec,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(AnalyzeBindings {
            roots: roots.into_boxed_slice(),
            columns,
        })
    }

    fn build_reader(
        builder: ParquetRecordBatchReaderBuilder<ParquetObjectReader>,
        roots: &[usize],
    ) -> Result<ParquetRecordBatchReader, ConnectorError> {
        let projection =
            ProjectionMask::roots(builder.parquet_schema(), roots.iter().copied());
        Ok(builder
            .with_projection(projection)
            .with_batch_size(PARQUET_BATCH_SIZE)
            .build()?)
    }

    fn open_next_reader(&mut self) -> Result<bool, ConnectorError> {
        let Some(file) = self.files.next() else {
            return Ok(false);
        };
        let builder = ParquetRecordBatchReaderBuilder::try_new(
            ParquetObjectReader::new(file?),
        )?;
        let expected_schema = self
            .expected_schema
            .as_ref()
            .expect("a non-empty Parquet ANALYZE input has a bound schema");
        if builder.schema().fields() != expected_schema.fields() {
            return Err(ConnectorError::invalid_object_schema(
                FormatKind::Parquet,
                "objects under one prefix do not share the same Arrow schema",
            ));
        }
        self.reader = Some(Self::build_reader(builder, &self.roots)?);
        Ok(true)
    }

    fn bind_batch(&self, batch: RecordBatch) -> Result<BoundBatch, ConnectorError> {
        let columns = self
            .columns
            .iter()
            .map(|column| {
                ColumnReader::bind(&column.rule, batch.column(column.source).as_ref())
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(BoundBatch {
            columns: columns.into_boxed_slice(),
            rows: batch.num_rows(),
        })
    }

    fn sample(
        &mut self,
        context: &mut ForeignSampleContext<'_>,
    ) -> Result<ForeignSampleStatistics, ForeignTableMaintenanceError> {
        let target_rows = context.target_rows();
        let mut selection_state = pg_sys::ReservoirStateData::default();
        if target_rows != 0 {
            unsafe {
                pg_sys::reservoir_init_selection_state(
                    &mut selection_state,
                    target_rows as i32,
                )
            };
        }
        let mut rows_to_skip = -1.0;
        let mut total_rows = 0.0;

        loop {
            let batch = loop {
                if let Some(reader) = self.reader.as_mut()
                    && let Some(batch) = reader.next()
                {
                    break Some(
                        self.bind_batch(batch.map_err(ConnectorError::from)?)
                            .map_err(ForeignTableMaintenanceError::from)?,
                    );
                }
                self.reader = None;
                if !self
                    .open_next_reader()
                    .map_err(ForeignTableMaintenanceError::from)?
                {
                    break None;
                }
            };
            let Some(batch) = batch else {
                break;
            };
            pg_sys::check_for_interrupts!();
            for row in 0..batch.rows {
                let destination = if context.len() < target_rows {
                    Some(context.len())
                } else if target_rows == 0 {
                    None
                } else {
                    if rows_to_skip < 0.0 {
                        rows_to_skip = unsafe {
                            pg_sys::reservoir_get_next_S(
                                &mut selection_state,
                                total_rows,
                                target_rows as i32,
                            )
                        };
                    }
                    let destination = if rows_to_skip <= 0.0 {
                        let selected = (target_rows as f64
                            * unsafe {
                                pg_sys::sampler_random_fract(
                                    &mut selection_state.randstate,
                                )
                            }) as usize;
                        Some(selected.min(target_rows - 1))
                    } else {
                        None
                    };
                    rows_to_skip -= 1.0;
                    destination
                };
                // Vitter's `t` is the number of rows processed before this
                // row; advance the population count only after selection.
                total_rows += 1.0;
                let Some(destination) = destination else {
                    continue;
                };
                let tuple = self
                    .form_tuple(&batch, row)
                    .map_err(ForeignTableMaintenanceError::from)?;
                if destination == context.len() {
                    context.push(tuple)?;
                } else {
                    context.replace(destination, tuple)?;
                }
            }
        }
        Ok(ForeignSampleStatistics::new(total_rows, 0.0))
    }

    fn form_tuple(
        &mut self,
        batch: &BoundBatch,
        row: usize,
    ) -> Result<HeapTupleGuard, ConnectorError> {
        self.nulls.fill(true);
        let values = &mut self.values;
        let nulls = &mut self.nulls;
        let columns = &self.columns;
        unsafe {
            self.datum_context.switch_to(|_| {
                for (plan, column) in columns.iter().zip(batch.columns.iter()) {
                    let value = column.read_datum_unchecked(row, plan.codec)?;
                    if let Some(value) = value {
                        values[plan.destination] = value;
                        nulls[plan.destination] = false;
                    }
                }
                Ok::<(), ConnectorError>(())
            })?;
        }
        let tuple = unsafe {
            HeapTupleGuard::new(pg_sys::heap_form_tuple(
                // The live ANALYZE relation owns this descriptor for the full
                // callback. `heap_form_tuple` copies all pass-by-reference data
                // before the per-row datum context is reset.
                self.tuple_desc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
            ))
        };
        unsafe { self.datum_context.reset() };
        Ok(tuple)
    }
}
