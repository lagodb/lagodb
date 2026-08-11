//! Parquet Foreign Table planning and scan execution.

use std::sync::Arc;

use arrow_schema::Schema;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use pg_arrow_conv::{ColumnReader, PgColumnType, resolve_column_rule};
use pg_lakebase_core::fdw::{
    BeginForeignScanContext, ForeignPathBuilder, ForeignPathContext, ForeignPathKeys,
    ForeignPathSpec, ForeignPlanContext, ForeignPlanSpec, ForeignRelSize,
    ForeignRelSizeContext, ReScanForeignScanContext, ScanOutputColumn,
    ScanSlotWriter,
};
use pg_lakebase_core::tuple::{ColumnDatumCodec, ColumnDatumTarget};
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::fdw::Lakebase;
use crate::format::{FormatScanPlanner, FormatScanPrivate, FormatScanState};
use crate::storage::ObjectFiles;

use super::reader::ParquetObjectReader;

const DEFAULT_ESTIMATED_ROWS: f64 = 1_000.0;
const DEFAULT_ESTIMATED_WIDTH: i32 = 32;
const PARQUET_BATCH_SIZE: usize = 8_192;

pub(super) struct ParquetScanPlanner {
    rows: f64,
}

impl ParquetScanPlanner {
    pub(super) const fn new() -> Self {
        Self {
            rows: DEFAULT_ESTIMATED_ROWS,
        }
    }
}

impl FormatScanPlanner for ParquetScanPlanner {
    fn estimate(
        &mut self,
        _context: &ForeignRelSizeContext<'_>,
    ) -> Result<ForeignRelSize, ConnectorError> {
        Ok(ForeignRelSize::new(self.rows, DEFAULT_ESTIMATED_WIDTH))
    }

    fn build_paths(
        &self,
        _context: &ForeignPathContext<'_>,
        paths: &mut ForeignPathBuilder<FormatScanPrivate>,
    ) -> Result<(), ConnectorError> {
        let mut path = ForeignPathSpec::new(
            self.rows,
            0.0,
            self.rows,
            FormatScanPrivate::new(crate::format::FormatKind::Parquet),
        );
        path.retrieved_rows = self.rows;
        paths.push(path);
        Ok(())
    }

    fn supports_pathkeys(
        &self,
        _context: &ForeignPathContext<'_>,
        _pathkeys: &mut ForeignPathKeys,
    ) -> Result<bool, ConnectorError> {
        Ok(false)
    }

    fn build_plan(
        &mut self,
        context: &ForeignPlanContext<'_, Lakebase>,
    ) -> Result<ForeignPlanSpec<FormatScanPrivate>, ConnectorError> {
        Ok(ForeignPlanSpec::new(context.path_private().to_owned()))
    }
}

struct ColumnPlan {
    source: usize,
    rule: pg_arrow_conv::ColumnRule,
    output: ScanOutputColumn,
    codec: ColumnDatumCodec,
}

struct BoundBatch {
    columns: Box<[ColumnReader]>,
    rows: usize,
}

pub(super) struct ParquetScanState {
    files: ObjectFiles,
    reader: Option<ParquetRecordBatchReader>,
    expected_schema: Option<Arc<Schema>>,
    projection_roots: Box<[usize]>,
    columns: Box<[ColumnPlan]>,
    current: Option<BoundBatch>,
    row: usize,
}

impl ParquetScanState {
    pub(super) fn begin(
        context: BeginForeignScanContext<'_, Lakebase>,
        mut files: ObjectFiles,
    ) -> Result<Self, ConnectorError> {
        let Some(first) = files.next() else {
            return Ok(Self {
                files,
                reader: None,
                expected_schema: None,
                projection_roots: Box::new([]),
                columns: Box::new([]),
                current: None,
                row: 0,
            });
        };
        let first = first?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(
            ParquetObjectReader::new(first),
        )?;
        let expected_schema = builder.schema().clone();
        let attr_types = context.relation.attr_types();
        let mut column_names = vec![None; attr_types.len()];
        for (attno, name) in context.relation.live_columns() {
            column_names[(attno - 1) as usize] = Some(name);
        }
        let mut source_roots =
            Vec::with_capacity(context.output_layout.columns().len());
        let mut pending = Vec::with_capacity(context.output_layout.columns().len());

        for output in context.output_layout.columns().iter().copied() {
            let relation_index = (output.attno() - 1) as usize;
            let name = column_names[relation_index].as_deref().ok_or_else(|| {
                ConnectorError::invalid_object_schema(
                    crate::format::FormatKind::Parquet,
                    "a planned output column is not a live relation attribute",
                )
            })?;
            let source = expected_schema.index_of(name).map_err(|_| {
                ConnectorError::invalid_object_schema(
                    crate::format::FormatKind::Parquet,
                    format!("column {name:?} is missing from the Parquet schema"),
                )
            })?;
            let target_oid = attr_types[relation_index].0;
            let pg_type =
                PgColumnType::from_pg_type(target_oid).ok_or_else(|| {
                    ConnectorError::invalid_object_schema(
                        crate::format::FormatKind::Parquet,
                        format!(
                            "PostgreSQL type OID {target_oid} has no Arrow conversion"
                        ),
                    )
                })?;
            let rule = resolve_column_rule(
                expected_schema.field(source).data_type(),
                pg_type,
            )?;
            let codec =
                ColumnDatumCodec::bind(ColumnDatumTarget::from_oid(target_oid))?;
            source_roots.push(source);
            pending.push((source, rule, output, codec));
        }

        source_roots.sort_unstable();
        source_roots.dedup();
        let columns = pending
            .into_iter()
            .map(|(source, rule, output, codec)| ColumnPlan {
                source: source_roots
                    .binary_search(&source)
                    .expect("projected Parquet source was retained"),
                rule,
                output,
                codec,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let reader = Self::build_reader(builder, &source_roots)?;
        Ok(Self {
            files,
            reader: Some(reader),
            expected_schema: Some(expected_schema),
            projection_roots: source_roots.into_boxed_slice(),
            columns,
            current: None,
            row: 0,
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
            .expect("a non-empty Parquet input has a bound schema");
        if builder.schema().fields() != expected_schema.fields() {
            return Err(ConnectorError::invalid_object_schema(
                crate::format::FormatKind::Parquet,
                "objects under one prefix do not share the same Arrow schema",
            ));
        }
        self.reader = Some(Self::build_reader(builder, &self.projection_roots)?);
        Ok(true)
    }

    fn bind_batch(
        &self,
        batch: arrow_array::RecordBatch,
    ) -> Result<BoundBatch, ConnectorError> {
        let columns = self
            .columns
            .iter()
            .map(|plan| {
                ColumnReader::bind(&plan.rule, batch.column(plan.source).as_ref())
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(BoundBatch {
            columns: columns.into_boxed_slice(),
            rows: batch.num_rows(),
        })
    }
}

impl FormatScanState for ParquetScanState {
    fn next_slot(
        &mut self,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ConnectorError> {
        loop {
            if let Some(batch) = self.current.as_ref()
                && self.row < batch.rows
            {
                let row = self.row;
                let mut writer = unsafe { output.datum_writer() };
                for (plan, column) in self.columns.iter().zip(batch.columns.iter()) {
                    let value = unsafe { column.read_datum_unchecked(row, plan.codec) }?;
                    unsafe {
                        writer.write(
                            plan.output,
                            value.unwrap_or(pg_sys::Datum::from(0)),
                            value.is_none(),
                        );
                    }
                }
                self.row += 1;
                return Ok(true);
            }

            self.current = None;
            self.row = 0;
            if let Some(reader) = self.reader.as_mut()
                && let Some(batch) = reader.next()
            {
                self.current = Some(self.bind_batch(batch?)?);
                continue;
            }
            self.reader = None;
            if !self.open_next_reader()? {
                return Ok(false);
            }
        }
    }

    fn rescan(
        &mut self,
        _context: ReScanForeignScanContext<'_, Lakebase>,
    ) -> Result<(), ConnectorError> {
        self.files.reset();
        self.reader = None;
        self.current = None;
        self.row = 0;
        self.open_next_reader()?;
        Ok(())
    }

    fn end(&mut self) -> Result<(), ConnectorError> {
        self.reader = None;
        self.current = None;
        Ok(())
    }
}
