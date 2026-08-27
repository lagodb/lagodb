//! Parquet Foreign Table planning and scan execution.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use lagodb_core::fdw::{
    ForeignPathBuilder, ForeignPathContext, ForeignPathKeys, ForeignPathSpec,
    ForeignPlanContext, ForeignPlanSpec, ForeignRelSize, ForeignRelSizeContext,
    ReScanForeignScanContext, ScanOutputColumn, ScanSlotWriter,
    StartForeignScanContext,
};
use lagodb_core::tuple::{ColumnDatumCodec, ColumnDatumTarget};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use pg_arrow_conv::{ColumnReader, ColumnRule, PgColumnType, resolve_column_rule};
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::fdw::LagodbConnectors;
use crate::format::{
    FormatBoundFilter, FormatKind, FormatScanPlanner, FormatScanPrivate,
    FormatScanState,
};
use crate::storage::ObjectFiles;

use super::{
    ParquetBoundPredicate, ParquetFilePredicate, reader::ParquetObjectReader,
};

const UNANALYZED_FALLBACK_PAGES: pg_sys::BlockNumber = 10;
const PARQUET_STARTUP_COST: f64 = 100.0;
const PARQUET_BATCH_SIZE: usize = 8_192;

pub(super) struct ParquetScanPlanner {
    base_tuples: f64,
    pages: f64,
}

impl ParquetScanPlanner {
    pub(super) const fn new() -> Self {
        Self {
            base_tuples: 0.0,
            pages: 0.0,
        }
    }
}

impl FormatScanPlanner for ParquetScanPlanner {
    fn estimate(
        &mut self,
        context: &ForeignRelSizeContext<'_>,
    ) -> Result<ForeignRelSize, ConnectorError> {
        let estimate = context.local_statistics_estimate(UNANALYZED_FALLBACK_PAGES);
        self.base_tuples = context.relation().base_tuples().max(estimate.rows);
        self.pages = context.relation().base_pages().max(0.0);
        Ok(estimate)
    }

    fn build_paths(
        &self,
        context: &ForeignPathContext<'_>,
        paths: &mut ForeignPathBuilder<FormatScanPrivate>,
    ) -> Result<(), ConnectorError> {
        let rows = context.rows();
        let pruning = context.pruning_estimate();
        let retrieved_rows = (self.base_tuples * pruning.selectivity).max(rows);
        let provider_startup_cost = PARQUET_STARTUP_COST + pruning.startup_cost;
        // PostgreSQL initializes planner cost GUCs before invoking FDW callbacks.
        let seq_page_cost = unsafe { pg_sys::seq_page_cost };
        let provider_total_cost = provider_startup_cost
            + self.pages * seq_page_cost
            + self.base_tuples * pruning.per_tuple_cost;
        let mut path = ForeignPathSpec::new(
            rows,
            provider_startup_cost,
            provider_total_cost,
            FormatScanPrivate::new(FormatKind::Parquet),
        );
        path.retrieved_rows = retrieved_rows;
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
        context: &ForeignPlanContext<'_, LagodbConnectors>,
    ) -> Result<ForeignPlanSpec<FormatScanPrivate>, ConnectorError> {
        Ok(ForeignPlanSpec::new(context.path_private().to_owned()))
    }
}

struct ColumnPlan {
    source: usize,
    rule: ColumnRule,
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
    filters: Box<[ParquetBoundPredicate]>,
    current: Option<BoundBatch>,
    row: usize,
}

impl ParquetScanState {
    pub(super) fn begin(
        context: StartForeignScanContext<'_, LagodbConnectors>,
        mut files: ObjectFiles,
    ) -> Result<Self, ConnectorError> {
        let live = context.relation.live_columns();
        let filters = Self::bound_filters(context.filters.iter());
        for column in live.iter() {
            column.name().to_str().map_err(|_| {
                ConnectorError::invalid_object_schema(
                    crate::format::FormatKind::Parquet,
                    "PostgreSQL column names must be valid UTF-8 for Parquet",
                )
            })?;
        }
        let Some(first) = files.next() else {
            return Ok(Self {
                files,
                reader: None,
                expected_schema: None,
                projection_roots: Box::new([]),
                columns: Box::new([]),
                filters,
                current: None,
                row: 0,
            });
        };
        let first = first?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(
            ParquetObjectReader::new(first),
        )?;
        let expected_schema = builder.schema().clone();
        let mut columns_by_attno = vec![None; context.relation.natts()];
        for column in live.iter() {
            columns_by_attno[(column.attno() - 1) as usize] = Some(column);
        }
        let mut source_roots =
            Vec::with_capacity(context.output_layout.columns().len());
        let mut pending = Vec::with_capacity(context.output_layout.columns().len());

        for output in context.output_layout.columns().iter().copied() {
            let relation_index = (output.attno() - 1) as usize;
            let column = columns_by_attno[relation_index].ok_or_else(|| {
                ConnectorError::invalid_object_schema(
                    crate::format::FormatKind::Parquet,
                    "a planned output column is not a live relation attribute",
                )
            })?;
            let name = column
                .name()
                .to_str()
                .expect("all live Parquet column names were validated as UTF-8");
            let source = expected_schema.index_of(name).map_err(|_| {
                ConnectorError::invalid_object_schema(
                    crate::format::FormatKind::Parquet,
                    format!("column {name:?} is missing from the Parquet schema"),
                )
            })?;
            let target_oid = column.type_oid();
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
        let reader = Self::build_reader(builder, &source_roots, &filters)?;
        Ok(Self {
            files,
            reader: Some(reader),
            expected_schema: Some(expected_schema),
            projection_roots: source_roots.into_boxed_slice(),
            columns,
            filters,
            current: None,
            row: 0,
        })
    }

    fn build_reader(
        builder: ParquetRecordBatchReaderBuilder<ParquetObjectReader>,
        roots: &[usize],
        filters: &[ParquetBoundPredicate],
    ) -> Result<ParquetRecordBatchReader, ConnectorError> {
        let projection =
            ProjectionMask::roots(builder.parquet_schema(), roots.iter().copied());
        let mut builder = builder.with_projection(projection);
        if !filters.is_empty() {
            let predicate = ParquetFilePredicate::try_new(
                filters,
                builder.parquet_schema(),
                builder.schema(),
            )?;
            let selected_row_groups =
                predicate.selected_row_groups(builder.metadata());
            builder = builder.with_row_groups(selected_row_groups);
            builder = builder.with_row_filter(predicate.into_row_filter());
        }
        Ok(builder.with_batch_size(PARQUET_BATCH_SIZE).build()?)
    }

    fn bound_filters<'a>(
        filters: impl IntoIterator<Item = &'a FormatBoundFilter>,
    ) -> Box<[ParquetBoundPredicate]> {
        filters
            .into_iter()
            .map(|filter| filter.parquet().clone())
            .collect::<Vec<_>>()
            .into_boxed_slice()
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
        self.reader = Some(Self::build_reader(
            builder,
            &self.projection_roots,
            &self.filters,
        )?);
        Ok(true)
    }

    fn bind_batch(&self, batch: RecordBatch) -> Result<BoundBatch, ConnectorError> {
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
                    let value =
                        unsafe { column.read_datum_unchecked(row, plan.codec) }?;
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
        context: ReScanForeignScanContext<'_, LagodbConnectors>,
    ) -> Result<(), ConnectorError> {
        if context.filters_changed {
            self.filters = Self::bound_filters(context.filters.iter());
        }
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
