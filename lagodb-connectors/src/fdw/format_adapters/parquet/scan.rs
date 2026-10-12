//! Parquet Foreign Table planning and scan execution.

use lagodb_arrow::BoundBatch;
use lagodb_core::fdw::{
    ForeignPathBuilder, ForeignPathContext, ForeignPathKeys, ForeignPathSpec,
    ForeignPlanContext, ForeignPlanSpec, ForeignRelSize, ForeignRelSizeContext,
    ReScanForeignScanContext, ScanSlotWriter, StartForeignScanContext,
};
use lagodb_core::tuple::PgType;
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::fdw::LagodbConnectors;
use crate::fdw::format_adapters::{
    FormatBoundFilter, FormatScanPlanner, FormatScanPrivate, FormatScanState,
};
use crate::format::FormatKind;
use crate::format::parquet::{
    ParquetBatchSource, ParquetBoundPredicate, ParquetReadMode, ParquetReadPlan,
};
use crate::storage::InputFiles;

const UNANALYZED_FALLBACK_PAGES: pg_sys::BlockNumber = 10;
const PARQUET_STARTUP_COST: f64 = 100.0;

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

pub(super) struct ParquetScanState {
    source: ParquetBatchSource,
    current: Option<BoundBatch>,
    row: usize,
}

impl ParquetScanState {
    pub(super) fn begin(
        context: StartForeignScanContext<'_, LagodbConnectors>,
        files: InputFiles,
    ) -> Result<Self, ConnectorError> {
        let live = context.relation.live_columns();
        let mut columns_by_attno = vec![None; context.relation.natts()];
        for column in live.iter() {
            columns_by_attno[(column.attno() - 1) as usize] = Some(column);
        }
        let source = ParquetBatchSource::open(
            files,
            Self::bound_filters(context.filters.iter()),
            |schema| {
                ParquetReadPlan::bind(
                    schema,
                    context.output_layout.columns().iter().map(|output| {
                        let column = columns_by_attno[(output.attno() - 1) as usize]
                            .expect(
                                "scan output refers to a live relation attribute",
                            );
                        let (oid, typmod) =
                            context.output_layout.slot_types()[output.destination()];
                        (
                            column.name(),
                            PgType::new(oid, typmod),
                            output.destination(),
                        )
                    }),
                    ParquetReadMode::Declared,
                )
            },
        )?;
        Ok(Self {
            source,
            current: None,
            row: 0,
        })
    }

    fn bound_filters<'a>(
        filters: impl IntoIterator<Item = &'a FormatBoundFilter>,
    ) -> Box<[ParquetBoundPredicate]> {
        filters
            .into_iter()
            .map(|filter| filter.parquet().clone())
            .collect()
    }
}

impl FormatScanState for ParquetScanState {
    fn next_slot(
        &mut self,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ConnectorError> {
        if self
            .current
            .as_ref()
            .is_none_or(|batch| self.row >= batch.num_rows())
        {
            self.current = None;
            self.current = self.source.next_batch()?;
            self.row = 0;
        }
        let Some(batch) = self.current.as_ref() else {
            return Ok(false);
        };
        let mut columns = unsafe { output.datum_columns() };
        // SAFETY: Begin bound every destination from this scan's output layout;
        // the batch row is in bounds and PG selected its per-tuple context.
        unsafe { batch.write_row_unchecked(self.row, &mut columns) }?;
        self.row += 1;
        Ok(true)
    }

    fn rescan(
        &mut self,
        context: ReScanForeignScanContext<'_, LagodbConnectors>,
    ) -> Result<(), ConnectorError> {
        self.current = None;
        self.row = 0;
        let filters = context
            .filters_changed
            .then(|| Self::bound_filters(context.filters.iter()));
        self.source.reset(filters);
        Ok(())
    }

    fn end(&mut self) -> Result<(), ConnectorError> {
        self.current = None;
        self.source.close_reader();
        Ok(())
    }
}
