//! Parquet-backed PostgreSQL ANALYZE sampling.

use lagodb_arrow::BoundBatch;
use lagodb_core::fdw::{
    ForeignAnalyzeSupport, ForeignSampleContext, ForeignSampleStatistics,
    ForeignTableMaintenanceError,
};
use lagodb_core::handles::HeapTupleGuard;
use lagodb_core::tuple::PgType;
use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::fdw::format_adapters::FormatAnalyzer;
use crate::format::parquet::{ParquetBatchSource, ParquetReadMode, ParquetReadPlan};
use crate::storage::InputFiles;

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
        files: InputFiles,
    ) -> Result<ForeignSampleStatistics, ForeignTableMaintenanceError> {
        let mut analyzer = ParquetAnalyzer::bind(context, files)
            .map_err(ForeignTableMaintenanceError::from)?;
        analyzer.sample(context)
    }
}

struct ParquetAnalyzer {
    source: ParquetBatchSource,
    values: Box<[pg_sys::Datum]>,
    nulls: Box<[bool]>,
    tuple_desc: pg_sys::TupleDesc,
    datum_context: PgMemoryContexts,
}

impl ParquetAnalyzer {
    fn bind(
        context: &ForeignSampleContext<'_>,
        files: InputFiles,
    ) -> Result<Self, ConnectorError> {
        let natts = context.relation().natts();
        let live = context.relation().live_columns();
        let source = ParquetBatchSource::open(files, Box::new([]), |schema| {
            ParquetReadPlan::bind(
                schema,
                live.iter().map(|column| {
                    (
                        column.name(),
                        PgType::new(column.type_oid(), column.type_mod()),
                        (column.attno() - 1) as usize,
                    )
                }),
                ParquetReadMode::Declared,
            )
        })?;
        Ok(Self {
            source,
            values: vec![pg_sys::Datum::from(0); natts].into_boxed_slice(),
            nulls: vec![true; natts].into_boxed_slice(),
            tuple_desc: context.relation().tuple_desc(),
            datum_context: PgMemoryContexts::new("lagodb parquet ANALYZE datum"),
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
            let batch = self
                .source
                .next_batch()
                .map_err(ForeignTableMaintenanceError::from)?;
            let Some(batch) = batch else {
                break;
            };
            pg_sys::check_for_interrupts!();
            for row in 0..batch.num_rows() {
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
        unsafe {
            self.datum_context.switch_to(|_| {
                for column in batch.columns() {
                    let value = column.read_datum_unchecked(row)?;
                    if let Some(value) = value {
                        values[column.destination()] = value;
                        nulls[column.destination()] = false;
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
