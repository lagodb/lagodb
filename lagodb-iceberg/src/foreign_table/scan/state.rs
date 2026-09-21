//! Executor state backed by `ScanSpec` and the shared query cursor core.

use std::sync::Arc;

use lagodb_core::fdw::{
    BeginForeignScanContext, ForeignRowIdentityRequirement, ForeignScanError,
    ReScanForeignScanContext, ScanSlotWriter, StartForeignScanContext,
};
use lagodb_core::parallel_scan::{ParallelScanCoordinator, PreparedParallelScan};
use pgrx::pg_sys;

use super::super::error::IcebergFdwError;
use super::super::provider::LagodbIceberg;
use super::super::relation::RestForeignTable;
use super::super::schema::ForeignSchemaBinding;
use super::super::source_identity::PlanSourceIdentity;
use crate::predicate::BoundIcebergPredicate;
use crate::scan::ScanError;
use crate::scan::parallel::{TaskGrouping, WorkerSource};
use crate::scan::projection::{ProjectedField, Projection};
use crate::scan::{QueryCursor, ScanSource, ScanSpec};
use crate::write::RelationRowRegistry;

use super::super::transaction::ForeignTransaction;
use super::ForeignMutationScan;
use super::cursor::ForeignMutationCursor;

enum ForeignScanCursor {
    Prepared,
    Query(QueryCursor),
    Mutation(ForeignMutationCursor),
}

pub(crate) struct IcebergFdwScanState {
    // Declaration order is intentional: the cursor releases readers before
    // the ScanSpec-owned table/FileIO is dropped by the framework at End.
    cursor: ForeignScanCursor,
    spec: ScanSpec,
    mutation_registry: Option<RelationRowRegistry>,
    mutation_context: Option<ForeignMutationScan>,
    parallel: ParallelScanCoordinator,
    parallel_source: Option<WorkerSource>,
    parallel_aware: bool,
}

impl IcebergFdwScanState {
    pub(crate) fn begin(
        context: BeginForeignScanContext<'_, LagodbIceberg>,
    ) -> Result<Self, ForeignScanError> {
        let resolved = RestForeignTable::resolve(
            context.relation.oid(),
            context.effective_user_id(),
        )?;
        if resolved.identity() != context.private_data.identity() {
            return Err(IcebergFdwError::PlanIdentityChanged.into());
        }

        let source = PlanSourceIdentity::from_table(resolved.table());
        if context
            .private_data
            .source()
            .is_some_and(|planned| planned != &source)
        {
            return Err(IcebergFdwError::PlanSourceChanged.into());
        }
        let mutation = matches!(
            context.row_identity_requirement,
            ForeignRowIdentityRequirement::ItemPointer
        );
        if mutation && context.parallel_aware {
            return Err(IcebergFdwError::InvalidPlan {
                detail: "mutation ForeignScan cannot be parallel-aware",
            }
            .into());
        }
        let view = if mutation {
            ForeignTransaction::begin_write(resolved)?
        } else {
            ForeignTransaction::scan_view(resolved)?
        };
        let table = view.table;
        let mutation_table = mutation.then(|| table.clone());
        let schema = table.metadata().current_schema();
        let shape = ForeignSchemaBinding::bind(&context.relation, schema)?
            .into_relation_shape();
        let mut columns = context
            .output_layout
            .columns()
            .iter()
            .map(|column| ProjectedField::new(column.attno(), column.destination()))
            .collect::<Vec<_>>();
        columns.sort_unstable_by_key(|column| column.attno);
        let projection = Projection::new(columns);
        let planning_filter =
            BoundIcebergPredicate::conjoin(context.filters.rescan_stable());
        let row_filter = planning_filter.clone();
        let mut spec = ScanSpec::projected(
            ScanSource::transaction_view(table, view.delta, None),
            projection,
            planning_filter,
            row_filter,
            &shape,
            context.output_layout.slot_types(),
        )
        .map_err(IcebergFdwError::from)?;
        let mutation_registry = if mutation {
            Some(ForeignTransaction::row_registry(&view.key)?)
        } else {
            None
        };
        let mutation_context = if mutation {
            spec.prepare_mutation_tasks()
                .map_err(IcebergFdwError::from)?;
            let tasks = spec.prepared_mutation_tasks().ok_or_else(|| {
                IcebergFdwError::InvalidPlan {
                    detail: "mutation scan did not retain its planned tasks",
                }
            })?;
            Some(ForeignMutationScan::new(
                context.private_data.identity().clone(),
                view.key,
                mutation_table
                    .expect("mutation scan retains its transaction-view table"),
                shape,
                spec.starting_snapshot_id(),
                tasks,
            ))
        } else {
            None
        };
        let mut state = Self {
            cursor: ForeignScanCursor::Prepared,
            spec,
            mutation_registry,
            mutation_context,
            parallel: ParallelScanCoordinator::default(),
            parallel_source: None,
            parallel_aware: context.parallel_aware,
        };
        if state.parallel_aware && unsafe { pg_sys::ParallelWorkerNumber } < 0 {
            state.prepare_parallel()?;
        }
        Ok(state)
    }

    pub(crate) fn start(
        &mut self,
        context: StartForeignScanContext<'_, LagodbIceberg>,
    ) -> Result<(), ForeignScanError> {
        if !matches!(self.cursor, ForeignScanCursor::Prepared) {
            return Err(IcebergFdwError::InvalidPlan {
                detail: "Iceberg scan was started more than once",
            }
            .into());
        }
        self.spec
            .set_row_filter(BoundIcebergPredicate::conjoin(context.filters.iter()));
        if !self.parallel_aware {
            self.cursor = self.open_cursor()?;
        }
        Ok(())
    }

    pub(crate) fn mutation_context(&self) -> Option<ForeignMutationScan> {
        self.mutation_context.clone()
    }

    fn open_cursor(&mut self) -> Result<ForeignScanCursor, ForeignScanError> {
        match self.mutation_registry.as_ref() {
            Some(registry) => {
                Ok(ForeignScanCursor::Mutation(ForeignMutationCursor::new(
                    self.spec.mutation_input().map_err(IcebergFdwError::from)?,
                    registry.clone(),
                )))
            }
            None => Ok(ForeignScanCursor::Query(
                self.spec
                    .open_query_cursor()
                    .map_err(IcebergFdwError::from)?,
            )),
        }
    }

    pub(crate) fn next_slot(
        &mut self,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ForeignScanError> {
        if self.parallel_aware {
            return self.next_parallel_slot(output);
        }
        match &mut self.cursor {
            ForeignScanCursor::Prepared => Err(IcebergFdwError::InvalidPlan {
                detail: "Iceberg scan cursor was not started",
            }
            .into()),
            ForeignScanCursor::Mutation(cursor) => cursor.next_slot(output),
            ForeignScanCursor::Query(cursor) => cursor
                .next_with(|decoder, batch, row_index| {
                    // SAFETY: Begin compiled the decoder from this exact output
                    // layout; the callback writes one complete datum row and the
                    // framework owns the slot for the duration of this closure.
                    let mut columns = unsafe { output.datum_columns() };
                    unsafe {
                        decoder.write_row_unchecked(batch, row_index, &mut columns)
                    }?;
                    Ok(())
                })
                .map_err(ForeignScanError::from),
        }
    }

    fn next_parallel_slot(
        &mut self,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ForeignScanError> {
        loop {
            let cursor =
                std::mem::replace(&mut self.cursor, ForeignScanCursor::Prepared);
            if let ForeignScanCursor::Query(mut cursor) = cursor {
                let produced = cursor
                    .next_with(|decoder, batch, row_index| {
                        let mut columns = unsafe { output.datum_columns() };
                        unsafe {
                            decoder.write_row_unchecked(
                                batch,
                                row_index,
                                &mut columns,
                            )
                        }?;
                        Ok(())
                    })
                    .map_err(ForeignScanError::from)?;
                if produced {
                    self.cursor = ForeignScanCursor::Query(cursor);
                    return Ok(true);
                }
            }
            let Some(work_id) =
                self.parallel.claim().map_err(ForeignScanError::provider)?
            else {
                return Ok(false);
            };
            let tasks = self
                .parallel_source
                .as_ref()
                .ok_or_else(|| IcebergFdwError::InvalidPlan {
                    detail: "parallel Iceberg source is not attached",
                })?
                .take_tasks(work_id)
                .map_err(ForeignScanError::provider)?;
            self.cursor = ForeignScanCursor::Query(
                self.spec
                    .open_query_cursor_with_tasks(Arc::from(tasks.into_boxed_slice()))
                    .map_err(IcebergFdwError::from)?,
            );
        }
    }

    pub(crate) fn rescan(
        &mut self,
        context: ReScanForeignScanContext<'_, LagodbIceberg>,
    ) -> Result<(), ForeignScanError> {
        if context.filters_changed {
            self.spec.set_row_filter(BoundIcebergPredicate::conjoin(
                context.filters.iter(),
            ));
        }
        if self.parallel_aware {
            self.cursor = ForeignScanCursor::Prepared;
            self.attach_parallel_source()?;
        } else {
            self.cursor = self.open_cursor()?;
        }
        Ok(())
    }

    fn prepare_parallel(&mut self) -> Result<(), ForeignScanError> {
        let tasks = self
            .spec
            .planned_query_tasks()
            .map_err(IcebergFdwError::from)?;
        let grouped = TaskGrouping::from_properties(self.spec.table_properties())
            .map_err(ForeignScanError::provider)?
            .group(&tasks)
            .map_err(ForeignScanError::provider)?;
        let work_count = u32::try_from(grouped.group_count()).map_err(|_| {
            IcebergFdwError::InvalidPlan {
                detail: "native parallel group count exceeds u32",
            }
        })?;
        let bytes = WorkerSource::encode(
            &self
                .spec
                .query_arrow_schema()
                .map_err(IcebergFdwError::from)?,
            &tasks,
            grouped,
        )
        .map_err(ForeignScanError::provider)?;
        self.parallel.prepare(
            PreparedParallelScan::new(bytes, work_count)
                .map_err(ForeignScanError::provider)?,
        );
        Ok(())
    }

    pub(crate) fn estimate_dsm(&mut self) -> Result<pg_sys::Size, ForeignScanError> {
        self.parallel.estimate().map_err(ForeignScanError::provider)
    }

    pub(crate) unsafe fn initialize_dsm(
        &mut self,
        coordinate: *mut core::ffi::c_void,
    ) -> Result<(), ForeignScanError> {
        unsafe { self.parallel.initialize(coordinate) }
            .map_err(ForeignScanError::provider)?;
        self.attach_parallel_source()
    }

    pub(crate) unsafe fn reinitialize_dsm(
        &mut self,
        coordinate: *mut core::ffi::c_void,
    ) -> Result<(), ForeignScanError> {
        unsafe { self.parallel.attach(coordinate) }
            .map_err(ForeignScanError::provider)?;
        self.parallel
            .reinitialize()
            .map_err(ForeignScanError::provider)?;
        Ok(())
    }

    pub(crate) unsafe fn initialize_worker(
        &mut self,
        coordinate: *mut core::ffi::c_void,
    ) -> Result<(), ForeignScanError> {
        unsafe { self.parallel.attach(coordinate) }
            .map_err(ForeignScanError::provider)?;
        self.attach_parallel_source()
    }

    fn attach_parallel_source(&mut self) -> Result<(), ForeignScanError> {
        // SAFETY: `parallel_source` is cleared before the coordinator detaches
        // this immutable native-parallel DSM payload on every lifecycle path.
        let source = unsafe {
            WorkerSource::decode_shared(
                self.parallel
                    .payload()
                    .map_err(ForeignScanError::provider)?,
                self.spec.file_io(),
            )
        }
        .map_err(ForeignScanError::provider)?;
        let expected = self
            .spec
            .query_arrow_schema()
            .map_err(IcebergFdwError::from)?;
        if source.schema().as_ref() != expected.as_ref() {
            return Err(ForeignScanError::provider(ScanError::WorkerPayload(
                "worker relation view does not match the leader scan schema"
                    .to_owned(),
            )));
        }
        self.parallel_source = Some(source);
        Ok(())
    }

    pub(crate) fn shutdown_parallel(&mut self) {
        self.cursor = ForeignScanCursor::Prepared;
        self.parallel_source = None;
        self.parallel.detach();
    }
}
