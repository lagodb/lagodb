//! Executor state backed by `ScanSpec` and the shared query cursor core.

use pg_lakebase_core::fdw::{
    BeginForeignScanContext, ForeignRowIdentityRequirement, ForeignScanError,
    ReScanForeignScanContext, ScanSlotWriter, StartForeignScanContext,
};

use super::super::error::IcebergFdwError;
use super::super::provider::LagodbIceberg;
use super::super::relation::RestForeignTable;
use super::super::schema::ForeignSchemaBinding;
use super::super::source_identity::PlanSourceIdentity;
use crate::engine::predicate::BoundIcebergPredicate;
use crate::engine::scan::projection::{ProjectedField, Projection};
use crate::engine::scan::{IcebergQueryCursor, ScanSource, ScanSpec};
use crate::engine::write::RelationRowRegistry;

use super::super::transaction::ForeignTransaction;
use super::ForeignMutationScan;
use super::cursor::ForeignMutationCursor;

enum ForeignScanCursor {
    Prepared,
    Query(IcebergQueryCursor),
    Mutation(ForeignMutationCursor),
}

pub(crate) struct IcebergFdwScanState {
    // Declaration order is intentional: the cursor releases readers before
    // the ScanSpec-owned table/FileIO is dropped by the framework at End.
    cursor: ForeignScanCursor,
    spec: ScanSpec,
    mutation_registry: Option<RelationRowRegistry>,
    mutation_context: Option<ForeignMutationScan>,
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
        Ok(Self {
            cursor: ForeignScanCursor::Prepared,
            spec,
            mutation_registry,
            mutation_context,
        })
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
        self.cursor = self.open_cursor()?;
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

    pub(crate) fn rescan(
        &mut self,
        context: ReScanForeignScanContext<'_, LagodbIceberg>,
    ) -> Result<(), ForeignScanError> {
        if context.filters_changed {
            self.spec.set_row_filter(BoundIcebergPredicate::conjoin(
                context.filters.iter(),
            ));
        }
        self.cursor = self.open_cursor()?;
        Ok(())
    }
}
