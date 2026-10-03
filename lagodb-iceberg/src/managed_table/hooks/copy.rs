//! PostgreSQL-format COPY routing for Iceberg partitioned tables.

use lagodb_core::copy::{
    CopyCompletion, CopyContext, CopyEndpoint, CopyError, CopyTargetProbe,
    RoutedCopyFromDriver, RoutedCopyToDriver,
};
use lagodb_core::hooks::{CopyConsumer, CopyRoute, register_copy_consumer};
use pgrx::pg_sys;

use crate::managed_table::catalog::IcebergAccessMethod;

pub(super) struct PartitionedTableCopyConsumer;

impl PartitionedTableCopyConsumer {
    fn owns_target(context: &CopyContext<'_>) -> bool {
        let Some(target) = CopyTargetProbe::new(context.statement()).find() else {
            return false;
        };
        target.relkind() as u8 == pg_sys::RELKIND_PARTITIONED_TABLE
            && IcebergAccessMethod::matches_oid(target.access_method_oid())
    }
}

impl CopyConsumer for PartitionedTableCopyConsumer {
    fn name(&self) -> &'static str {
        "lagodb-iceberg.partitioned-table-copy"
    }

    fn route(&self, context: &CopyContext<'_>) -> Result<CopyRoute, CopyError> {
        // URI COPY belongs to the connector consumer. Passing it through also
        // avoids two consumers claiming a managed partitioned table backed
        // by an object URI.
        if context.statement().endpoint() == CopyEndpoint::ExternalUri {
            return Ok(CopyRoute::PassThrough);
        }
        Ok(if Self::owns_target(context) {
            CopyRoute::Consumed
        } else {
            CopyRoute::PassThrough
        })
    }

    fn consume(
        &self,
        context: &mut CopyContext<'_>,
    ) -> Result<CopyCompletion, CopyError> {
        let parse_state = context.parse_state();
        let processed = if context.statement().is_from() {
            let preparation = context.prepare_from(&parse_state)?;
            let preparation =
                preparation.into_partitioned_table(IcebergAccessMethod::oid())?;
            unsafe {
                RoutedCopyFromDriver::begin(
                    context.statement(),
                    &parse_state,
                    preparation,
                )?
            }
            .execute()?
        } else {
            let preparation = context.prepare_to(&parse_state)?;
            let preparation =
                preparation.into_partitioned_table(IcebergAccessMethod::oid())?;
            unsafe {
                RoutedCopyToDriver::begin(
                    context.statement(),
                    &parse_state,
                    preparation,
                )?
            }
            .execute()?
        };
        parse_state.dispose()?;
        Ok(CopyCompletion::new(processed))
    }
}

pub(super) fn init() {
    register_copy_consumer(Box::new(PartitionedTableCopyConsumer));
}
