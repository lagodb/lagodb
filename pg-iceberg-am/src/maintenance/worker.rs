//! Periodic logical Iceberg maintenance scheduled by `pg_lakebase_runtime`.

use std::panic::AssertUnwindSafe;

use pg_lakebase_core::diag::{PgReportError, report_warning};
use pg_lakebase_core::extension_worker::{WorkerContext, WorkerExit};
use pg_lakebase_core::handles::RelationGuard;
use pg_lakebase_core::table_maintenance::{
    LakebaseTableMaintenanceProvider, TableMaintenanceBudget,
    TableMaintenanceCommandTime, TableMaintenanceMode, TableMaintenanceOptions,
    TableMaintenanceRequest,
};
use pgrx::bgworkers::BackgroundWorker;
use pgrx::datum::Internal;
use pgrx::prelude::*;

use super::IcebergTableMaintenanceProvider;

fn candidate_relations() -> Result<Vec<pg_sys::Oid>, PgReportError> {
    let limit = crate::gucs::auto_maintenance_max_tables();
    Spi::connect(|client| {
        let query = format!(
            "SELECT c.oid \
             FROM pg_catalog.pg_class AS c \
             JOIN pg_catalog.pg_am AS a ON a.oid = c.relam \
             WHERE a.amname = 'iceberg' AND c.relkind = 'r' \
             ORDER BY pg_catalog.hashint8(c.oid::int8 # pg_catalog.pg_backend_pid()::int8) \
             LIMIT {limit}"
        );
        client
            .select(&query, None, &[])?
            .map(|row| {
                row.get::<pg_sys::Oid>(1)?.ok_or_else(|| {
                    pgrx::spi::Error::NoTupleTable
                })
            })
            .collect()
    })
    .map_err(|source| {
        PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("failed to discover Iceberg maintenance candidates: {source}"),
        )
    })
}

fn maintain_relation(relid: pg_sys::Oid) -> Result<bool, PgReportError> {
    let locked = unsafe {
        pg_sys::ConditionalLockRelationOid(
            relid,
            pg_sys::ShareUpdateExclusiveLock as pg_sys::LOCKMODE,
        )
    };
    if !locked {
        return Ok(false);
    }
    let relation = RelationGuard::open(relid, pg_sys::NoLock as pg_sys::LOCKMODE)
        .map_err(PgReportError::from_domain_error)?;
    let relation = relation.as_handle();
    let expected_am = <IcebergTableMaintenanceProvider as LakebaseTableMaintenanceProvider>::access_method_oid();
    if expected_am != Some(relation.access_method_oid()) {
        return Ok(false);
    }
    let command_time = TableMaintenanceCommandTime::now()
        .map_err(PgReportError::from_domain_error)?;
    <IcebergTableMaintenanceProvider as LakebaseTableMaintenanceProvider>::execute(
        TableMaintenanceRequest {
            relation: &relation,
            mode: TableMaintenanceMode::Routine,
            options: TableMaintenanceOptions {
                skip_locked: true,
                process_main: true,
                ..TableMaintenanceOptions::default()
            },
            budget: TableMaintenanceBudget::configured(),
            command_time,
        },
    )
    .map_err(PgReportError::from_domain_error)?;
    Ok(true)
}

#[pg_schema]
mod iceberg {
    use super::*;

    #[pg_extern]
    fn automatic_maintenance_worker(worker_context: Internal) -> i64 {
        unsafe { WorkerContext::from_internal(&worker_context) }
            .map_err(|source| {
                PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!("invalid Iceberg maintenance worker context: {source}"),
                )
            })
            .unwrap_or_else(|error| error.report());

        if !crate::gucs::auto_maintenance_enabled() {
            return WorkerExit::RestartAfter(crate::gucs::auto_maintenance_interval())
                .encode();
        }
        let relations = BackgroundWorker::transaction(AssertUnwindSafe(candidate_relations))
            .unwrap_or_else(|error| error.report());
        for relid in relations {
            pgrx::pg_sys::check_for_interrupts!();
            if let Err(error) = BackgroundWorker::transaction(AssertUnwindSafe(|| {
                maintain_relation(relid)
            })) {
                report_warning(format_args!(
                    "automatic Iceberg maintenance skipped relation {}: {}",
                    relid.to_u32(),
                    error,
                ));
            }
        }
        WorkerExit::RestartAfter(crate::gucs::auto_maintenance_interval()).encode()
    }
}
