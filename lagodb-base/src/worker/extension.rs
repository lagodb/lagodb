use std::ffi::CStr;

use pgrx::bgworkers::BackgroundWorker;
use pgrx::prelude::*;

use crate::diag;
use crate::error::LagodbError;
use crate::registry;
use crate::worker::state::WorkerKey;
use lagodb_core::diag::PgReportError;
use lagodb_core::extension_worker::{
    WorkerContextRaw, WorkerSchedule, WorkerTransaction,
};

use super::store::{Store, WorkerStart};

pub(super) struct Worker;

enum WorkerPreparation {
    Ready(pg_sys::FmgrInfo, u32, i32, String, String),
    RegistrationMissing,
    Stale,
}

impl Worker {
    pub(super) fn run(arg: pg_sys::Datum) {
        let key = WorkerKey::from_datum(arg);
        // SAFETY: worker_exit_callback has PostgreSQL's before_shmem_exit ABI,
        // and arg is the scalar WorkerKey supplied to this process.
        unsafe { pg_sys::before_shmem_exit(Some(worker_exit_callback), arg) };
        super::signals::BackgroundWorkerSignals::install_extension_worker();

        let store = Store::new();
        let Some(start) = store.begin_worker(key) else {
            return;
        };
        Self::run_worker_entrypoint(&store, key, start);
    }

    fn run_worker_entrypoint(store: &Store, key: WorkerKey, start: WorkerStart) {
        let database_oid = start.database_oid;
        let worker_id = start.worker_id;
        let extension_oid = start.extension_oid;
        let worker_name = start.worker_name;

        BackgroundWorker::connect_worker_to_spi_by_oid(
            Some(pg_sys::Oid::from(database_oid)),
            None,
        );
        super::injection::InjectionPoints::WORKER_AFTER_DATABASE_CONNECTION.run();
        if !store.validate_worker(key) {
            return;
        }
        let function = WorkerTransaction::run(|| -> Result<_, PgReportError> {
            if !store.validate_worker(key) {
                return Ok(WorkerPreparation::Stale);
            }
            let Some(registration) =
                registry::load_one(worker_id).map_err(PgReportError::from)?
            else {
                return Ok(WorkerPreparation::RegistrationMissing);
            };
            let mut function = std::mem::MaybeUninit::<pg_sys::FmgrInfo>::zeroed();
            // SAFETY: this runs inside an active database transaction, so fmgr can
            // resolve pg_proc and the library. TopMemoryContext outlives the
            // worker callback invoked after this transaction commits.
            unsafe {
                pg_sys::fmgr_info_cxt(
                    registration.function_oid,
                    function.as_mut_ptr(),
                    pg_sys::TopMemoryContext,
                );
            }
            Spi::run("SELECT set_config('search_path', '', false)").map_err(
                |source| {
                    PgReportError::from(LagodbError::WorkerEntrypointPreparation {
                        source,
                    })
                },
            )?;
            // SAFETY: this worker is connected to database_oid. PostgreSQL
            // prevents DROP DATABASE from removing its pg_database row while
            // that connection remains alive, so get_database_name returns a
            // NUL-terminated name. Copy it before the transaction context ends.
            let database_name = unsafe {
                let name = pg_sys::get_database_name(pg_sys::Oid::from(database_oid));
                CStr::from_ptr(name).to_string_lossy().into_owned()
            };
            // SAFETY: returning normally from fmgr_info_cxt above means it
            // initialized every field of FmgrInfo.
            let function = unsafe { function.assume_init() };
            Ok(WorkerPreparation::Ready(
                function,
                registration.registration_owner_oid.to_u32(),
                registration.worker_id,
                registration.worker_name,
                database_name,
            ))
        });
        let (mut function, extension_oid, worker_id, worker_name, database_name) =
            match function {
                Ok(WorkerPreparation::Ready(
                    function,
                    extension_oid,
                    worker_id,
                    worker_name,
                    database_name,
                )) => (
                    function,
                    extension_oid,
                    worker_id,
                    worker_name,
                    database_name,
                ),
                Ok(WorkerPreparation::RegistrationMissing) => {
                    diag::warning(format_args!(
                        "LagoDB worker registration disappeared before start: database_oid={database_oid}, extension_oid={extension_oid}, worker_name={worker_name}"
                    ));
                    store.worker_registration_missing(key);
                    return;
                }
                Ok(WorkerPreparation::Stale) => return,
                Err(error) => {
                    diag::warning(format_args!(
                        "failed to load LagoDB worker entry point: database_oid={database_oid}, extension_oid={extension_oid}, worker_name={worker_name}, error={error}"
                    ));
                    // Entry-point preparation errors must reach the single FFI
                    // exit classifier as a nonzero process exit.
                    unsafe { pg_sys::proc_exit(1) };
                }
            };

        diag::info(format_args!(
            "starting LagoDB extension worker: database={database_name}, database_oid={database_oid}, extension_oid={extension_oid}, worker_name={worker_name}"
        ));

        let worker_context = WorkerContextRaw::new(
            database_oid,
            extension_oid,
            worker_id,
            &worker_name,
            super::signals::BackgroundWorkerSignals::process_config_reload_callback,
            deregister_worker_self,
        );
        if !store.validate_worker(key) {
            return;
        }
        // SAFETY: FmgrInfo was resolved in the committed validation transaction
        // into TopMemoryContext. The registered signature was revalidated as
        // (internal) RETURNS bigint. FunctionCall1Coll is synchronous, so the
        // borrowed context pointer remains valid for the entire invocation.
        let result = unsafe {
            pg_sys::FunctionCall1Coll(
                &mut function,
                pg_sys::InvalidOid,
                pg_sys::Datum::from((&raw const worker_context) as usize),
            )
        };
        // SAFETY: the registered entry point was revalidated as returning
        // bigint, so result is a valid i64 Datum. FunctionCall1Coll cannot
        // represent a SQL NULL result, hence is_null is false.
        let code = unsafe { i64::from_datum(result, false) }
            .expect("validated bigint worker return must decode");
        let schedule = match code {
            -1 => WorkerSchedule::RunImmediately,
            positive if positive > 0 => {
                WorkerSchedule::RunAfter(std::time::Duration::from_millis(
                    u64::try_from(positive)
                        .expect("positive restart delay must fit u64"),
                ))
            }
            0 => WorkerSchedule::Idle,
            unsupported => {
                diag::warning(format_args!(
                    "LagoDB worker returned an unsupported negative restart delay: database_oid={database_oid}, extension_oid={extension_oid}, worker_name={worker_name}, delay={unsupported}"
                ));
                WorkerSchedule::Idle
            }
        };
        if store.complete_worker(key, schedule) {
            store.signal_supervisor();
        }
    }
}

extern "C-unwind" fn deregister_worker_self(worker_id: i32) {
    registry::deregister_self(worker_id).unwrap_or_else(|error| error.report());
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn worker_exit_callback(code: i32, arg: pg_sys::Datum) {
    let key = WorkerKey::from_datum(arg);
    let store = Store::new();
    if store.worker_exit(key, code) {
        store.signal_supervisor();
    }
}
