use std::ffi::CStr;
use std::panic::AssertUnwindSafe;

use pgrx::bgworkers::BackgroundWorker;
use pgrx::prelude::*;

use crate::error::LakebaseError;
use crate::registry;
use pg_lakebase_core::extension_worker::{WorkerContextRaw, WorkerDirective};

use super::CRASH_BACKOFF;
use super::bgworker::WorkerToken;
use super::store::{RuntimeStore, WorkerStart};

pub(super) struct ExtensionWorker;

impl ExtensionWorker {
    pub(super) fn run(arg: pg_sys::Datum) {
        let token = WorkerToken::from_datum(arg);
        // SAFETY: worker_exit_callback has PostgreSQL's before_shmem_exit ABI,
        // and arg is the scalar worker token supplied to this process.
        unsafe { pg_sys::before_shmem_exit(Some(worker_exit_callback), arg) };
        super::signals::BackgroundWorkerSignals::install_dynamic_worker();
        #[cfg(feature = "pg_test")]
        super::test_support::RuntimeTestInjection::before_worker_start();

        let store = RuntimeStore::new();
        let Some(start) = store.begin_worker(token) else {
            return;
        };
        Self::run_worker_entrypoint(&store, token, start);
    }

    fn run_worker_entrypoint(
        store: &RuntimeStore,
        token: WorkerToken,
        start: WorkerStart,
    ) {
        let database_oid = start.database_oid;
        let extension_oid = start.extension_oid;
        let worker_name = start.worker_name;

        BackgroundWorker::connect_worker_to_spi_by_oid(
            Some(pg_sys::Oid::from(database_oid)),
            None,
        );
        #[cfg(feature = "pg_test")]
        super::test_support::RuntimeTestInjection::after_running(database_oid);
        if !store.validate_worker_token(token) {
            return;
        }
        let function = BackgroundWorker::transaction(AssertUnwindSafe(|| {
            if !store.validate_worker_token(token) {
                return Ok::<_, LakebaseError>(None);
            }
            let Some(registration) =
                registry::load_one(pg_sys::Oid::from(extension_oid), &worker_name)?
            else {
                return Ok::<_, LakebaseError>(None);
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
                |source| LakebaseError::WorkerEntrypointPreparation { source },
            )?;
            // SAFETY: get_database_name returns either null or a palloc'd,
            // NUL-terminated name valid in the current transaction context. The
            // pointer is read immediately and copied into an owned String.
            let database_name = unsafe {
                let name = pg_sys::get_database_name(pg_sys::Oid::from(database_oid));
                if name.is_null() {
                    database_oid.to_string()
                } else {
                    CStr::from_ptr(name).to_string_lossy().into_owned()
                }
            };
            // SAFETY: returning normally from fmgr_info_cxt above means it
            // initialized every field of FmgrInfo.
            let function = unsafe { function.assume_init() };
            Ok(Some((
                function,
                registration.extension_oid.to_u32(),
                registration.worker_name,
                database_name,
            )))
        }));
        let (mut function, extension_oid, worker_name, database_name) = match function
        {
            Ok(Some(function)) => function,
            Ok(None) => {
                crate::diag::warning(format_args!(
                    "Lakebase worker registration disappeared before start: database_oid={database_oid}, extension_oid={extension_oid}, worker_name={worker_name}, generation={}",
                    token.generation()
                ));
                return;
            }
            Err(error) => {
                crate::diag::warning(format_args!(
                    "failed to load Lakebase worker entry point: database_oid={database_oid}, extension_oid={extension_oid}, worker_name={worker_name}, generation={}, error={error}",
                    token.generation()
                ));
                return;
            }
        };

        crate::diag::info(format_args!(
            "starting Lakebase extension worker: database={database_name}, database_oid={database_oid}, extension_oid={extension_oid}, worker_name={worker_name}, generation={}",
            token.generation()
        ));

        let worker_context = WorkerContextRaw::new(
            database_oid,
            extension_oid,
            &worker_name,
            super::signals::BackgroundWorkerSignals::process_config_reload_callback,
        );
        if !store.validate_worker_token(token) {
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
        let code = unsafe { i64::from_datum(result, false) }.unwrap_or(0);
        let directive = WorkerDirective::decode(code).unwrap_or_else(|error| {
            crate::diag::warning(format_args!(
                "Lakebase worker returned an invalid exit directive: database_oid={database_oid}, extension_oid={extension_oid}, worker_name={worker_name}, generation={}, error={error}",
                token.generation()
            ));
            WorkerDirective::RunAfter(CRASH_BACKOFF)
        });
        store.finish_worker(token, directive);
        #[cfg(feature = "pg_test")]
        super::test_support::RuntimeTestInjection::after_directive(database_oid);
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn worker_exit_callback(code: i32, arg: pg_sys::Datum) {
    let token = WorkerToken::from_datum(arg);
    let store = RuntimeStore::new();
    if store.worker_exit(token, code) {
        store.signal_launcher();
    }
}
