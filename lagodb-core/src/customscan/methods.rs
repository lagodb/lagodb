//! Process-lifetime PostgreSQL CustomScan method-table construction.
//!
//! Relation scans and query-subtree scans keep independent typed states and
//! callback implementations. This object owns only the identical PostgreSQL
//! table layout, immutable lifetime contract, and default unsupported callbacks.

use core::ffi::c_int;
use std::ffi::CStr;
use std::ptr;

use pgrx::pg_sys;

pub type PlanCustomPath = unsafe extern "C-unwind" fn(
    *mut pg_sys::PlannerInfo,
    *mut pg_sys::RelOptInfo,
    *mut pg_sys::CustomPath,
    *mut pg_sys::List,
    *mut pg_sys::List,
    *mut pg_sys::List,
) -> *mut pg_sys::Plan;

pub type ReparameterizeCustomPath = unsafe extern "C-unwind" fn(
    *mut pg_sys::PlannerInfo,
    *mut pg_sys::List,
    *mut pg_sys::RelOptInfo,
) -> *mut pg_sys::List;

pub type CreateCustomScanState =
    unsafe extern "C-unwind" fn(*mut pg_sys::CustomScan) -> *mut pg_sys::Node;
pub type BeginCustomScan = unsafe extern "C-unwind" fn(
    *mut pg_sys::CustomScanState,
    *mut pg_sys::EState,
    c_int,
);
pub type ExecCustomScan = unsafe extern "C-unwind" fn(
    *mut pg_sys::CustomScanState,
) -> *mut pg_sys::TupleTableSlot;
pub type EndCustomScan = unsafe extern "C-unwind" fn(*mut pg_sys::CustomScanState);
pub type ReScanCustomScan = unsafe extern "C-unwind" fn(*mut pg_sys::CustomScanState);
pub type ExplainCustomScan = unsafe extern "C-unwind" fn(
    *mut pg_sys::CustomScanState,
    *mut pg_sys::List,
    *mut pg_sys::ExplainState,
);
pub type EstimateDSMCustomScan = unsafe extern "C-unwind" fn(
    *mut pg_sys::CustomScanState,
    *mut pg_sys::ParallelContext,
) -> pg_sys::Size;
pub type InitializeDSMCustomScan = unsafe extern "C-unwind" fn(
    *mut pg_sys::CustomScanState,
    *mut pg_sys::ParallelContext,
    *mut core::ffi::c_void,
);
pub type ReInitializeDSMCustomScan = InitializeDSMCustomScan;
pub type InitializeWorkerCustomScan = unsafe extern "C-unwind" fn(
    *mut pg_sys::CustomScanState,
    *mut pg_sys::shm_toc,
    *mut core::ffi::c_void,
);
pub type ShutdownCustomScan =
    unsafe extern "C-unwind" fn(*mut pg_sys::CustomScanState);

/// Callback family implemented by one serial CustomScan consumer.
pub struct SerialCustomScanCallbacks {
    pub plan: PlanCustomPath,
    pub reparameterize: Option<ReparameterizeCustomPath>,
    pub create_state: CreateCustomScanState,
    pub begin: BeginCustomScan,
    pub execute: ExecCustomScan,
    pub end: EndCustomScan,
    pub rescan: ReScanCustomScan,
    pub explain: ExplainCustomScan,
}

pub struct ParallelCustomScanCallbacks {
    pub estimate_dsm: EstimateDSMCustomScan,
    pub initialize_dsm: InitializeDSMCustomScan,
    pub reinitialize_dsm: ReInitializeDSMCustomScan,
    pub initialize_worker: InitializeWorkerCustomScan,
    pub shutdown: ShutdownCustomScan,
}

/// The three immutable PostgreSQL callback tables for one CustomScan consumer.
pub struct CustomScanMethodTables {
    path: pg_sys::CustomPathMethods,
    scan: pg_sys::CustomScanMethods,
    exec: pg_sys::CustomExecMethods,
    first_exec: Option<pg_sys::CustomExecMethods>,
}

// SAFETY: `CustomName` points to process-static bytes supplied by the caller;
// all other non-null fields are immutable function pointers.
unsafe impl Send for CustomScanMethodTables {}
// SAFETY: construction initializes every field and callers publish the object
// only through process-lifetime immutable storage.
unsafe impl Sync for CustomScanMethodTables {}

impl CustomScanMethodTables {
    /// Construct one complete method-table set for a serial CustomScan.
    pub fn serial(name: &'static CStr, callbacks: SerialCustomScanCallbacks) -> Self {
        Self::new(name, callbacks, None)
    }

    pub fn parallel(
        name: &'static CStr,
        callbacks: SerialCustomScanCallbacks,
        parallel: ParallelCustomScanCallbacks,
    ) -> Self {
        Self::new(name, callbacks, Some(parallel))
    }

    fn new(
        name: &'static CStr,
        callbacks: SerialCustomScanCallbacks,
        parallel: Option<ParallelCustomScanCallbacks>,
    ) -> Self {
        let (
            estimate_dsm,
            initialize_dsm,
            reinitialize_dsm,
            initialize_worker,
            shutdown,
        ) = parallel.map_or((None, None, None, None, None), |callbacks| {
            (
                Some(callbacks.estimate_dsm),
                Some(callbacks.initialize_dsm),
                Some(callbacks.reinitialize_dsm),
                Some(callbacks.initialize_worker),
                Some(callbacks.shutdown),
            )
        });
        Self {
            first_exec: None,
            path: pg_sys::CustomPathMethods {
                CustomName: name.as_ptr(),
                PlanCustomPath: Some(callbacks.plan),
                ReparameterizeCustomPathByChild: callbacks.reparameterize,
            },
            scan: pg_sys::CustomScanMethods {
                CustomName: name.as_ptr(),
                CreateCustomScanState: Some(callbacks.create_state),
            },
            exec: pg_sys::CustomExecMethods {
                CustomName: name.as_ptr(),
                BeginCustomScan: Some(callbacks.begin),
                ExecCustomScan: Some(callbacks.execute),
                EndCustomScan: Some(callbacks.end),
                ReScanCustomScan: Some(callbacks.rescan),
                MarkPosCustomScan: None,
                RestrPosCustomScan: None,
                EstimateDSMCustomScan: estimate_dsm,
                InitializeDSMCustomScan: initialize_dsm,
                ReInitializeDSMCustomScan: reinitialize_dsm,
                InitializeWorkerCustomScan: initialize_worker,
                ShutdownCustomScan: shutdown,
                ExplainCustomScan: Some(callbacks.explain),
            },
        }
    }

    #[inline]
    pub fn path(&'static self) -> &'static pg_sys::CustomPathMethods {
        &self.path
    }

    #[inline]
    pub fn scan(&'static self) -> &'static pg_sys::CustomScanMethods {
        &self.scan
    }

    #[inline]
    pub fn exec(&'static self) -> &'static pg_sys::CustomExecMethods {
        &self.exec
    }

    /// Install a one-time execution entry point without a per-row start check.
    pub(crate) fn with_first_exec(mut self, execute: ExecCustomScan) -> Self {
        self.first_exec = Some(pg_sys::CustomExecMethods {
            ExecCustomScan: Some(execute),
            ..self.exec
        });
        self
    }

    pub(crate) fn initial_exec(&'static self) -> &'static pg_sys::CustomExecMethods {
        self.first_exec.as_ref().unwrap_or(&self.exec)
    }

    pub(crate) fn owns_exec(
        &'static self,
        methods: *const pg_sys::CustomExecMethods,
    ) -> bool {
        ptr::eq(methods, self.exec()) || ptr::eq(methods, self.initial_exec())
    }
}
