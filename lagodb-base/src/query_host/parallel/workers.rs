//! PostgreSQL parallel-context owner and the DSM activation gate.
//!
//! `WaitForParallelWorkersToAttach` is the sole worker-startup barrier. The
//! extension adds only a durable RUN/ABORT predicate plus a condition variable
//! so workers cannot observe the transport region before the leader publishes
//! it, without duplicating PostgreSQL's worker lifecycle with a second
//! entrypoint-ready protocol.

use std::ffi::c_void;
use std::mem::{self, size_of};
use std::ptr::{self, NonNull};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};

use lagodb_core::diag::PgReportError;
use lagodb_query::datafusion::ParallelWorkers;
use pgrx::{pg_guard, pg_sys};

use super::PgParallelHost;

const CONTROL_KEY: u64 = 1;
const REGION_KEY: u64 = 2;
const WAIT: u32 = 0;
const RUN: u32 = 1;
const ABORT: u32 = 2;

#[repr(C)]
struct SharedControl {
    activation: AtomicU32,
    activation_cv: pg_sys::ConditionVariable,
    region_bytes: AtomicUsize,
}

pub(super) struct WorkerMapping {
    segment: *mut pg_sys::dsm_segment,
    control: NonNull<SharedControl>,
    pub(super) region: NonNull<c_void>,
}

impl WorkerMapping {
    fn estimate(estimator: &mut pg_sys::shm_toc_estimator, region_bytes: usize) {
        let chunks = [size_of::<SharedControl>(), region_bytes];
        let alignment = pg_sys::ALIGNOF_BUFFER as usize;

        for &bytes in &chunks {
            // This is shm_toc_estimate_chunk: PostgreSQL's DSM allocator rounds
            // every chunk to BUFFERALIGN, whose target-specific value is exported
            // by pgrx as ALIGNOF_BUFFER.
            let aligned =
                unsafe { pg_sys::add_size(bytes, alignment - 1) } & !(alignment - 1);
            estimator.space_for_chunks =
                unsafe { pg_sys::add_size(estimator.space_for_chunks, aligned) };
        }
        estimator.number_of_keys =
            unsafe { pg_sys::add_size(estimator.number_of_keys, chunks.len()) };
    }

    /// # Safety
    /// `segment` and `toc` must belong to an initialized leader parallel DSM
    /// whose estimator was populated by `Self::estimate`. This must be called
    /// exactly once, before any worker is activated.
    unsafe fn allocate(
        segment: *mut pg_sys::dsm_segment,
        toc: *mut pg_sys::shm_toc,
        region_bytes: usize,
    ) -> Self {
        let control = unsafe {
            pg_sys::shm_toc_allocate(toc, size_of::<SharedControl>())
                .cast::<SharedControl>()
        };
        unsafe {
            ptr::write(
                control,
                SharedControl {
                    activation: AtomicU32::new(WAIT),
                    activation_cv: pg_sys::ConditionVariable::default(),
                    region_bytes: AtomicUsize::new(0),
                },
            );
            pg_sys::ConditionVariableInit(&raw mut (*control).activation_cv);
        }
        let region = unsafe { pg_sys::shm_toc_allocate(toc, region_bytes) };
        unsafe {
            pg_sys::shm_toc_insert(toc, CONTROL_KEY, control.cast());
            pg_sys::shm_toc_insert(toc, REGION_KEY, region);
        }

        Self {
            segment,
            // SAFETY: shm_toc_allocate returns a valid chunk or raises PG ERROR.
            control: unsafe { NonNull::new_unchecked(control) },
            // SAFETY: shm_toc_allocate returns a valid chunk or raises PG ERROR.
            region: unsafe { NonNull::new_unchecked(region) },
        }
    }

    /// # Safety
    /// The TOC must be the initialized LagoDB query-worker TOC passed by PG.
    pub(super) unsafe fn attach(
        segment: *mut pg_sys::dsm_segment,
        toc: *mut pg_sys::shm_toc,
    ) -> Self {
        Self {
            segment,
            // SAFETY: PG lookup errors if a required key is absent.
            control: unsafe {
                NonNull::new_unchecked(
                    pg_sys::shm_toc_lookup(toc, CONTROL_KEY, false).cast(),
                )
            },
            region: unsafe {
                NonNull::new_unchecked(pg_sys::shm_toc_lookup(toc, REGION_KEY, false))
            },
        }
    }

    pub(super) fn region_bytes(&self) -> usize {
        // SAFETY: this participant owns an attached DSM mapping.
        unsafe { self.control.as_ref() }
            .region_bytes
            .load(Ordering::Acquire)
    }

    pub(super) fn wait_for_activation(&self) -> Result<bool, PgReportError> {
        let control = self.control.as_ptr();
        // This gate precedes construction of the worker's Tokio runtime. Use
        // the same DSM condition-variable predicate loop as PG parallel scan.
        // RUN/ABORT is persistent, so a worker that reaches this code after the
        // leader's broadcast still observes the state and cannot lose a wakeup.
        unsafe {
            pg_sys::ConditionVariablePrepareToSleep(
                &raw mut (*control).activation_cv,
            );
        }
        let result = loop {
            // SAFETY: the worker retains its mapping until the entrypoint returns.
            match unsafe { (*control).activation.load(Ordering::Acquire) } {
                RUN => break Ok(true),
                ABORT => break Ok(false),
                _ => match PgParallelHost::capture(|| unsafe {
                    pg_sys::ConditionVariableSleep(
                        &raw mut (*control).activation_cv,
                        pg_sys::WaitEventIPC::WAIT_EVENT_PARALLEL_FINISH,
                    );
                }) {
                    Ok(()) => {}
                    Err(error) => break Err(error),
                },
            }
        };
        unsafe {
            pg_sys::ConditionVariableCancelSleep();
        }
        result
    }

    fn abort_activation(&self) {
        // Only a worker still behind the activation gate observes ABORT here.
        let control = self.control.as_ptr();
        if unsafe { &(*control).activation }
            .compare_exchange(WAIT, ABORT, Ordering::Release, Ordering::Relaxed)
            .is_ok()
        {
            unsafe {
                pg_sys::ConditionVariableBroadcast(&raw mut (*control).activation_cv);
            }
        }
    }

    pub(super) fn watch_mapping(
        &self,
        alive: Arc<AtomicBool>,
    ) -> Result<(), PgReportError> {
        let raw = Arc::into_raw(alive);
        let result = PgParallelHost::capture(|| unsafe {
            pg_sys::on_dsm_detach(
                self.segment,
                Some(mapping_detached),
                pg_sys::Datum::from(raw as usize),
            );
        });
        if result.is_err() {
            // SAFETY: registration failed, so no callback owns this Arc share.
            drop(unsafe { Arc::from_raw(raw) });
        }
        result
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn mapping_detached(
    _segment: *mut pg_sys::dsm_segment,
    data: pg_sys::Datum,
) {
    // SAFETY: watch_mapping transfers exactly one Arc share to this callback.
    let alive = unsafe { Arc::from_raw(data.value() as *const AtomicBool) };
    alive.store(false, Ordering::Release);
}

pub(super) struct PgParallelWorkers {
    context: Option<NonNull<pg_sys::ParallelContext>>,
    mapping: Option<WorkerMapping>,
    context_alive: Arc<AtomicBool>,
    attached: u32,
    entered: bool,
}

impl PgParallelWorkers {
    fn abandon_to_postgres(&mut self) {
        if self.context_alive.load(Ordering::Acquire)
            && let Some(mapping) = &self.mapping
        {
            mapping.abort_activation();
        }
        self.context = None;
        self.mapping = None;
        self.entered = false;
    }

    pub(super) fn launch(
        workers: u32,
        region_bytes: usize,
    ) -> Result<Option<Self>, PgReportError> {
        let mut owner = Self {
            context: None,
            mapping: None,
            attached: 0,
            entered: false,
            context_alive: Arc::new(AtomicBool::new(true)),
        };
        PgParallelHost::capture(|| unsafe {
            pg_sys::EnterParallelMode();
            owner.entered = true;
            let context = pg_sys::CreateParallelContext(
                c"lagodb_base".as_ptr(),
                c"lagodb_query_worker".as_ptr(),
                workers as i32,
            );
            owner.context = Some(NonNull::new_unchecked(context));
            WorkerMapping::estimate(&mut (*context).estimator, region_bytes);
            pg_sys::InitializeParallelDSM(context);
            if (*context).seg.is_null() {
                return;
            }
            owner.mapping = Some(WorkerMapping::allocate(
                (*context).seg,
                (*context).toc,
                region_bytes,
            ));
        })?;
        if let Some(mapping) = &owner.mapping {
            mapping.watch_mapping(Arc::clone(&owner.context_alive))?;
        } else {
            owner.destroy()?;
            return Ok(None);
        }
        let launch = PgParallelHost::capture(|| unsafe {
            let context = owner.context.expect("created parallel context").as_ptr();
            pg_sys::LaunchParallelWorkers(context);
            pg_sys::WaitForParallelWorkersToAttach(context);
            owner.attached = (*context).nworkers_launched as u32;
        });
        if let Err(error) = launch {
            owner.abandon_to_postgres();
            return Err(error);
        }
        // Do not add a second "entered extension" barrier here. PostgreSQL
        // owns bootstrap failures through the ParallelContext error queues and
        // finish protocol. Once fragment execution starts, PostgreSQL/pgrx
        // errors take the separate query-layer path: the fragment request
        // boundary converts the typed payload to DataFusionError, and
        // run_execute_task_loop sends DSD TaskError to the requester before the
        // worker entrypoint reports the PostgreSQL error. An entry counter would
        // duplicate PG lifecycle without strengthening that task-error protocol.
        if owner.attached < 2 {
            owner.mapping().abort_activation();
            owner.finish()?;
            return Ok(None);
        }
        Ok(Some(owner))
    }

    fn mapping(&self) -> &WorkerMapping {
        self.mapping
            .as_ref()
            .expect("attached workers own a DSM mapping")
    }

    fn destroy(&mut self) -> Result<(), PgReportError> {
        if !self.context_alive.load(Ordering::Acquire) {
            // PG's transaction cleanup already destroyed the context and reset
            // parallel mode, before AFTER_LOCKS ResourceOwner callbacks run.
            self.context = None;
            self.entered = false;
            return Ok(());
        }
        let context = self.context.take();
        let result = PgParallelHost::capture(|| unsafe {
            if let Some(context) = context {
                pg_sys::DestroyParallelContext(context.as_ptr());
            }
        });
        if mem::take(&mut self.entered) {
            let exit =
                PgParallelHost::capture(|| unsafe { pg_sys::ExitParallelMode() });
            return result.and(exit);
        }
        result
    }
}

impl ParallelWorkers for PgParallelWorkers {
    fn region(&self) -> NonNull<c_void> {
        self.mapping().region
    }
    fn attached_workers(&self) -> u32 {
        self.attached
    }
    fn watch_mapping(&self, alive: Arc<AtomicBool>) -> Result<(), PgReportError> {
        self.mapping().watch_mapping(alive)
    }
    fn activate(&mut self, region_bytes: usize) {
        // Release of RUN publishes the initialized mesh and actual mapped length.
        let control = self.mapping().control.as_ptr();
        unsafe { &(*control).region_bytes }.store(region_bytes, Ordering::Relaxed);
        unsafe { &(*control).activation }.store(RUN, Ordering::Release);
        unsafe {
            pg_sys::ConditionVariableBroadcast(&raw mut (*control).activation_cv);
        }
    }
    fn finish(&mut self) -> Result<(), PgReportError> {
        let wait = PgParallelHost::capture(|| unsafe {
            if let Some(context) = self.context {
                pg_sys::WaitForParallelWorkersToFinish(context.as_ptr());
            }
        });
        if let Err(error) = wait {
            self.abandon_to_postgres();
            return Err(error);
        }
        self.destroy()
    }

    fn abandon(mut self: Box<Self>) {
        // ParallelContext is linked into PostgreSQL's backend-local context
        // list. AtEOXact_Parallel(false) owns forced worker termination, DSM
        // detach and the uninterruptible exit wait after ERROR. Dropping the
        // Rust wrapper here must therefore forget, not destroy, that context.
        self.abandon_to_postgres();
    }
}

impl Drop for PgParallelWorkers {
    fn drop(&mut self) {
        if self.context.is_some() {
            self.abandon_to_postgres();
        }
    }
}
