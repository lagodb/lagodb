use std::marker::PhantomData;
use std::rc::Rc;

use pgrx::datum::Internal;

const MAX_WORKER_NAME_BYTES: usize = 255;

/// Runtime-owned wire representation passed through PostgreSQL's `internal`
/// datum. Runtime and provider are built against the same core definition.
#[doc(hidden)]
#[derive(Debug)]
#[repr(C)]
pub struct WorkerContextRaw {
    database_oid: u32,
    extension_oid: u32,
    worker_id: i32,
    worker_name_len: u16,
    _padding: [u8; 2],
    process_config_reload: extern "C-unwind" fn() -> bool,
    deregister_self: extern "C-unwind" fn(i32),
    worker_name: [u8; MAX_WORKER_NAME_BYTES],
}

impl WorkerContextRaw {
    #[doc(hidden)]
    pub fn new(
        database_oid: u32,
        extension_oid: u32,
        worker_id: i32,
        worker_name: &str,
        process_config_reload: extern "C-unwind" fn() -> bool,
        deregister_self: extern "C-unwind" fn(i32),
    ) -> Self {
        let bytes = worker_name.as_bytes();
        assert!(bytes.len() <= MAX_WORKER_NAME_BYTES);
        let mut context = Self {
            database_oid,
            extension_oid,
            worker_id,
            worker_name_len: u16::try_from(bytes.len())
                .expect("validated worker name exceeds u16"),
            _padding: [0; 2],
            process_config_reload,
            deregister_self,
            worker_name: [0; MAX_WORKER_NAME_BYTES],
        };
        context.worker_name[..bytes.len()].copy_from_slice(bytes);
        context
    }
}

/// Borrowed capabilities for one LagoDB worker invocation.
///
/// The value is valid only for the duration of the worker entry-point call. It
/// is deliberately neither `Send` nor `Sync`: methods such as configuration
/// reload access PostgreSQL process-local state and must run on the background
/// worker's main thread.
#[derive(Debug)]
pub struct WorkerContext<'a> {
    raw: &'a WorkerContextRaw,
    _main_thread: PhantomData<Rc<()>>,
}

impl<'a> WorkerContext<'a> {
    /// Borrow a context supplied through PostgreSQL's `internal` type.
    ///
    /// # Safety
    ///
    /// The caller must be the LagoDB background-worker main thread.
    /// `internal` must come directly from the LagoDB worker runtime and point
    /// to a live [`WorkerContextRaw`] for the entire lifetime of `internal`.
    pub unsafe fn from_internal(internal: &'a Internal) -> Self {
        // SAFETY: the caller guarantees that this non-null datum was created by
        // the LagoDB runtime from a live WorkerContextRaw.
        let raw = unsafe { internal.get::<WorkerContextRaw>().unwrap_unchecked() };
        Self {
            raw,
            _main_thread: PhantomData,
        }
    }

    pub const fn database_oid(&self) -> u32 {
        self.raw.database_oid
    }

    /// OID of the extension that owns this worker registration.
    ///
    /// The registered entry-point function may belong to another extension.
    pub const fn extension_oid(&self) -> u32 {
        self.raw.extension_oid
    }

    pub const fn worker_id(&self) -> i32 {
        self.raw.worker_id
    }

    pub fn worker_name(&self) -> &str {
        let len = usize::from(self.raw.worker_name_len);
        // SAFETY: WorkerContextRaw::new copied this prefix from a Rust `&str`.
        unsafe { std::str::from_utf8_unchecked(&self.raw.worker_name[..len]) }
    }

    /// Process one pending SIGHUP through the runtime-owned signal state.
    ///
    /// Returns `true` when PostgreSQL configuration was reloaded. Call this
    /// only at a safe point where no transaction or extension lock is held.
    pub fn process_config_reload_if_pending(&self) -> bool {
        (self.raw.process_config_reload)()
    }

    /// Transactionally remove this worker registration after one successful run.
    ///
    /// Call this inside the worker transaction and then return normally. An abort
    /// restores the registration; a commit makes the invocation one-shot.
    pub fn deregister_self(&self) {
        (self.raw.deregister_self)(self.raw.worker_id);
    }
}
