use std::marker::PhantomData;
use std::rc::Rc;

use pgrx::datum::Internal;

const WORKER_CONTEXT_MAGIC: u64 = 0x5047_4c42_5743_5458;
const WORKER_CONTEXT_ABI_VERSION: u32 = 3;
const MAX_WORKER_NAME_BYTES: usize = 255;

/// Runtime-owned wire representation passed through PostgreSQL's `internal`
/// datum. Extension callbacks must validate it into [`WorkerContext`] before
/// accessing any invocation capability.
#[doc(hidden)]
#[derive(Debug)]
#[repr(C)]
pub struct WorkerContextRaw {
    magic: u64,
    abi_version: u32,
    struct_size: u32,
    database_oid: u32,
    extension_oid: u32,
    worker_name_len: u16,
    _padding: [u8; 6],
    process_config_reload: extern "C-unwind" fn() -> bool,
    worker_name: [u8; MAX_WORKER_NAME_BYTES],
}

impl WorkerContextRaw {
    #[doc(hidden)]
    pub fn new(
        database_oid: u32,
        extension_oid: u32,
        worker_name: &str,
        process_config_reload: extern "C-unwind" fn() -> bool,
    ) -> Self {
        let bytes = worker_name.as_bytes();
        assert!(bytes.len() <= MAX_WORKER_NAME_BYTES);
        let mut context = Self {
            magic: WORKER_CONTEXT_MAGIC,
            abi_version: WORKER_CONTEXT_ABI_VERSION,
            struct_size: u32::try_from(std::mem::size_of::<Self>())
                .expect("worker context exceeds u32"),
            database_oid,
            extension_oid,
            worker_name_len: u16::try_from(bytes.len())
                .expect("validated worker name exceeds u16"),
            _padding: [0; 6],
            process_config_reload,
            worker_name: [0; MAX_WORKER_NAME_BYTES],
        };
        context.worker_name[..bytes.len()].copy_from_slice(bytes);
        context
    }

    fn validate_abi(&self) -> bool {
        self.magic == WORKER_CONTEXT_MAGIC
            && self.abi_version == WORKER_CONTEXT_ABI_VERSION
            && usize::try_from(self.struct_size).ok()
                == Some(std::mem::size_of::<Self>())
            && usize::from(self.worker_name_len) <= MAX_WORKER_NAME_BYTES
    }
}

/// Borrowed capabilities for one Lakebase worker invocation.
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
    /// Validate a context supplied through PostgreSQL's `internal` type.
    ///
    /// # Safety
    ///
    /// The caller must be the Lakebase background-worker main thread.
    /// `internal` must come directly from the Lakebase worker runtime and point
    /// to a live [`WorkerContextRaw`] for the entire lifetime of `internal`.
    pub unsafe fn from_internal(
        internal: &'a Internal,
    ) -> Result<Self, WorkerContextError> {
        // SAFETY: the caller guarantees that this datum was created by the
        // Lakebase runtime. No field is exposed until the ABI header and all
        // bounds used by safe accessors have been validated.
        let raw = unsafe { internal.get::<WorkerContextRaw>() }
            .ok_or(WorkerContextError::Missing)?;
        if !raw.validate_abi() {
            return Err(WorkerContextError::AbiMismatch);
        }
        Ok(Self {
            raw,
            _main_thread: PhantomData,
        })
    }

    pub const fn database_oid(&self) -> u32 {
        self.raw.database_oid
    }

    pub const fn extension_oid(&self) -> u32 {
        self.raw.extension_oid
    }

    pub fn worker_name(&self) -> &str {
        let len = usize::from(self.raw.worker_name_len);
        // Construction accepts only UTF-8 `&str`, and ABI validation rejects a
        // length outside the fixed buffer.
        std::str::from_utf8(&self.raw.worker_name[..len]).unwrap_or("<invalid utf8>")
    }

    /// Process one pending SIGHUP through the runtime-owned signal state.
    ///
    /// Returns `true` when PostgreSQL configuration was reloaded. Call this
    /// only at a safe point where no transaction or extension lock is held.
    pub fn process_config_reload_if_pending(&self) -> bool {
        (self.raw.process_config_reload)()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum WorkerContextError {
    #[error("Lakebase worker context is missing")]
    Missing,
    #[error("Lakebase worker context ABI mismatch")]
    AbiMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;

    extern "C-unwind" fn no_config_reload() -> bool {
        false
    }

    #[test]
    fn raw_context_validates_identity_and_bounds() {
        let mut context = WorkerContextRaw::new(42, 8, "worker", no_config_reload);
        assert!(context.validate_abi());
        assert_eq!(context.database_oid, 42);
        assert_eq!(context.extension_oid, 8);

        context.worker_name_len = u16::try_from(MAX_WORKER_NAME_BYTES + 1)
            .expect("test length exceeds u16");
        assert!(!context.validate_abi());
    }

    #[test]
    fn raw_context_rejects_header_mismatch() {
        let mut context = WorkerContextRaw::new(42, 8, "worker", no_config_reload);
        context.abi_version += 1;
        assert!(!context.validate_abi());
    }
}
