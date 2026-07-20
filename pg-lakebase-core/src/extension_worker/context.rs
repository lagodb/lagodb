use pgrx::datum::Internal;

const WORKER_CONTEXT_MAGIC: u64 = 0x5047_4c42_5743_5458;
const WORKER_CONTEXT_ABI_VERSION: u32 = 1;
const MAX_WORKER_NAME_BYTES: usize = 255;

/// Borrowed invocation context passed to a registered Lakebase worker.
///
/// The context is valid only for the duration of the worker entry-point call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct WorkerContext {
    magic: u64,
    abi_version: u32,
    struct_size: u32,
    database_oid: u32,
    extension_oid: u32,
    worker_name_len: u16,
    _padding: [u8; 6],
    worker_name: [u8; MAX_WORKER_NAME_BYTES],
}

impl WorkerContext {
    #[doc(hidden)]
    pub fn new(database_oid: u32, extension_oid: u32, worker_name: &str) -> Self {
        let bytes = worker_name.as_bytes();
        assert!(bytes.len() <= MAX_WORKER_NAME_BYTES);
        let mut context = Self {
            magic: WORKER_CONTEXT_MAGIC,
            abi_version: WORKER_CONTEXT_ABI_VERSION,
            struct_size: std::mem::size_of::<Self>() as u32,
            database_oid,
            extension_oid,
            worker_name_len: bytes.len() as u16,
            _padding: [0; 6],
            worker_name: [0; MAX_WORKER_NAME_BYTES],
        };
        context.worker_name[..bytes.len()].copy_from_slice(bytes);
        context
    }

    /// Validate and borrow a context supplied through PostgreSQL's `internal` type.
    ///
    /// # Safety
    ///
    /// `internal` must come directly from the Lakebase worker runtime. The
    /// returned reference must not outlive the registered entry-point call.
    pub unsafe fn from_internal(
        internal: &Internal,
    ) -> Result<&Self, WorkerContextError> {
        // SAFETY: the caller guarantees that this `internal` datum was created
        // by the Lakebase runtime. The header is validated before fields are
        // exposed to the worker implementation.
        let context =
            unsafe { internal.get::<Self>() }.ok_or(WorkerContextError::Missing)?;
        if context.validate_abi() {
            Ok(context)
        } else {
            Err(WorkerContextError::AbiMismatch)
        }
    }

    pub const fn database_oid(&self) -> u32 {
        self.database_oid
    }

    pub const fn extension_oid(&self) -> u32 {
        self.extension_oid
    }

    pub fn worker_name(&self) -> &str {
        let len = usize::from(self.worker_name_len).min(MAX_WORKER_NAME_BYTES);
        std::str::from_utf8(&self.worker_name[..len]).unwrap_or("<invalid utf8>")
    }

    const fn validate_abi(&self) -> bool {
        self.magic == WORKER_CONTEXT_MAGIC
            && self.abi_version == WORKER_CONTEXT_ABI_VERSION
            && self.struct_size as usize == std::mem::size_of::<Self>()
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

    #[test]
    fn context_exposes_identity_and_rejects_header_mismatch() {
        let mut context = WorkerContext::new(42, 8, "worker");
        assert!(context.validate_abi());
        assert_eq!(context.database_oid(), 42);
        assert_eq!(context.extension_oid(), 8);
        assert_eq!(context.worker_name(), "worker");

        context.abi_version += 1;
        assert!(!context.validate_abi());
    }
}
