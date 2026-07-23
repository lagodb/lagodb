//! External file-descriptor accounting supplied by the embedding runtime.

use crate::error::StorageResult;

/// Owns one reservation for an operating-system file descriptor.
///
/// Implementations normally release an embedding runtime's descriptor budget
/// from `Drop`.
pub trait ExternalFdLease: Send + Sync {}

/// Integrates storage-owned descriptors with an embedding runtime's FD budget.
///
/// The policy is invoked before opening a client socket or staging file and
/// before receiving a direct-I/O descriptor over `SCM_RIGHTS`.
pub trait ExternalFdPolicy {
    fn acquire(&self) -> StorageResult<Box<dyn ExternalFdLease>>;
}
