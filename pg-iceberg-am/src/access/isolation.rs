//! PostgreSQL transaction isolation as it applies to Iceberg access.
//!
//! PostgreSQL transaction isolation and Iceberg row-level DML isolation are
//! separate contracts. The transaction level supplies a minimum requirement
//! for the current PostgreSQL transaction, while the command-specific Iceberg
//! table property may request stronger conflict validation. Resolution is
//! therefore monotonic: neither source can weaken the other.

use iceberg_lite::transaction::IsolationLevel;
use pgrx::pg_sys;

use crate::error::{IcebergError, IcebergResult};

/// PostgreSQL transaction modes currently supported by the Iceberg AM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PgTransactionIsolation {
    ReadCommitted,
    Serializable,
}

impl PgTransactionIsolation {
    /// Read and validate PostgreSQL's effective isolation level for the current
    /// transaction.
    pub(crate) fn current() -> IcebergResult<Self> {
        // SAFETY: `XactIsoLevel` is backend-local PostgreSQL transaction state.
        // Table-AM callbacks execute on the owning backend thread.
        Self::from_raw(unsafe { pg_sys::XactIsoLevel })
    }

    /// Resolve the Iceberg validation level without allowing either the
    /// transaction requirement or table policy to weaken the other.
    pub(crate) fn effective_iceberg(
        self,
        table_isolation: IsolationLevel,
    ) -> IsolationLevel {
        match (self, table_isolation) {
            (Self::ReadCommitted, IsolationLevel::Snapshot) => {
                IsolationLevel::Snapshot
            }
            _ => IsolationLevel::Serializable,
        }
    }

    fn from_raw(value: i32) -> IcebergResult<Self> {
        match value {
            value
                if value == pg_sys::XACT_READ_UNCOMMITTED as i32
                    || value == pg_sys::XACT_READ_COMMITTED as i32 =>
            {
                // PostgreSQL implements READ UNCOMMITTED as READ COMMITTED.
                Ok(Self::ReadCommitted)
            }
            value if value == pg_sys::XACT_SERIALIZABLE as i32 => {
                // TODO(pg-serializable-ssi): this currently strengthens
                // Iceberg row-level DML conflict validation only. Integrate
                // PredicateLock*/CheckForSerializableConflict* before claiming
                // full PostgreSQL SSI semantics.
                Ok(Self::Serializable)
            }
            value if value == pg_sys::XACT_REPEATABLE_READ as i32 => {
                Err(IcebergError::NotImplemented(
                    "PostgreSQL REPEATABLE READ isolation for Iceberg tables",
                ))
            }
            _ => Err(IcebergError::InvariantViolated(
                "PostgreSQL reported an unknown transaction isolation level",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_committed_preserves_table_policy() {
        assert_eq!(
            PgTransactionIsolation::ReadCommitted
                .effective_iceberg(IsolationLevel::Snapshot),
            IsolationLevel::Snapshot
        );
        assert_eq!(
            PgTransactionIsolation::ReadCommitted
                .effective_iceberg(IsolationLevel::Serializable),
            IsolationLevel::Serializable
        );
    }

    #[test]
    fn serializable_strengthens_snapshot_table_policy() {
        assert_eq!(
            PgTransactionIsolation::Serializable
                .effective_iceberg(IsolationLevel::Snapshot),
            IsolationLevel::Serializable
        );
        assert_eq!(
            PgTransactionIsolation::Serializable
                .effective_iceberg(IsolationLevel::Serializable),
            IsolationLevel::Serializable
        );
    }

    #[test]
    fn postgres_read_uncommitted_normalizes_to_read_committed() {
        assert_eq!(
            PgTransactionIsolation::from_raw(
                pg_sys::XACT_READ_UNCOMMITTED as i32
            )
            .unwrap(),
            PgTransactionIsolation::ReadCommitted
        );
    }

    #[test]
    fn postgres_repeatable_read_is_rejected() {
        assert!(
            PgTransactionIsolation::from_raw(
                pg_sys::XACT_REPEATABLE_READ as i32
            )
            .is_err()
        );
    }

    #[test]
    fn postgres_serializable_is_supported_as_iceberg_validation() {
        assert_eq!(
            PgTransactionIsolation::from_raw(pg_sys::XACT_SERIALIZABLE as i32)
                .unwrap(),
            PgTransactionIsolation::Serializable
        );
    }
}
