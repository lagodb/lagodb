//! PostgreSQL transaction isolation as it applies to Iceberg writes.
//!
//! PostgreSQL transaction isolation and Iceberg row-level write isolation are
//! separate contracts. The transaction level supplies a minimum requirement,
//! while the command-specific table property may require stronger validation.

use iceberg_lite::transaction::IsolationLevel;
use pgrx::pg_sys;

use crate::error::{IcebergError, IcebergResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PgTransactionIsolation {
    ReadCommitted,
    Serializable,
}

impl PgTransactionIsolation {
    pub(crate) fn current() -> IcebergResult<Self> {
        // SAFETY: XactIsoLevel is backend-local transaction state and all AM
        // and FDW callbacks execute on their owning PostgreSQL backend thread.
        Self::from_raw(unsafe { pg_sys::XactIsoLevel })
    }

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
                Ok(Self::ReadCommitted)
            }
            value if value == pg_sys::XACT_SERIALIZABLE as i32 => {
                // This strengthens Iceberg conflict validation; it does not
                // claim PostgreSQL SSI integration for the external system.
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
    fn postgres_and_table_policies_combine_monotonically() {
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
        assert_eq!(
            PgTransactionIsolation::Serializable
                .effective_iceberg(IsolationLevel::Snapshot),
            IsolationLevel::Serializable
        );
    }

    #[test]
    fn postgres_isolation_mapping_is_explicit() {
        assert_eq!(
            PgTransactionIsolation::from_raw(pg_sys::XACT_READ_UNCOMMITTED as i32)
                .unwrap(),
            PgTransactionIsolation::ReadCommitted
        );
        assert!(
            PgTransactionIsolation::from_raw(pg_sys::XACT_REPEATABLE_READ as i32)
                .is_err()
        );
        assert_eq!(
            PgTransactionIsolation::from_raw(pg_sys::XACT_SERIALIZABLE as i32)
                .unwrap(),
            PgTransactionIsolation::Serializable
        );
    }
}
