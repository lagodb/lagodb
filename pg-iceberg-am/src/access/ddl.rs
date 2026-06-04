use crate::IcebergTableAm;
use pg_lakebase_core::diag::PgReportError;
use pg_lakebase_core::prelude::*;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

impl AmDdl for IcebergTableAm {
    fn relation_set_new_filelocator(
        _rel: &RelationHandle,
        _newrlocator: &RelFileLocator,
        _persistence: u8,
    ) -> AmResult<(pg_sys::TransactionId, pg_sys::MultiXactId)> {
        Ok((pg_sys::InvalidTransactionId, 0u32.into()))
    }

    fn relation_nontransactional_truncate(rel: &RelationHandle) -> AmResult<()> {
        Err(PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            format!(
                "cannot TRUNCATE Iceberg table \"{}\": Iceberg truncate requires metadata rewrite support",
                rel.relation_name()
            ),
        ))
    }
}
