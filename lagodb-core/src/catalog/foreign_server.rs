use std::ffi::CStr;

use pgrx::{PgTryBuilder, pg_sys};

use crate::diag::{PgError, PgReportError};

/// Existing foreign-server target admitted for provider ALTER policy checks.
///
/// Resolution deliberately performs PostgreSQL's ownership check before a
/// provider inspects server-specific policy. A missing target remains the
/// native utility command's responsibility.
#[derive(Debug, Clone, Copy)]
pub struct ForeignServerDdlTarget {
    oid: pg_sys::Oid,
}

impl ForeignServerDdlTarget {
    pub fn resolve(name: &CStr) -> Result<Option<Self>, PgReportError> {
        let name = name.as_ptr();
        unsafe {
            PgTryBuilder::new(move || {
                let server_oid = pg_sys::get_foreign_server_oid(name, true);
                if server_oid == pg_sys::InvalidOid {
                    return Ok(None);
                }
                if !pg_sys::object_ownercheck(
                    pg_sys::ForeignServerRelationId,
                    server_oid,
                    pg_sys::GetUserId(),
                ) {
                    pg_sys::aclcheck_error(
                        pg_sys::AclResult::ACLCHECK_NOT_OWNER,
                        pg_sys::ObjectType::OBJECT_FOREIGN_SERVER,
                        name,
                    );
                }
                Ok(Some(Self { oid: server_oid }))
            })
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
        .map_err(PgReportError::from_pg_error)
    }

    #[inline]
    pub const fn oid(self) -> pg_sys::Oid {
        self.oid
    }
}
