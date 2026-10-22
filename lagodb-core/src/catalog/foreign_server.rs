use std::ffi::CStr;

use pgrx::pg_sys;

use crate::diag::PgReportError;
use crate::wrapper::PgWrapper;

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
        PgWrapper::resolve_owned_foreign_server(name)
            .map(|target| target.map(|oid| Self { oid }))
            .map_err(PgReportError::from_pg_error)
    }

    #[inline]
    pub const fn oid(self) -> pg_sys::Oid {
        self.oid
    }
}
