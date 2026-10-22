use std::ffi::CStr;

use pgrx::{PgTryBuilder, pg_sys};

use super::PgWrapper;
use crate::diag::PgError;

impl PgWrapper {
    pub(crate) fn resolve_owned_foreign_server(
        name: &CStr,
    ) -> Result<Option<pg_sys::Oid>, PgError> {
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
                Ok(Some(server_oid))
            })
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
    }
}
