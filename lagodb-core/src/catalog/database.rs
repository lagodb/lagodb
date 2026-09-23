use std::ffi::CStr;

use pgrx::pg_sys;

use super::search_syscache1;
use crate::diag::PgError;
use crate::wrapper::PgWrapper;

/// Database catalog identity protected by a transaction-level object lock.
///
/// The lock deliberately remains held until transaction end. PostgreSQL's
/// database lifecycle commands use the same protocol so that a resolved name,
/// OID, and `pg_database` row cannot be invalidated before the command uses
/// them.
#[derive(Debug)]
pub struct LockedDatabase {
    oid: pg_sys::Oid,
    is_template: bool,
    tablespace_oid: pg_sys::Oid,
}

impl LockedDatabase {
    /// Resolve `name`, acquire `lockmode`, and revalidate the name-to-OID
    /// mapping using PostgreSQL's `get_db_info()` locking protocol.
    pub fn resolve(
        name: &CStr,
        lockmode: pg_sys::LOCKMODE,
    ) -> Result<Option<Self>, PgError> {
        loop {
            let candidate = unsafe { pg_sys::get_database_oid(name.as_ptr(), true) };
            if candidate == pg_sys::InvalidOid {
                return Ok(None);
            }

            PgWrapper::lock_database(candidate, lockmode)?;
            if unsafe { pg_sys::get_database_oid(name.as_ptr(), true) } != candidate {
                PgWrapper::unlock_database(candidate, lockmode);
                continue;
            }

            let Some(tuple) = search_syscache1(
                pg_sys::SysCacheIdentifier::DATABASEOID as i32,
                pg_sys::Datum::from(u32::from(candidate) as usize),
            ) else {
                PgWrapper::unlock_database(candidate, lockmode);
                continue;
            };
            // SAFETY: DATABASEOID returned a live pg_database tuple whose
            // fixed fields remain valid for the lifetime of `tuple`.
            let form = unsafe {
                &*(pg_sys::GETSTRUCT(tuple.as_raw()) as pg_sys::Form_pg_database)
            };
            return Ok(Some(Self {
                oid: candidate,
                is_template: form.datistemplate,
                tablespace_oid: form.dattablespace,
            }));
        }
    }

    #[inline]
    pub const fn oid(&self) -> pg_sys::Oid {
        self.oid
    }

    #[inline]
    pub const fn tablespace_oid(&self) -> pg_sys::Oid {
        self.tablespace_oid
    }

    /// Whether the current user may copy this database as a template.
    pub fn current_user_can_copy(&self) -> bool {
        self.is_template || self.is_owned_by_current_user()
    }

    /// Whether the current user owns this database.
    pub fn is_owned_by_current_user(&self) -> bool {
        unsafe {
            pg_sys::object_ownercheck(
                pg_sys::DatabaseRelationId,
                self.oid,
                pg_sys::GetUserId(),
            )
        }
    }

    /// Whether another backend or prepared transaction is using the database.
    ///
    /// Callers hold the database lock acquired by [`Self::resolve`], so no new
    /// backend can enter after a `false` result.
    pub fn has_other_backends(&self) -> bool {
        let mut other_backends = 0;
        let mut prepared_transactions = 0;
        unsafe {
            pg_sys::CountOtherDBBackends(
                self.oid,
                &mut other_backends,
                &mut prepared_transactions,
            )
        }
    }
}
