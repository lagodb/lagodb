//! Owning guard for a relation opened by this crate.

use pgrx::{PgTryBuilder, pg_sys};

use super::RelationHandle;
use crate::diag::PgError;

/// RAII guard for an opened PostgreSQL relation.
///
/// This type only owns the open/close lifecycle. Catalog-specific operations
/// live in `crate::catalog::CatalogRelation`.
#[derive(Debug)]
pub struct RelationGuard {
    rel: pg_sys::Relation,
    lock_mode: pg_sys::LOCKMODE,
}

impl RelationGuard {
    /// Open any relation kind for scoped inspection, releasing this lock on drop.
    pub fn open_relation(
        oid: pg_sys::Oid,
        lock_mode: pg_sys::LOCKMODE,
    ) -> Result<Self, PgError> {
        Ok(Self {
            rel: Self::relation_open(oid, lock_mode)?,
            lock_mode,
        })
    }

    /// Open a relation of any kind, retaining the lock until transaction end.
    /// Unlike table_open, this also admits standalone composite relations,
    /// which PostgreSQL permits as CREATE TABLE LIKE sources. The caller owns
    /// admission of relation kinds for its operation.
    pub fn open_relation_retain_lock(
        oid: pg_sys::Oid,
        lock_mode: pg_sys::LOCKMODE,
    ) -> Result<Self, PgError> {
        Ok(Self {
            rel: Self::relation_open(oid, lock_mode)?,
            lock_mode: pg_sys::NoLock as _,
        })
    }

    /// Opens a table relation with the specified lock mode.
    ///
    /// PostgreSQL rejects indexes, partitioned indexes, and composite types at
    /// this boundary.
    pub fn open_table(
        oid: pg_sys::Oid,
        lock_mode: pg_sys::LOCKMODE,
    ) -> Result<Self, PgError> {
        let rel = Self::table_open(oid, lock_mode)?;
        Ok(Self { rel, lock_mode })
    }

    /// Opens a table relation while retaining its lock until transaction end.
    ///
    /// Closing the relcache handle with `NoLock` leaves PostgreSQL's lock
    /// manager to release the originally acquired lock at commit or abort.
    pub fn open_table_retain_lock(
        oid: pg_sys::Oid,
        lock_mode: pg_sys::LOCKMODE,
    ) -> Result<Self, PgError> {
        let rel = Self::table_open(oid, lock_mode)?;
        Ok(Self {
            rel,
            lock_mode: pg_sys::NoLock as _,
        })
    }

    fn relation_open(
        relation_id: pg_sys::Oid,
        lockmode: pg_sys::LOCKMODE,
    ) -> Result<pg_sys::Relation, PgError> {
        PgTryBuilder::new(move || {
            Ok(unsafe { pg_sys::relation_open(relation_id, lockmode) })
        })
        .catch_others(|err| Err(PgError::from_caught(err)))
        .execute()
    }

    fn table_open(
        relation_id: pg_sys::Oid,
        lockmode: pg_sys::LOCKMODE,
    ) -> Result<pg_sys::Relation, PgError> {
        unsafe {
            PgTryBuilder::new(move || Ok(pg_sys::table_open(relation_id, lockmode)))
                .catch_others(|err| Err(PgError::from_caught(err)))
                .execute()
        }
    }

    /// Get a `RelationHandle` from this guard.
    ///
    /// The returned handle borrows from this guard, ensuring the relation
    /// remains open while the handle is in use.
    #[inline]
    pub fn as_handle(&self) -> RelationHandle<'_> {
        unsafe { RelationHandle::from_raw(self.rel) }
    }

    /// Get the raw relation pointer.
    #[inline]
    pub fn as_raw(&self) -> pg_sys::Relation {
        self.rel
    }
}

impl Drop for RelationGuard {
    fn drop(&mut self) {
        // SAFETY: this guard opened `rel` with `lock_mode` and closes it once.
        unsafe { pg_sys::relation_close(self.rel, self.lock_mode) };
    }
}
