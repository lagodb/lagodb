use pgrx::pg_sys;

// SQL advisory locks use classes 1 and 2; pg_extension_base uses 20 and 21.
// A private class keeps this internal lifecycle protocol out of both spaces.
const LAGODB_DATABASE_LOCK_CLASS: u16 = 0x4c42;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DatabaseLifecycleLock {
    database_oid: u32,
}

impl DatabaseLifecycleLock {
    pub(crate) const fn new(database_oid: u32) -> Self {
        Self { database_oid }
    }

    /// Coordinate a registration transaction or supervisor handoff.
    pub(crate) fn acquire_shared(self) {
        let acquired = self.acquire(pg_sys::ShareLock as _, true);
        debug_assert!(acquired);
    }

    /// Avoid launching a coordinator while DROP owns the lifecycle.
    pub(crate) fn try_acquire_shared(self) -> bool {
        self.acquire(pg_sys::ShareLock as _, false)
    }

    /// Wait for registration transactions before reading their committed catalog state.
    pub(crate) fn acquire_reconciliation(self) {
        let acquired = self.acquire(pg_sys::RowExclusiveLock as _, true);
        debug_assert!(acquired);
    }

    /// Exclude supervisor/coordinator activity for DROP EXTENSION or DROP DATABASE.
    pub(crate) fn acquire_drop(self) {
        let acquired = self.acquire(pg_sys::ExclusiveLock as _, true);
        debug_assert!(acquired);
    }

    fn acquire(self, mode: pg_sys::LOCKMODE, wait: bool) -> bool {
        let tag = self.tag();
        // SAFETY: tag is a fully initialized advisory LOCKTAG. sessionLock=false
        // binds it to the current PostgreSQL transaction, and LockAcquire either
        // returns a documented result or raises a PostgreSQL ERROR.
        let result = unsafe { pg_sys::LockAcquire(&tag, mode, false, !wait) };
        result != pg_sys::LockAcquireResult::LOCKACQUIRE_NOT_AVAIL
    }

    const fn tag(self) -> pg_sys::LOCKTAG {
        pg_sys::LOCKTAG {
            locktag_field1: self.database_oid,
            locktag_field2: self.database_oid,
            locktag_field3: 0,
            locktag_field4: LAGODB_DATABASE_LOCK_CLASS,
            locktag_type: pg_sys::LockTagType::LOCKTAG_ADVISORY as u8,
            locktag_lockmethodid: pg_sys::USER_LOCKMETHOD as u8,
        }
    }
}
