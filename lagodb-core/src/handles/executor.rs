//! Borrowed executor lifetime and visibility at scan binding boundaries.

use pgrx::{PgMemoryContexts, pg_sys};

use super::SnapshotHandle;
use super::borrowed::PgBorrowed;

/// A live executor. Provider state retained here is released on normal query
/// teardown and on PostgreSQL ERROR, independently of scan-end callbacks.
pub struct ExecutorHandle<'a> {
    inner: PgBorrowed<'a, pg_sys::EState>,
}

impl<'a> ExecutorHandle<'a> {
    /// # Safety
    ///
    /// `estate` must be an initialized executor that remains live for `'a`.
    pub unsafe fn from_raw(estate: *mut pg_sys::EState) -> Self {
        Self {
            // SAFETY: the caller supplies the live executor.
            inner: unsafe { PgBorrowed::from_raw(estate) },
        }
    }

    /// Identity for executor-local provider bindings; never a plan cache key.
    pub fn as_raw(&self) -> *mut pg_sys::EState {
        self.inner.as_ptr()
    }

    /// Borrow the snapshot already selected and registered by PostgreSQL.
    pub fn snapshot(&self) -> SnapshotHandle<'_> {
        // SAFETY: an initialized executor owns es_snapshot for its lifetime.
        unsafe { SnapshotHandle::from_raw(self.inner.as_ref().es_snapshot) }
    }

    /// The role whose privileges PostgreSQL uses for a result relation,
    /// as `ExecGetResultRelCheckAsUser()` resolves it from PostgreSQL 16 on.
    ///
    /// # Safety
    ///
    /// `rinfo` must be a result relation of this executor that has a
    /// range-table entry or a routing root, as every FDW modify or insert
    /// callback receives.
    pub unsafe fn result_relation_user(
        &self,
        rinfo: *mut pg_sys::ResultRelInfo,
    ) -> pg_sys::Oid {
        // SAFETY: guaranteed by the caller.
        unsafe {
            #[cfg(feature = "pg15")]
            let user = {
                // A routed partition and an inherited child share the root's
                // `checkAsUser`.
                let root = (*rinfo).ri_RootResultRelInfo;
                let rti = if root.is_null() {
                    (*rinfo).ri_RangeTableIndex
                } else {
                    (*root).ri_RangeTableIndex
                };
                self.range_table_user(rti)
            };
            #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
            let user = pg_sys::ExecGetResultRelCheckAsUser(rinfo, self.as_raw());
            user
        }
    }

    /// Range-table entry `rti`'s `checkAsUser`, or the current user. PostgreSQL
    /// 16 moved the field into `RTEPermissionInfo` and copies it into plans.
    ///
    /// # Safety
    ///
    /// `rti` must index this executor's range table.
    #[cfg(feature = "pg15")]
    pub(crate) unsafe fn range_table_user(&self, rti: pg_sys::Index) -> pg_sys::Oid {
        // SAFETY: this executor owns a live EState and the caller supplies an
        // index into its range table, as `exec_rt_fetch()` requires.
        unsafe {
            let rte = pg_sys::exec_rt_fetch(rti, self.as_raw());
            if (*rte).checkAsUser != pg_sys::InvalidOid {
                (*rte).checkAsUser
            } else {
                pg_sys::GetUserId()
            }
        }
    }

    /// Retain an owning Rust value until this executor's context is deleted.
    /// The value must not borrow callback-local PostgreSQL data.
    pub fn retain<T: 'static>(&self, value: T) {
        // SAFETY: es_query_cxt is owned by this live executor.
        let context = unsafe { self.inner.as_ref().es_query_cxt };
        PgMemoryContexts::For(context).leak_and_drop_on_delete(value);
    }
}
