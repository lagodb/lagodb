//! What `amgettuple` and `amgetbitmap` hand back to PostgreSQL.

use std::ptr;

use pgrx::{PgMemoryContexts, PgSqlErrorCode, pg_sys};

use super::context::IndexScanContext;
use crate::backend::MemoryContextHandle;
use crate::diag::PgReportError;
use crate::handles::{ScanDirection, ValidItemPointer};
use crate::index_am::IndexAmResult;

pub use pg_sys::IndexOrderByDistance;

#[derive(Debug, Clone, Copy)]
pub struct IndexMatch {
    pub tid: ValidItemPointer,
    pub recheck: bool,
}

pub struct IndexTupleOutput<'a, S> {
    scan: IndexScanContext<'a, S>,
    direction: ScanDirection,
    order_by_types: *mut pg_sys::Oid,
    result_context: MemoryContextHandle,
    values: &'a mut [pg_sys::Datum],
    nulls: &'a mut [bool],
    distances_set: bool,
}

impl<'a, S> IndexTupleOutput<'a, S> {
    pub(super) fn new(
        scan: IndexScanContext<'a, S>,
        direction: ScanDirection,
        order_by_types: *mut pg_sys::Oid,
        result_context: MemoryContextHandle,
        values: &'a mut [pg_sys::Datum],
        nulls: &'a mut [bool],
    ) -> Self {
        Self {
            scan,
            direction,
            order_by_types,
            result_context,
            values,
            nulls,
            distances_set: false,
        }
    }

    #[inline]
    pub fn scan(&self) -> &IndexScanContext<'a, S> {
        &self.scan
    }

    #[inline]
    pub fn direction(&self) -> ScanDirection {
        self.direction
    }

    #[inline]
    pub fn kill_prior_tuple(&self) -> bool {
        // SAFETY: live for the callback.
        unsafe { (*self.scan.as_raw()).kill_prior_tuple }
    }

    /// Return the row as a heap tuple over the returned tuple descriptor.
    ///
    /// `fill` runs in the result context, so datums it builds live until the
    /// next fetch or rescan. Columns it leaves unset are NULL.
    pub fn write_heap_tuple(
        &mut self,
        fill: impl FnOnce(&mut IndexReturnedValues<'_>) -> IndexAmResult<()>,
    ) -> IndexAmResult<()> {
        // SAFETY: the descriptor is live for the callback.
        let desc = unsafe { (*self.scan.as_raw()).xs_hitupdesc };
        // SAFETY: the arrays hold one value per column of `desc`.
        let tuple = self.form(fill, |values, nulls| unsafe {
            pg_sys::heap_form_tuple(desc, values, nulls)
        })?;
        // SAFETY: as above.
        let scan = unsafe { &mut *self.scan.as_raw() };
        scan.xs_hitup = tuple;
        scan.xs_itup = ptr::null_mut();
        Ok(())
    }

    /// Return the row as an index tuple; values must have the index's stored
    /// types. Otherwise as [`Self::write_heap_tuple`].
    pub fn write_index_tuple(
        &mut self,
        fill: impl FnOnce(&mut IndexReturnedValues<'_>) -> IndexAmResult<()>,
    ) -> IndexAmResult<()> {
        // SAFETY: the descriptor is live for the callback.
        let desc = unsafe { (*self.scan.as_raw()).xs_itupdesc };
        // SAFETY: the arrays hold one value per column of `desc`.
        let tuple = self.form(fill, |values, nulls| unsafe {
            pg_sys::index_form_tuple(desc, values, nulls)
        })?;
        // SAFETY: as above.
        let scan = unsafe { &mut *self.scan.as_raw() };
        scan.xs_itup = tuple;
        scan.xs_hitup = ptr::null_mut();
        Ok(())
    }

    /// On failure, reset the result context and unpublish any earlier tuple
    /// of this fetch, so the context is empty whenever no tuple is published.
    fn form<T>(
        &mut self,
        fill: impl FnOnce(&mut IndexReturnedValues<'_>) -> IndexAmResult<()>,
        form: impl FnOnce(*mut pg_sys::Datum, *mut bool) -> *mut T,
    ) -> IndexAmResult<*mut T> {
        self.nulls.fill(true);
        let mut returned = IndexReturnedValues {
            values: &mut *self.values,
            nulls: &mut *self.nulls,
        };
        // SAFETY: the result context is live, and what is allocated in it
        // stays there until the next release of the returned tuple.
        let formed = unsafe {
            PgMemoryContexts::For(self.result_context.as_ptr()).switch_to(|_| {
                fill(&mut returned)?;
                Ok(form(
                    returned.values.as_mut_ptr(),
                    returned.nulls.as_mut_ptr(),
                ))
            })
        };
        if formed.is_err() {
            // SAFETY: the result context is live, and `switch_to` restored
            // the previous context when it returned. The descriptor no
            // longer points into it once both pointers are cleared.
            unsafe {
                self.result_context.reset();
                let scan = &mut *self.scan.as_raw();
                scan.xs_hitup = ptr::null_mut();
                scan.xs_itup = ptr::null_mut();
            }
        }
        formed
    }

    /// Report the row's ORDER BY distances. With ORDER BY keys, every
    /// returned row must report them.
    pub fn set_order_by_distances(
        &mut self,
        distances: &[IndexOrderByDistance],
        recheck: bool,
    ) {
        let scan = self.scan.as_raw();
        // SAFETY: the descriptor is live; the ORDER BY count is fixed at
        // ambeginscan.
        let norderbys = unsafe { (*scan).numberOfOrderBys } as usize;
        assert_eq!(
            distances.len(),
            norderbys,
            "one distance per ORDER BY key is required"
        );
        // SAFETY: `order_by_types` and the descriptor's ORDER BY arrays hold
        // `norderbys` entries; PostgreSQL only reads `distances`.
        unsafe {
            pg_sys::index_store_float8_orderby_distances(
                scan,
                self.order_by_types,
                distances.as_ptr().cast_mut(),
                recheck,
            );
        }
        self.distances_set = true;
    }

    pub(super) fn publish(self, found: IndexMatch) -> IndexAmResult<()> {
        // SAFETY: the descriptor is live for the callback.
        let scan = unsafe { &mut *self.scan.as_raw() };
        if scan.numberOfOrderBys > 0 && !self.distances_set {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "index access method returned a row without its ORDER BY distances",
            ));
        }
        scan.xs_heaptid = found.tid.to_pg_sys();
        scan.xs_recheck = found.recheck;
        Ok(())
    }
}

pub struct IndexReturnedValues<'a> {
    values: &'a mut [pg_sys::Datum],
    nulls: &'a mut [bool],
}

impl IndexReturnedValues<'_> {
    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Set zero-based `column`; `None` is SQL NULL.
    #[inline]
    pub fn set(&mut self, column: usize, value: Option<pg_sys::Datum>) {
        self.nulls[column] = value.is_none();
        self.values[column] = value.unwrap_or(pg_sys::Datum::from(0));
    }
}

pub struct IndexBitmap<'a, S> {
    scan: IndexScanContext<'a, S>,
    bitmap: *mut pg_sys::TIDBitmap,
    added: i64,
}

impl<'a, S> IndexBitmap<'a, S> {
    pub(super) fn new(
        scan: IndexScanContext<'a, S>,
        bitmap: *mut pg_sys::TIDBitmap,
    ) -> Self {
        Self {
            scan,
            bitmap,
            added: 0,
        }
    }

    #[inline]
    pub fn scan(&self) -> &IndexScanContext<'a, S> {
        &self.scan
    }

    #[inline]
    pub fn add(&mut self, tid: ValidItemPointer, recheck: bool) {
        let mut tid = tid.to_pg_sys();
        // SAFETY: the bitmap is live for the call and copies the TID.
        unsafe { pg_sys::tbm_add_tuples(self.bitmap, &mut tid, 1, recheck) };
        self.added += 1;
    }

    /// Add a lossy page. It counts as one row in the result, as in GIN.
    #[inline]
    pub fn add_page(&mut self, block: pg_sys::BlockNumber) {
        // SAFETY: as above.
        unsafe { pg_sys::tbm_add_page(self.bitmap, block) };
        self.added += 1;
    }

    pub(super) fn added(&self) -> i64 {
        self.added
    }
}
