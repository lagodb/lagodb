//! Framework state behind `IndexScanDesc::opaque`.

use std::ptr::{self, NonNull};

use pgrx::pg_sys;

use super::context::IndexScanContext;
use super::output::IndexTupleOutput;
use crate::backend::MemoryContextHandle;
use crate::handles::ScanDirection;
use crate::index_am::IndexAmResult;

const INDEX_MAX_KEYS: usize = pg_sys::INDEX_MAX_KEYS as usize;

/// Framework state hung off `scan->opaque`, allocated in the scan's
/// lifecycle context.
///
/// Unlike the other session owners, this one is published before its
/// session exists: PostgreSQL sets the heap relation, snapshot and
/// `xs_want_itup` only after `ambeginscan` returns, so the session starts at
/// the first `amrescan`.
pub(super) struct IndexScanOpaque<S: 'static> {
    context: MemoryContextHandle,
    session: Option<NonNull<S>>,
    /// Holds only the returned index-only tuple, so it is empty whenever
    /// `xs_hitup` and `xs_itup` are NULL.
    result_context: MemoryContextHandle,
    /// NULL without ORDER BY.
    order_by_types: *mut pg_sys::Oid,
    returned_values: [pg_sys::Datum; INDEX_MAX_KEYS],
    returned_nulls: [bool; INDEX_MAX_KEYS],
}

impl<S: 'static> IndexScanOpaque<S> {
    /// # Safety
    ///
    /// `scan` must come from `RelationGetIndexScan` in this `ambeginscan`.
    pub(super) unsafe fn publish(scan: pg_sys::IndexScanDesc) {
        unsafe {
            let context = MemoryContextHandle::create(
                pg_sys::CurrentMemoryContext,
                c"lagodb index scan",
            );
            let result_context = context.create_child(c"lagodb index scan result");
            let norderbys = (*scan).numberOfOrderBys as usize;
            let order_by_types = if norderbys > 0 {
                (*scan).xs_orderbyvals = context.alloc_zeroed_array(norderbys);
                let nulls = context.alloc_zeroed_array::<bool>(norderbys);
                ptr::write_bytes(nulls, 1, norderbys);
                (*scan).xs_orderbynulls = nulls;
                context.alloc_zeroed_array(norderbys)
            } else {
                ptr::null_mut()
            };
            let index_desc = (*(*scan).indexRelation).rd_att;
            (*scan).xs_itupdesc = index_desc;
            (*scan).xs_hitupdesc = index_desc;

            let opaque = context.alloc_zeroed::<Self>();
            opaque.write(Self {
                context,
                session: None,
                result_context,
                order_by_types,
                returned_values: [pg_sys::Datum::from(0); INDEX_MAX_KEYS],
                returned_nulls: [true; INDEX_MAX_KEYS],
            });
            (*scan).opaque = opaque.cast();
        }
    }

    /// # Safety
    ///
    /// `scan` must have been published by [`Self::publish`] with the same
    /// `S` and not ended.
    #[inline]
    pub(super) unsafe fn of<'a>(scan: pg_sys::IndexScanDesc) -> &'a mut Self {
        unsafe { &mut *(*scan).opaque.cast::<Self>() }
    }

    pub(super) fn context(&self) -> MemoryContextHandle {
        self.context
    }

    /// Start the session at the first `amrescan`, or rescan it at a later
    /// one.
    pub(super) fn start_or_rescan(
        &mut self,
        start: impl FnOnce() -> IndexAmResult<S>,
        rescan: impl FnOnce(&mut S) -> IndexAmResult<()>,
    ) -> IndexAmResult<()> {
        match self.session {
            // SAFETY: the session lives until the lifecycle context, which
            // holds `self`, is deleted.
            Some(mut session) => rescan(unsafe { session.as_mut() }),
            None => {
                let session = start()?;
                // SAFETY: the lifecycle context holds `self` and is live.
                self.session = Some(unsafe { self.context.install(session) });
                Ok(())
            }
        }
    }

    #[inline]
    pub(super) fn session_mut(&mut self) -> &mut S {
        // SAFETY: the borrow is tied to `&mut self`.
        unsafe { Self::rescanned(self.session) }
    }

    /// Release the previous fetch's tuple and hand out the session with the
    /// output of this `amgettuple`.
    ///
    /// # Safety
    ///
    /// `scan` must be the descriptor this owner was published on, live for
    /// the current callback.
    #[inline]
    pub(super) unsafe fn begin_fetch<'a>(
        &'a mut self,
        scan: pg_sys::IndexScanDesc,
        direction: ScanDirection,
    ) -> (&'a mut S, IndexTupleOutput<'a, S>) {
        unsafe {
            self.release_returned_tuple(&mut *scan);
            let columns = (*(*(*scan).indexRelation).rd_att).natts as usize;
            let out = IndexTupleOutput::new(
                IndexScanContext::new(scan),
                direction,
                self.order_by_types,
                self.result_context,
                &mut self.returned_values[..columns],
                &mut self.returned_nulls[..columns],
            );
            // The session is disjoint from the returned arrays.
            (Self::rescanned(self.session), out)
        }
    }

    /// # Safety
    ///
    /// `session` must be this owner's, and `'s` must be covered by a
    /// mutable borrow of the owner.
    #[inline]
    unsafe fn rescanned<'s>(session: Option<NonNull<S>>) -> &'s mut S {
        match session {
            // SAFETY: the session lives until the lifecycle context, which
            // holds the owner, is deleted.
            Some(mut session) => unsafe { session.as_mut() },
            None => unreachable!("PostgreSQL rescans an index scan before using it"),
        }
    }

    /// # Safety
    ///
    /// As for [`Self::of`]; `scan` must not be used by this framework again.
    pub(super) unsafe fn end(
        scan: pg_sys::IndexScanDesc,
        end: impl FnOnce(&mut S) -> IndexAmResult<()>,
    ) -> IndexAmResult<()> {
        unsafe {
            let opaque = (*scan).opaque.cast::<Self>();
            (*scan).opaque = ptr::null_mut();
            let context = (*opaque).context;
            let result = (*opaque)
                .session
                .map_or(Ok(()), |mut session| end(session.as_mut()));
            // SAFETY: amendscan runs with PostgreSQL's context current; this
            // framework enters the lifecycle and result contexts only through
            // scoped switches, and `end` has returned.
            context.delete();
            result
        }
    }

    /// Non-NULL keys replace the stored ones and NULL keeps them, as in
    /// `btrescan`.
    ///
    /// # Safety
    ///
    /// `keys` and `orderbys` must be NULL or hold the descriptor's key and
    /// ORDER BY counts.
    pub(super) unsafe fn apply_rescan(
        &mut self,
        scan: pg_sys::IndexScanDesc,
        keys: pg_sys::ScanKey,
        orderbys: pg_sys::ScanKey,
    ) {
        unsafe {
            let scan = &mut *scan;
            let nkeys = scan.numberOfKeys as usize;
            if !keys.is_null() && nkeys > 0 {
                ptr::copy(keys, scan.keyData, nkeys);
            }
            let norderbys = scan.numberOfOrderBys as usize;
            if !orderbys.is_null() && norderbys > 0 {
                ptr::copy(orderbys, scan.orderByData, norderbys);
                for position in 0..norderbys {
                    let operator = (*scan.orderByData.add(position)).sk_func.fn_oid;
                    *self.order_by_types.add(position) =
                        pg_sys::get_func_rettype(operator);
                }
            }
            self.release_returned_tuple(scan);
        }
    }

    #[inline]
    fn release_returned_tuple(&mut self, scan: &mut pg_sys::IndexScanDescData) {
        if scan.xs_hitup.is_null() && scan.xs_itup.is_null() {
            return;
        }
        // SAFETY: the result context is live and holds only that tuple,
        // which PostgreSQL stops using when it calls amgettuple or amrescan.
        // Both call this at entry, before any switch into the context.
        unsafe { self.result_context.reset() };
        scan.xs_hitup = ptr::null_mut();
        scan.xs_itup = ptr::null_mut();
    }
}
