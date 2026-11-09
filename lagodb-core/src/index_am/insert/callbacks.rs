//! `aminsert` / `aminsertcleanup` and the `ii_AmCache` session owner.

use std::marker::PhantomData;
#[cfg(any(feature = "pg17", feature = "pg18"))]
use std::ptr;

use pgrx::{PgMemoryContexts, pg_guard, pg_sys};

#[cfg(any(feature = "pg17", feature = "pg18"))]
use super::IndexInsertCleanupContext;
use super::{
    IndexInsertContext, IndexInsertRow, IndexInsertSession, IndexUniqueCheck,
};
use crate::am_lifecycle::SessionContext;
use crate::backend::MemoryContextHandle;
use crate::diag::ReportableError;
use crate::handles::{IndexInfoHandle, RelationHandle, ValidItemPointer};
use crate::index_am::values::IndexValues;
use crate::index_am::{IndexAccessMethod, IndexAmResult};

struct InsertCache<S: 'static> {
    index_info: *mut pg_sys::IndexInfo,
    _session: PhantomData<S>,
}

impl<S: IndexInsertSession> InsertCache<S> {
    fn new(index_info: *mut pg_sys::IndexInfo) -> Self {
        Self {
            index_info,
            _session: PhantomData,
        }
    }

    /// `begin` runs in a child of `ii_Context`, as `gistinsert` switches to
    /// it: `aminsert` may be called in a per-tuple context (`copyfrom.c`).
    /// An error from `begin` leaves `ii_AmCache` NULL.
    ///
    /// # Safety
    ///
    /// `ii_AmCache` must be NULL or an owner published by this type, and the
    /// returned borrow must end within the current callback.
    unsafe fn session<'s>(
        &self,
        ctx: &IndexInsertContext<'_, S::AccessMethod>,
    ) -> IndexAmResult<&'s mut S> {
        unsafe {
            let cached = (*self.index_info).ii_AmCache.cast::<SessionContext<S>>();
            if !cached.is_null() {
                return Ok((*cached).session_mut());
            }
            let context = MemoryContextHandle::create(
                (*self.index_info).ii_Context,
                c"lagodb index insert",
            );
            let session = PgMemoryContexts::For(context.as_ptr())
                .switch_to(|_| S::begin(ctx))?;
            let owner = SessionContext::install(context, session).into_own_context();
            (*self.index_info).ii_AmCache = owner.cast();
            Ok((*owner).session_mut())
        }
    }

    /// # Safety
    ///
    /// As for [`Self::session`].
    #[cfg(any(feature = "pg17", feature = "pg18"))]
    unsafe fn finish(
        &self,
        ctx: &IndexInsertCleanupContext<'_, S::AccessMethod>,
    ) -> IndexAmResult<()> {
        unsafe {
            let owner = (*self.index_info).ii_AmCache.cast::<SessionContext<S>>();
            // PostgreSQL calls aminsertcleanup whether or not any row was
            // inserted (`index_insert_cleanup`).
            if owner.is_null() {
                return Ok(());
            }
            (*self.index_info).ii_AmCache = ptr::null_mut();
            SessionContext::finish_with(owner, |session| session.finish(ctx))
        }
    }
}

#[pg_guard]
#[allow(clippy::too_many_arguments)] // PG IndexAmRoutine::aminsert ABI.
pub(in crate::index_am) extern "C-unwind" fn aminsert<A: IndexAccessMethod>(
    index: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    heap_tid: pg_sys::ItemPointer,
    heap: pg_sys::Relation,
    check_unique: pg_sys::IndexUniqueCheck::Type,
    index_unchanged: bool,
    index_info: *mut pg_sys::IndexInfo,
) -> bool {
    // SAFETY: PostgreSQL passes live relations, IndexInfo, a valid heap TID
    // and one value per index column for this call.
    unsafe {
        let ctx = IndexInsertContext::<A>::new(
            RelationHandle::from_raw(index),
            RelationHandle::from_raw(heap),
            IndexInfoHandle::from_raw(index_info),
        );
        let row = IndexInsertRow {
            values: IndexValues::from_raw(
                values,
                isnull,
                ctx.index_info.num_index_attributes(),
            ),
            heap_tid: ValidItemPointer::from_raw(heap_tid),
            unique_check: IndexUniqueCheck::from_raw(check_unique),
            index_unchanged,
        };
        InsertCache::<A::InsertSession>::new(index_info)
            .session(&ctx)
            .and_then(|session| session.insert(&ctx, row))
            .report_unwrap()
    }
}

#[cfg(any(feature = "pg17", feature = "pg18"))]
#[pg_guard]
pub(in crate::index_am) extern "C-unwind" fn aminsertcleanup<A: IndexAccessMethod>(
    index: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) {
    // SAFETY: PostgreSQL passes the live index and the IndexInfo whose
    // inserts are ending.
    unsafe {
        let ctx = IndexInsertCleanupContext::<A>::new(
            RelationHandle::from_raw(index),
            IndexInfoHandle::from_raw(index_info),
        );
        InsertCache::<A::InsertSession>::new(index_info)
            .finish(&ctx)
            .report_unwrap();
    }
}
