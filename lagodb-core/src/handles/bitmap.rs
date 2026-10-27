//! PostgreSQL 18 bitmap table-scan protocol handles.

use core::marker::PhantomData;
use core::mem::{MaybeUninit, offset_of, size_of};
use core::ptr::from_ref;
use core::slice;
use std::fmt;

use pgrx::pg_sys;

const MAX_ALIGNMENT: usize = pg_sys::MAXIMUM_ALIGNOF as usize;
const HEAP_TUPLE_HEADER_SIZE: usize = offset_of!(pg_sys::HeapTupleHeaderData, t_bits);
const ALIGNED_HEAP_TUPLE_HEADER_SIZE: usize =
    (HEAP_TUPLE_HEADER_SIZE + MAX_ALIGNMENT - 1) & !(MAX_ALIGNMENT - 1);
const PAGE_HEADER_SIZE: usize = offset_of!(pg_sys::PageHeaderData, pd_linp);

// PostgreSQL's MaxHeapTuplesPerPage/TBM_MAX_TUPLES_PER_PAGE formula. pgrx
// does not export either C macro, so keep its layout-derived value beside the
// only buffer whose capacity depends on it.
const MAX_BITMAP_OFFSETS: usize = (pg_sys::BLCKSZ as usize - PAGE_HEADER_SIZE)
    / (ALIGNED_HEAP_TUPLE_HEADER_SIZE + size_of::<pg_sys::ItemIdData>());

/// One callback-scoped page yielded by PostgreSQL's bitmap iterator.
///
/// The page token is independent of the mutable iterator borrow, so a provider
/// can advance the iterator again while retaining earlier tokens for
/// callback-local prefetch decisions. It must extract exact offsets before the
/// bitmap-scan callback returns.
#[derive(Debug, Clone, Copy)]
pub struct BitmapPage<'a> {
    result: pg_sys::TBMIterateResult,
    _iterator: PhantomData<&'a pg_sys::TBMIterator>,
}

impl BitmapPage<'_> {
    #[inline]
    pub fn block_number(&self) -> pg_sys::BlockNumber {
        self.result.blockno
    }

    #[inline]
    pub fn is_lossy(&self) -> bool {
        self.result.lossy
    }

    #[inline]
    pub fn recheck(&self) -> bool {
        self.result.recheck
    }

    /// Extract exact-page offsets into caller-owned reusable storage.
    ///
    /// Lossy pages return `None` because they have no exact offsets.
    #[inline]
    pub fn exact_offsets<'buffer>(
        &self,
        buffer: &'buffer mut BitmapPageOffsets,
    ) -> Option<&'buffer [pg_sys::OffsetNumber]> {
        if self.result.lossy {
            return None;
        }

        let mut result = self.result;
        // SAFETY: only the `TBMIteratorHandle` iterator constructs page
        // tokens. For an exact page, PostgreSQL keeps result.internal_page
        // valid for the bitmap iterator lifetime carried by this token.
        let count = unsafe {
            pg_sys::tbm_extract_page_tuple(
                &mut result,
                buffer.offsets.as_mut_ptr().cast(),
                MAX_BITMAP_OFFSETS as u32,
            )
        } as usize;
        debug_assert!(count <= MAX_BITMAP_OFFSETS);
        let initialized = count.min(MAX_BITMAP_OFFSETS);
        // SAFETY: tbm_extract_page_tuple initialized the first `initialized`
        // entries. Bounding the returned count locally keeps this slice within
        // the fixed allocation even if the mirrored PostgreSQL limit diverges.
        Some(unsafe {
            slice::from_raw_parts(
                buffer.offsets.as_ptr().cast::<pg_sys::OffsetNumber>(),
                initialized,
            )
        })
    }
}

/// Reusable fixed-capacity storage for one exact bitmap page's offsets.
#[derive(Debug)]
pub struct BitmapPageOffsets {
    offsets: [MaybeUninit<pg_sys::OffsetNumber>; MAX_BITMAP_OFFSETS],
}

impl BitmapPageOffsets {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            offsets: [MaybeUninit::uninit(); MAX_BITMAP_OFFSETS],
        }
    }
}

impl Default for BitmapPageOffsets {
    fn default() -> Self {
        Self::new()
    }
}

/// Mutable view of PostgreSQL's bitmap iterator.
///
/// PostgreSQL 18 moved bitmap iteration into the table access method. This
/// handle lets an access method control iteration, look-ahead, and prefetch
/// policy while keeping the raw iterator pointer inside the framework.
pub struct TBMIteratorHandle<'a> {
    inner: &'a mut pg_sys::TBMIterator,
}

impl fmt::Debug for TBMIteratorHandle<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("TBMIteratorHandle")
            .field(&from_ref(&*self.inner))
            .finish()
    }
}

impl<'a> TBMIteratorHandle<'a> {
    /// # Safety
    ///
    /// `ptr` must be non-null, exclusively borrowed for `'a`, and point to the
    /// iterator owned by the current bitmap table scan.
    #[inline]
    pub(crate) unsafe fn from_raw(ptr: *mut pg_sys::TBMIterator) -> Self {
        Self {
            inner: unsafe { &mut *ptr },
        }
    }
}

impl<'a> Iterator for TBMIteratorHandle<'a> {
    type Item = BitmapPage<'a>;

    /// Advance to the next callback-scoped bitmap page.
    ///
    /// Iteration does not extract offsets or count a consumed page. The
    /// provider may retain multiple page tokens during this callback while it
    /// advances for prefetch, then extract an exact page through
    /// [`BitmapPage::exact_offsets`] when it processes that page. It records
    /// lossy/exact pages through [`BitmapScanOutputHandle`] only after
    /// processing them.
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let mut result = pg_sys::TBMIterateResult::default();
        // SAFETY: `inner` is the live, exclusively borrowed iterator supplied
        // to the current PostgreSQL bitmap-scan callback.
        if !unsafe { pg_sys::tbm_iterate(self.inner, &mut result) } {
            return None;
        }

        Some(BitmapPage {
            result,
            _iterator: PhantomData,
        })
    }
}

/// Callback-scoped output values for PostgreSQL's bitmap protocol.
///
/// The provider owns page processing in PG18. It therefore records a page
/// only after consuming it, independently of callback-local iterator
/// look-ahead, and sets `recheck` for the page that produced the returned
/// tuple.
#[derive(Debug)]
pub struct BitmapScanOutputHandle<'a> {
    recheck: &'a mut bool,
    lossy_pages: &'a mut u64,
    exact_pages: &'a mut u64,
}

impl<'a> BitmapScanOutputHandle<'a> {
    /// # Safety
    ///
    /// All pointers must be non-null, exclusively borrowed for `'a`, and be
    /// the outputs supplied to the current PG18 bitmap callback.
    #[inline]
    pub(crate) unsafe fn from_raw(
        recheck: *mut bool,
        lossy_pages: *mut u64,
        exact_pages: *mut u64,
    ) -> Self {
        Self {
            recheck: unsafe { &mut *recheck },
            lossy_pages: unsafe { &mut *lossy_pages },
            exact_pages: unsafe { &mut *exact_pages },
        }
    }

    /// Set the executor recheck flag for tuples returned from the current
    /// bitmap page.
    #[inline]
    pub fn set_recheck(&mut self, recheck: bool) {
        *self.recheck = recheck;
    }

    /// Record one lossy bitmap page after the provider has processed it.
    #[inline]
    pub fn count_lossy_page(&mut self) {
        *self.lossy_pages += 1;
    }

    /// Record one exact bitmap page after the provider has processed it.
    #[inline]
    pub fn count_exact_page(&mut self) {
        *self.exact_pages += 1;
    }
}
