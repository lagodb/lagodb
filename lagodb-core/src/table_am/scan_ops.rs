use super::AmResult;
#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
use crate::handles::TBMIterateResultHandle;
#[cfg(feature = "pg18")]
use crate::handles::{BitmapScanOutputHandle, TBMIteratorHandle};
use crate::handles::{ItemPointer, ScanDirection};
use crate::tuple::Row;

/// Complete callback group for PostgreSQL TID-range scans.
///
/// Supplying this value declares support and provides both callbacks required
/// by PostgreSQL. Omitting it leaves both callbacks unregistered.
pub struct TidRangeOps<S: ?Sized> {
    set: fn(&mut S, &ItemPointer, &ItemPointer) -> AmResult<()>,
    next: fn(&mut S, ScanDirection, &mut Row) -> AmResult<bool>,
}

impl<S: ?Sized> TidRangeOps<S> {
    #[must_use]
    pub const fn new(
        set: fn(&mut S, &ItemPointer, &ItemPointer) -> AmResult<()>,
        next: fn(&mut S, ScanDirection, &mut Row) -> AmResult<bool>,
    ) -> Self {
        Self { set, next }
    }

    #[inline]
    pub(crate) fn set(
        &self,
        session: &mut S,
        min_tid: &ItemPointer,
        max_tid: &ItemPointer,
    ) -> AmResult<()> {
        (self.set)(session, min_tid, max_tid)
    }

    #[inline]
    pub(crate) fn next(
        &self,
        session: &mut S,
        direction: ScanDirection,
        row: &mut Row,
    ) -> AmResult<bool> {
        (self.next)(session, direction, row)
    }
}

/// Complete callback group for PostgreSQL 17 bitmap table scans.
///
/// PostgreSQL selects a bitmap block before `next_block`; `next_tuple` then
/// produces the visible tuples selected from that block. PostgreSQL requires
/// these callbacks to be present or absent together.
#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
pub struct BitmapScanOps<S: ?Sized> {
    next_block: fn(&mut S, &TBMIterateResultHandle) -> AmResult<bool>,
    next_tuple: fn(&mut S, &TBMIterateResultHandle, &mut Row) -> AmResult<bool>,
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
impl<S: ?Sized> BitmapScanOps<S> {
    #[must_use]
    pub const fn new(
        next_block: fn(&mut S, &TBMIterateResultHandle) -> AmResult<bool>,
        next_tuple: fn(&mut S, &TBMIterateResultHandle, &mut Row) -> AmResult<bool>,
    ) -> Self {
        Self {
            next_block,
            next_tuple,
        }
    }

    #[inline]
    pub(crate) fn next_block(
        &self,
        session: &mut S,
        result: &TBMIterateResultHandle,
    ) -> AmResult<bool> {
        (self.next_block)(session, result)
    }

    #[inline]
    pub(crate) fn next_tuple(
        &self,
        session: &mut S,
        result: &TBMIterateResultHandle,
        row: &mut Row,
    ) -> AmResult<bool> {
        (self.next_tuple)(session, result, row)
    }
}

/// Complete callback group for PostgreSQL 18 bitmap table scans.
///
/// PostgreSQL 18 assigns bitmap iteration and prefetch policy to the table
/// access method. Advancing `iterator` does not count a page; callback-local
/// page tokens may be retained for look-ahead, and the provider records a
/// lossy or exact page through `output` only when it processes it.
#[cfg(feature = "pg18")]
pub struct BitmapScanOps<S: ?Sized> {
    next_tuple: for<'iterator, 'output> fn(
        &mut S,
        &mut TBMIteratorHandle<'iterator>,
        &mut BitmapScanOutputHandle<'output>,
        &mut Row,
    ) -> AmResult<bool>,
}

#[cfg(feature = "pg18")]
impl<S: ?Sized> BitmapScanOps<S> {
    #[must_use]
    pub const fn new(
        next_tuple: for<'iterator, 'output> fn(
            &mut S,
            &mut TBMIteratorHandle<'iterator>,
            &mut BitmapScanOutputHandle<'output>,
            &mut Row,
        ) -> AmResult<bool>,
    ) -> Self {
        Self { next_tuple }
    }

    #[inline]
    pub(crate) fn next_tuple(
        &self,
        session: &mut S,
        iterator: &mut TBMIteratorHandle<'_>,
        output: &mut BitmapScanOutputHandle<'_>,
        row: &mut Row,
    ) -> AmResult<bool> {
        (self.next_tuple)(session, iterator, output, row)
    }
}
