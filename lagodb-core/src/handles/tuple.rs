use core::mem;
use core::num::NonZeroU16;
use core::ptr::NonNull;

use super::borrowed::PgBorrowed;
use pgrx::pg_sys;

/// Callback-scoped borrowed handle for a physical PostgreSQL `TupleTableSlot`.
///
/// This handle intentionally exposes no datum view. Some table-AM callbacks
/// require the slot's physical representation, including buffer-backed heap
/// slots, while datum materialization belongs to [`TupleSlotRow`].
#[derive(Debug)]
pub struct TupleTableSlotHandle<'a> {
    inner: PgBorrowed<'a, pg_sys::TupleTableSlot>,
}

impl<'a> TupleTableSlotHandle<'a> {
    /// # Safety
    ///
    /// PostgreSQL's `tuple_satisfies_snapshot` callback supplies a non-null,
    /// initialized slot that remains valid for `'a`.
    #[inline]
    pub(crate) unsafe fn from_raw(ptr: *mut pg_sys::TupleTableSlot) -> Self {
        // SAFETY: the caller is the FFI dispatcher for the PostgreSQL callback
        // whose ABI contract guarantees a non-null slot.
        let ptr = unsafe { NonNull::new_unchecked(ptr) };
        Self {
            inner: unsafe { PgBorrowed::from_non_null(ptr) },
        }
    }

    /// Return the underlying physical slot pointer for explicit PostgreSQL FFI
    /// interop.
    ///
    /// The raw pointer does not carry `'a`; it is valid only while the active
    /// table-AM callback keeps the slot alive and must not be used afterward.
    #[inline]
    pub fn as_raw(&self) -> *mut pg_sys::TupleTableSlot {
        self.inner.as_ptr()
    }
}

/// Safe value wrapper for PostgreSQL ItemPointer (TID).
///
/// Some PostgreSQL APIs deliberately use offset zero as a range bound or pass
/// arbitrary user input to an AM validity callback, so this general value type
/// also represents invalid physical identities.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ItemPointer {
    pub block_number: u32,
    pub offset: u16,
}

impl ItemPointer {
    /// # Safety
    ///
    /// `ptr` must be non-null and point to a valid `ItemPointerData`.
    #[inline]
    pub unsafe fn from_raw(ptr: pg_sys::ItemPointer) -> Self {
        unsafe {
            let sys = *ptr;
            let block_number =
                (sys.ip_blkid.bi_hi as u32) << 16 | (sys.ip_blkid.bi_lo as u32);
            Self {
                block_number,
                offset: sys.ip_posid,
            }
        }
    }

    /// Convert this value to PostgreSQL's C representation.
    #[inline]
    #[must_use]
    pub fn to_pg_sys(&self) -> pg_sys::ItemPointerData {
        pg_sys::ItemPointerData {
            ip_blkid: pg_sys::BlockIdData {
                bi_hi: (self.block_number >> 16) as u16,
                bi_lo: (self.block_number & 0xFFFF) as u16,
            },
            ip_posid: self.offset,
        }
    }

    /// # Safety
    ///
    /// `ptr` must be non-null and point to writable PostgreSQL
    /// `ItemPointerData` storage.
    #[inline]
    pub(crate) unsafe fn write_to_raw(&self, ptr: pg_sys::ItemPointer) {
        unsafe {
            *ptr = self.to_pg_sys();
        }
    }
}

/// PostgreSQL ItemPointer proven suitable for use as a physical row identity.
///
/// PostgreSQL's `ItemPointerIsValid` contract requires a nonzero item offset.
/// FDW scan and modify writers accept this type so they do not repeat the same
/// validity check for every returned row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ValidItemPointer {
    block_number: u32,
    offset: NonZeroU16,
}

impl ValidItemPointer {
    /// Construct a valid identity from its block number and nonzero offset.
    #[inline]
    #[must_use]
    pub const fn new(block_number: u32, offset: NonZeroU16) -> Self {
        Self {
            block_number,
            offset,
        }
    }

    /// Construct an identity from an offset already proven to be nonzero.
    ///
    /// # Safety
    ///
    /// `offset` must not be zero.
    #[inline]
    #[must_use]
    pub const unsafe fn new_unchecked(block_number: u32, offset: u16) -> Self {
        Self {
            block_number,
            // SAFETY: upheld by the caller.
            offset: unsafe { NonZeroU16::new_unchecked(offset) },
        }
    }

    /// Copy a PostgreSQL ItemPointer already proven valid.
    ///
    /// # Safety
    ///
    /// `ptr` must be non-null and satisfy PostgreSQL's
    /// `ItemPointerIsValid` contract.
    #[inline]
    pub unsafe fn from_raw(ptr: pg_sys::ItemPointer) -> Self {
        let value = unsafe { ItemPointer::from_raw(ptr) };
        // SAFETY: the caller guarantees a nonzero offset.
        unsafe { Self::new_unchecked(value.block_number, value.offset) }
    }

    /// Return the PostgreSQL block number.
    #[inline]
    #[must_use]
    pub const fn block_number(self) -> u32 {
        self.block_number
    }

    /// Return the one-based PostgreSQL item offset.
    #[inline]
    #[must_use]
    pub const fn offset(self) -> u16 {
        self.offset.get()
    }

    /// Convert this identity to PostgreSQL's C representation.
    #[inline]
    #[must_use]
    pub fn to_pg_sys(self) -> pg_sys::ItemPointerData {
        ItemPointer {
            block_number: self.block_number,
            offset: self.offset.get(),
        }
        .to_pg_sys()
    }
}

/// Borrowed heap tuple whose lifetime is tied to a PostgreSQL owner such as a
/// system scan or syscache guard.
#[derive(Debug, Clone, Copy)]
pub struct HeapTupleRef<'a> {
    inner: PgBorrowed<'a, pg_sys::HeapTupleData>,
}

impl<'a> HeapTupleRef<'a> {
    /// # Safety
    ///
    /// `tuple` must be valid for the lifetime represented by this value.
    pub unsafe fn from_raw(tuple: pg_sys::HeapTuple) -> Self {
        Self {
            inner: unsafe { PgBorrowed::from_raw(tuple) },
        }
    }

    #[inline]
    pub fn as_raw(&self) -> pg_sys::HeapTuple {
        self.inner.as_ptr()
    }

    #[inline]
    pub(crate) fn item_pointer_data(&self) -> pg_sys::ItemPointerData {
        unsafe { self.inner.as_ref().t_self }
    }
}

/// RAII guard for heap tuple - automatically frees tuple when dropped.
#[derive(Debug)]
pub struct HeapTupleGuard {
    tuple: pg_sys::HeapTuple,
}

impl HeapTupleGuard {
    /// Create a new guard from a raw heap tuple.
    /// The guard takes ownership of the tuple and will free it on drop.
    ///
    /// # Safety
    ///
    /// `tuple` must be owned by the caller and must require `heap_freetuple`
    /// cleanup.
    pub unsafe fn new(tuple: pg_sys::HeapTuple) -> Self {
        Self { tuple }
    }

    /// Get the raw heap tuple pointer.
    #[inline]
    pub fn as_raw(&self) -> pg_sys::HeapTuple {
        self.tuple
    }

    /// Transfer ownership of the tuple to PostgreSQL or another owner that
    /// will eventually release it with `heap_freetuple`.
    #[inline]
    pub(crate) fn into_raw(self) -> pg_sys::HeapTuple {
        let tuple = self.tuple;
        mem::forget(self);
        tuple
    }

    #[inline]
    pub fn as_tuple_ref(&self) -> HeapTupleRef<'_> {
        unsafe { HeapTupleRef::from_raw(self.tuple) }
    }
}

impl Drop for HeapTupleGuard {
    fn drop(&mut self) {
        unsafe {
            pg_sys::heap_freetuple(self.tuple);
        }
    }
}
