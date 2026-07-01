use super::borrowed::PgBorrowed;
use pgrx::pg_sys;

/// Safe wrapper for PostgreSQL TupleTableSlot.
#[derive(Debug)]
pub struct TupleTableSlotHandle<'a> {
    inner: PgBorrowed<'a, pg_sys::TupleTableSlot>,
}

impl<'a> TupleTableSlotHandle<'a> {
    /// # Safety
    ///
    /// `ptr` must be non-null and valid for `'a`.
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::TupleTableSlot) -> Self {
        Self {
            inner: unsafe { PgBorrowed::from_raw(ptr) },
        }
    }

    #[inline]
    pub fn as_raw(&self) -> *mut pg_sys::TupleTableSlot {
        self.inner.as_ptr()
    }
}

/// Safe wrapper for PostgreSQL ItemPointer (TID).
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

    #[inline]
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
