use std::marker::PhantomData;
use std::ptr::NonNull;

/// Borrowed, non-null pointer to a PostgreSQL-owned object.
///
/// This is intentionally crate-private. Public handle constructors decide which
/// PostgreSQL pointers are allowed to be null and which represent a required
/// borrowed object.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PgBorrowed<'a, T> {
    ptr: NonNull<T>,
    _marker: PhantomData<&'a T>,
}

impl<'a, T> PgBorrowed<'a, T> {
    /// # Safety
    ///
    /// `ptr` must be valid for `'a` and must point to an object owned by
    /// PostgreSQL or by an owning guard that outlives `'a`.
    pub(crate) unsafe fn from_raw(ptr: *mut T) -> Self {
        Self {
            ptr: NonNull::new(ptr).expect("PostgreSQL passed a null pointer"),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub(crate) fn as_ptr(&self) -> *mut T {
        self.ptr.as_ptr()
    }

    /// # Safety
    ///
    /// The underlying PostgreSQL object must still be alive and readable for
    /// the duration of the returned borrow.
    #[inline]
    pub(crate) unsafe fn as_ref(&self) -> &T {
        unsafe { self.ptr.as_ref() }
    }
}

/// Borrowed pointer to a PostgreSQL-owned object where null is a valid value.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PgNullable<'a, T> {
    ptr: *mut T,
    _marker: PhantomData<&'a T>,
}

impl<'a, T> PgNullable<'a, T> {
    /// # Safety
    ///
    /// If `ptr` is non-null, it must be valid for `'a`.
    pub(crate) unsafe fn from_raw(ptr: *mut T) -> Self {
        Self {
            ptr,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub(crate) fn as_ptr(&self) -> *mut T {
        self.ptr
    }
}
