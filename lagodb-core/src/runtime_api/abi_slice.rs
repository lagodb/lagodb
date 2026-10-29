//! Borrowed array with a fixed C layout for synchronous runtime ABI calls.

use std::marker::PhantomData;
use std::slice;

/// Pointer/length view of a slice borrowed for `'a`.
///
/// The only constructor takes `&'a [T]`, so the pointer is never null and the
/// length always describes live storage; readers obtain the slice without
/// re-validating either field.
#[repr(C)]
pub struct AbiSlice<'a, T> {
    pointer: *const T,
    len: usize,
    _borrow: PhantomData<&'a [T]>,
}

impl<'a, T> AbiSlice<'a, T> {
    #[must_use]
    pub const fn new(items: &'a [T]) -> Self {
        Self {
            pointer: items.as_ptr(),
            len: items.len(),
            _borrow: PhantomData,
        }
    }

    #[must_use]
    pub const fn as_slice(self) -> &'a [T] {
        // SAFETY: `new` is the only constructor and stores a live slice
        // borrowed for `'a`.
        unsafe { slice::from_raw_parts(self.pointer, self.len) }
    }
}

// Copying the view never copies `T`, so these impls carry no `T` bounds.
impl<T> Clone for AbiSlice<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for AbiSlice<'_, T> {}
