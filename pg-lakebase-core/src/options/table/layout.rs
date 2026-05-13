//! Inline variable-data layout for `rd_amcache` payloads.
//!
//! Access method caches store a fixed-size `#[repr(C)]` header followed by
//! nul-terminated string bytes in the same Postgres allocation. Header fields
//! keep offsets from the allocation base.

use std::ffi::CStr;

pub type AmCacheStringOffset = u32;

/// Builds the variable-data section that follows an `rd_amcache` header.
pub struct AmCacheLayoutBuilder {
    header_size: usize,
    data: Vec<u8>,
}

impl AmCacheLayoutBuilder {
    pub fn for_header<T>() -> Self {
        Self {
            header_size: std::mem::size_of::<T>(),
            data: Vec::new(),
        }
    }

    /// Appends a string and returns its offset from the allocation base.
    ///
    /// Offset 0 is reserved for an empty string.
    pub fn push_str(&mut self, value: &str) -> AmCacheStringOffset {
        if value.is_empty() {
            return 0;
        }

        let offset = (self.header_size + self.data.len()) as AmCacheStringOffset;
        self.data.extend_from_slice(value.as_bytes());
        self.data.push(0);
        offset
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.data
    }
}

/// Reads values from an `rd_amcache` inline variable-data layout.
pub struct AmCacheLayout;

impl AmCacheLayout {
    /// Reads a nul-terminated string from an offset stored in the cache header.
    ///
    /// # Safety
    /// `base_ptr` must point at the start of a valid `rd_amcache` allocation,
    /// and `offset` must either be 0 or point at a nul-terminated string inside
    /// that allocation.
    pub unsafe fn str_at_offset<'a>(
        base_ptr: *const u8,
        offset: AmCacheStringOffset,
    ) -> &'a str {
        if offset == 0 {
            return "";
        }
        // SAFETY: caller guarantees offset points at a nul-terminated string.
        let ptr = unsafe { base_ptr.add(offset as usize) };
        let c_str = unsafe { CStr::from_ptr(ptr as *const i8) };
        c_str.to_str().unwrap_or_else(|_| {
            debug_assert!(false, "rd_amcache contained invalid UTF-8");
            ""
        })
    }
}
