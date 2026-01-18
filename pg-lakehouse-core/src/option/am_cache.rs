//! Access Method Cache (rd_amcache) management.
//!
//! This module provides a high-performance, safe mechanism for using Postgres' `rd_amcache`
//! field in `RelationData`.
//!
//! # Problem
//!
//! `rd_amcache` is a `void*` pointer that Postgres manages. When a relation cache invalidation
//! occurs (e.g. `ALTER TABLE`, `DROP TABLE`, or just cache pressure), Postgres calls `pfree()`
//! on this pointer and sets it to NULL.
//!
//! This creates two constraints for Rust:
//! 1. **Memory Safety**: The memory MUST be allocated via Postgres' `palloc` (or equivalent).
//!    We cannot put a standard Rust struct (like `Box<T>`, `Vec<T>`, `String`) there because
//!    `pfree` won't call Rust's `drop`, leading to memory leaks of the heap-allocated parts.
//! 2. **Performance**: This cache is accessed in hot paths (e.g. every tuple scan). We need
//!    O(1) access to options, avoiding repeated string parsing or hash map lookups.
//!
//! # Solution: `#[repr(C)]` Header + Variable Data Area
//!
//! We allow Access Methods to define a custom `#[repr(C)]` struct (Header) that acts as the
//! typed view of the cache. Variable-length data (strings) are stored immediately after
//! this header in the same contiguous memory block.
//!
//! ```text
//! +-------------------------------------------------------+
//! |  IcebergCacheHeader (Fixed Size, #[repr(C)])          | <- rd_amcache points here
//! |-------------------------------------------------------|
//! |  format_version: i32                                  |
//! |  compression_offset: u32  (relative to start)         |
//! |  ...                                                  |
//! +-------------------------------------------------------+
//! |  Variable Data Area (u8 bytes)                        |
//! |-------------------------------------------------------|
//! |  "zstd\0"                                             |
//! |  "s3://bucket/path\0"                                 |
//! +-------------------------------------------------------+
//! ```
//!
//! Accessing `format_version` is a direct memory read. Accessing `compression` involves
//! adding `compression_offset` to the base pointer. Both are O(1).

use crate::handles::RelationHandle;
use crate::option::{TableOptionError, TableOptions};
use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys;

/// Trait for types that can be stored in `rd_amcache`.
///
/// Implementors must be `#[repr(C)]` and `Copy`.
///
/// # Safety
///
/// Implementors MUST be `#[repr(C)]` and contain only POD (Plain Old Data) types.
/// No `String`, `Vec`, `Box`, or other types that require `Drop`.
pub unsafe trait AmCacheable: Copy + Sized {
    /// Parse options and return the Header struct and the variable data bytes.
    ///
    /// The Header struct will be placed at the start of the allocated memory.
    /// The variable data bytes will be copied immediately after the Header.
    ///
    /// Offsets in the Header should be calculated relative to the start of the Header (0).
    fn from_options(opts: &TableOptions) -> (Self, Vec<u8>);

    /// Create a default Header and data when no options are present.
    fn default_options() -> (Self, Vec<u8>);
}

/// Helper to manage `rd_amcache`.
pub struct AmCache;

impl AmCache {
    /// Get typed reference to the cached data.
    ///
    /// If the cache is empty, it loads options from the catalog, parses them using `T::from_options`,
    /// allocates a single contiguous memory block via `palloc`, and updates `rd_amcache`.
    ///
    /// # Returns
    ///
    /// Returns a reference `&T` to the Header part of the cache.
    /// The lifetime is tied to the `RelationHandle`, ensuring safety as long as the relation is locked.
    pub fn get<'a, T: AmCacheable>(
        rel: &RelationHandle<'a>,
    ) -> Result<&'a T, TableOptionError> {
        let rel_ptr = rel.as_raw();
        unsafe {
            if rel_ptr.is_null() {
                return Err(TableOptionError::PersistFailed(
                    "Relation is null".to_string(),
                ));
            }

            // Fast path: Check if cache is already populated
            if !(*rel_ptr).rd_amcache.is_null() {
                return Ok(&*((*rel_ptr).rd_amcache as *const T));
            }

            // Slow path: Load and populate cache
            Self::load_and_cache::<T>(rel_ptr)
        }
    }

    #[cold]
    unsafe fn load_and_cache<'a, T: AmCacheable>(
        rel: pg_sys::Relation,
    ) -> Result<&'a T, TableOptionError> {
        unsafe {
            // 1. Load options from Catalog
            let opts = TableOptions::load_from_catalog((*rel).rd_id)?;

            // 2. Parse options into Header + Data
            let (header, data) = if let Some(opts) = opts {
                T::from_options(&opts)
            } else {
                T::default_options()
            };

            // 3. Calculate total size and alignment
            let header_size = std::mem::size_of::<T>();
            let total_size = header_size + data.len();

            // 4. Allocate memory in CacheMemoryContext via Postgres allocator (palloc)
            // We switch to CacheMemoryContext to ensure the memory persists as long as the Relation
            let ptr = PgMemoryContexts::CacheMemoryContext.switch_to(|_| {
                let ptr = pg_sys::palloc(total_size) as *mut u8;

                // Copy Header
                std::ptr::copy_nonoverlapping(
                    &header as *const T as *const u8,
                    ptr,
                    header_size,
                );

                // Copy Data
                if !data.is_empty() {
                    std::ptr::copy_nonoverlapping(
                        data.as_ptr(),
                        ptr.add(header_size),
                        data.len(),
                    );
                }
                ptr
            });

            // 5. Update rd_amcache
            (*rel).rd_amcache = ptr as *mut std::ffi::c_void;

            // 6. Return reference
            Ok(&*(ptr as *const T))
        }
    }
}
