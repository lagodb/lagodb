//! Access Method Cache (rd_amcache) management.
//!
//! This module provides a high-performance primitive for using PostgreSQL's
//! `rd_amcache` field in `RelationData`, with safety completed by an
//! access-method-specific typed accessor.
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
//! # Solution: typed headers with an optional inline payload
//!
//! Access methods parse their persisted options into a compact `#[repr(C)]`
//! header. Finite values use POD discriminants; arbitrary strings live in an
//! optional inline payload and are represented by inert [`AmCacheString`]
//! handles. A handle can only be resolved through [`AmCacheRef`], which retains
//! the original allocation base and validates payload bounds.
//!
//! The generic cache primitive remains unsafe because PostgreSQL exposes
//! `rd_amcache` as an untyped `void *`; access methods must provide their own
//! safe, single-type accessor.
//!
//! ```text
//! +-------------------------------------------------------+
//! |  RawAmCache<IcebergHeader>                            | <- rd_amcache points here
//! |-------------------------------------------------------|
//! |  payload_len: usize                                   |
//! |  header: IcebergHeader                                |
//! +-------------------------------------------------------+
//! |  Optional inline UTF-8 payload                        |
//! +-------------------------------------------------------+
//! ```

use std::ops::Range;

use crate::handles::RelationHandle;

use super::{TableOptionError, TableOptions};
use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys;

/// Trait for types that can be stored in `rd_amcache`.
///
/// Implementors must be `#[repr(C)]` and `Copy`.
///
/// # Safety
///
/// Implementors MUST be `#[repr(C)]` values containing only POD (Plain Old
/// Data) fields and [`AmCacheString`] handles. No `String`, `Vec`, `Box`, raw
/// pointers, references, or other address-dependent state. Every string handle
/// in the header must come from the [`AmCacheValueBuilder`] returned with that
/// same header.
pub unsafe trait AmCacheable: Copy + Sized {
    /// Resolve persisted options into a cache header and optional inline payload.
    fn from_options(
        opts: Option<&TableOptions>,
    ) -> Result<AmCacheValue<Self>, TableOptionError>;
}

/// Address-independent reference to one UTF-8 string in an inline cache payload.
///
/// The fields are private and the handle has no standalone dereference API, so
/// copying a cache header cannot accidentally change the address used to resolve
/// the string.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AmCacheString {
    offset: u32,
    len: u32,
}

impl AmCacheString {
    fn range(self) -> Option<Range<usize>> {
        let start = usize::try_from(self.offset).ok()?;
        let len = usize::try_from(self.len).ok()?;
        Some(start..start.checked_add(len)?)
    }

    fn resolve<'a>(self, payload: &'a [u8]) -> &'a str {
        let bytes = self
            .range()
            .and_then(|range| payload.get(range))
            .expect("AM cache string handle is outside its inline payload");
        std::str::from_utf8(bytes)
            .expect("AM cache string payload is not valid UTF-8")
    }
}

/// Builder for an AM cache's optional inline string payload.
///
/// An implementation stores returned [`AmCacheString`] handles in its private
/// header and finishes the allocation with that header:
///
/// ```ignore
/// let mut builder = AmCacheValueBuilder::new();
/// let label = builder.push_str(label)?;
/// Ok(builder.finish(MyPrivateHeader { label }))
/// ```
#[derive(Debug, Default)]
pub struct AmCacheValueBuilder {
    payload: Vec<u8>,
}

impl AmCacheValueBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_str(
        &mut self,
        value: &str,
    ) -> Result<AmCacheString, TableOptionError> {
        let end = self
            .payload
            .len()
            .checked_add(value.len())
            .filter(|end| *end <= u32::MAX as usize)
            .ok_or_else(|| {
                TableOptionError::InvalidOption(
                    "AM cache string payload exceeds 4 GiB".to_owned(),
                )
            })?;
        let offset = u32::try_from(self.payload.len()).map_err(|_| {
            TableOptionError::InvalidOption(
                "AM cache string payload exceeds 4 GiB".to_owned(),
            )
        })?;
        let len = u32::try_from(value.len()).map_err(|_| {
            TableOptionError::InvalidOption(
                "AM cache string value exceeds 4 GiB".to_owned(),
            )
        })?;
        self.payload.reserve(end - self.payload.len());
        self.payload.extend_from_slice(value.as_bytes());
        Ok(AmCacheString { offset, len })
    }

    pub fn finish<T>(self, header: T) -> AmCacheValue<T> {
        AmCacheValue {
            header,
            payload: self.payload,
        }
    }
}

/// Owned cache material produced before entering PostgreSQL cache memory.
pub struct AmCacheValue<T> {
    header: T,
    payload: Vec<u8>,
}

impl<T> AmCacheValue<T> {
    pub fn fixed(header: T) -> Self {
        Self {
            header,
            payload: Vec::new(),
        }
    }
}

#[repr(C)]
struct RawAmCache<T> {
    payload_len: usize,
    header: T,
}

/// Relation-lifetime-bound view of one typed AM cache allocation.
#[derive(Clone, Copy)]
pub struct AmCacheRef<'a, T> {
    raw: &'a RawAmCache<T>,
}

impl<'a, T> AmCacheRef<'a, T> {
    pub fn header(self) -> &'a T {
        &self.raw.header
    }

    pub fn str(self, handle: AmCacheString) -> &'a str {
        handle.resolve(self.payload())
    }

    fn payload(self) -> &'a [u8] {
        let start = (self.raw as *const RawAmCache<T>).wrapping_add(1);
        // SAFETY: `RawAmCache` and exactly `payload_len` following bytes are
        // allocated together by `load_and_cache`; the relation lifetime keeps
        // that allocation alive.
        unsafe {
            std::slice::from_raw_parts(start.cast::<u8>(), self.raw.payload_len)
        }
    }
}

/// Helper to manage `rd_amcache`.
pub struct AmCache;

impl AmCache {
    /// Get a typed reference to the cached data.
    ///
    /// If the cache is empty, it loads options from the catalog, parses them using `T::from_options`,
    /// allocates a single contiguous memory block via `palloc`, and updates `rd_amcache`.
    ///
    /// # Safety
    ///
    /// For a given relation and access method, every access to `rd_amcache` must
    /// use the same `T`. If the pointer is already non-null, PostgreSQL stores no
    /// runtime type information that can verify it was initialized as `T`.
    ///
    /// The caller must hold the relation lock for the returned lifetime and must
    /// not trigger relation-cache invalidation while the reference is live.
    pub unsafe fn get<'a, T: AmCacheable>(
        rel: &RelationHandle<'a>,
    ) -> Result<AmCacheRef<'a, T>, TableOptionError> {
        let rel_ptr = rel.as_raw();
        if rel_ptr.is_null() {
            return Err(TableOptionError::NullRelation);
        }

        // SAFETY: caller guarantees that a pre-existing cache has type `T`;
        // `rel_ptr` is non-null and belongs to the locked relation.
        unsafe {
            if !(*rel_ptr).rd_amcache.is_null() {
                let raw = &*((*rel_ptr).rd_amcache as *const RawAmCache<T>);
                return Ok(AmCacheRef { raw });
            }
            Self::load_and_cache::<T>(rel_ptr)
        }
    }

    #[cold]
    unsafe fn load_and_cache<'a, T: AmCacheable>(
        rel: pg_sys::Relation,
    ) -> Result<AmCacheRef<'a, T>, TableOptionError> {
        // SAFETY: rel is a valid, locked Relation obtained from RelationHandle.
        let opts = unsafe { TableOptions::load_from_catalog((*rel).rd_id)? };

        let AmCacheValue { header, payload } = T::from_options(opts.as_ref())?;
        let raw_size = std::mem::size_of::<RawAmCache<T>>();
        let total_size = raw_size.checked_add(payload.len()).ok_or_else(|| {
            TableOptionError::InvalidOption(
                "AM cache allocation size overflow".to_owned(),
            )
        })?;

        // SAFETY: palloc + writes within CacheMemoryContext; memory persists
        // as long as the Relation and is freed by Postgres on invalidation.
        // switch_to is unsafe because it changes the active memory context.
        let ptr = unsafe {
            PgMemoryContexts::CacheMemoryContext.switch_to(|_| {
                let ptr = pg_sys::palloc(total_size).cast::<RawAmCache<T>>();
                ptr.write(RawAmCache {
                    payload_len: payload.len(),
                    header,
                });
                if !payload.is_empty() {
                    std::ptr::copy_nonoverlapping(
                        payload.as_ptr(),
                        ptr.add(1).cast::<u8>(),
                        payload.len(),
                    );
                }
                ptr
            })
        };

        // SAFETY: rel is valid and writable while the lock is held.
        unsafe {
            (*rel).rd_amcache = ptr.cast();
            Ok(AmCacheRef { raw: &*ptr })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copied_string_handle_requires_and_uses_explicit_payload() {
        let mut builder = AmCacheValueBuilder::new();
        let handle = builder.push_str("arbitrary/value").unwrap();
        let copied = handle;
        let value = builder.finish(());

        assert_eq!(copied.resolve(&value.payload), "arbitrary/value");
    }
}
