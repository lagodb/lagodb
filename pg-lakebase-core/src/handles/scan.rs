use super::borrowed::PgBorrowed;
use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use std::fmt;

/// Safe wrapper for PostgreSQL TableScanDesc.
#[derive(Debug)]
pub struct TableScanDescHandle<'a> {
    inner: PgBorrowed<'a, pg_sys::TableScanDescData>,
}

impl<'a> TableScanDescHandle<'a> {
    /// # Safety
    ///
    /// `ptr` must be non-null and valid for `'a`.
    #[inline]
    pub unsafe fn from_raw(ptr: pg_sys::TableScanDesc) -> Self {
        Self {
            inner: unsafe { PgBorrowed::from_raw(ptr) },
        }
    }

    #[inline]
    pub fn as_raw(&self) -> pg_sys::TableScanDesc {
        self.inner.as_ptr()
    }
}

/// Owned copy of a PostgreSQL `ScanKeyData` array.
///
/// PostgreSQL passes scan keys to access-method callbacks as a borrowed
/// pointer that is only valid for the duration of the call. Because table
/// AMs frequently need keys at a later moment than the callback in which
/// they are received (notably, an Iceberg AM cannot translate a key into an
/// Iceberg `Predicate` until it has resolved the table's schema in
/// `scan_begin`), the `pg-lakebase-core` dispatcher copies the keys here
/// once and lends a stable reference to the AM.
///
/// This mirrors PostgreSQL heap-AM internals: `heap_beginscan` allocates the
/// `rs_key` buffer and `initscan` rewrites it with `memcpy` on every
/// `heap_rescan` whose `key != NULL`. `OwnedScanKeys::replace_with` is the
/// Rust equivalent: scan_rescan with a non-null key replaces the buffer
/// wholesale (PG's *replace*, not *merge*, semantics), while a null key
/// keeps the previous contents.
///
/// The `Vec` storage means an empty key set incurs no heap allocation,
/// which is the common case (plain `SeqScan` always passes `key=NULL`).
#[derive(Debug, Default, Clone)]
pub struct OwnedScanKeys {
    keys: Vec<pg_sys::ScanKeyData>,
}

impl OwnedScanKeys {
    /// An empty key set. No allocation.
    #[inline]
    pub fn empty() -> Self {
        Self { keys: Vec::new() }
    }

    /// Copy `nkeys` `ScanKeyData` values out of `ptr` into an owned buffer.
    ///
    /// Returns an empty set when `ptr` is null or `nkeys <= 0`.
    ///
    /// # Safety
    ///
    /// When `ptr` is non-null and `nkeys > 0`, `ptr` must point to a valid
    /// array of at least `nkeys` `ScanKeyData` entries. The caller is
    /// responsible for ensuring those entries have been initialized by
    /// PostgreSQL (e.g. by `ScanKeyInit` or by the executor) before this
    /// call.
    pub unsafe fn copy_from_raw(ptr: *const pg_sys::ScanKeyData, nkeys: i32) -> Self {
        if ptr.is_null() || nkeys <= 0 {
            return Self::empty();
        }
        let len = nkeys as usize;
        // ScanKeyData is `#[repr(C)] Copy`, so a slice copy is the
        // Rust-level equivalent of PostgreSQL's `memcpy(rs_key, key,
        // nkeys * sizeof(ScanKeyData))` in `initscan`.
        let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
        Self {
            keys: slice.to_vec(),
        }
    }

    /// Replace the contents of this buffer with a fresh copy of the keys at
    /// `ptr`. Equivalent to `*self = copy_from_raw(ptr, nkeys)` but reuses
    /// the existing allocation when capacity allows.
    ///
    /// When `ptr` is null or `nkeys <= 0` this clears the buffer.
    ///
    /// # Safety
    ///
    /// Same constraints as [`copy_from_raw`](Self::copy_from_raw).
    pub unsafe fn replace_with(
        &mut self,
        ptr: *const pg_sys::ScanKeyData,
        nkeys: i32,
    ) {
        self.keys.clear();
        if ptr.is_null() || nkeys <= 0 {
            return;
        }
        let len = nkeys as usize;
        let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
        self.keys.extend_from_slice(slice);
    }

    /// Number of keys in this set.
    #[inline]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether this set is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Pointer to publish through `TableScanDescData::rs_key`.
    ///
    /// Used by the `pg-lakebase-core` scan dispatcher to publish the
    /// AM-owned buffer through `TableScanDescData::rs_key`, mirroring the
    /// PostgreSQL heap-AM contract that `rs_key` always points at the AM's
    /// own buffer (and never at the caller's borrowed pointer).
    ///
    /// Returns `null_mut()` when the buffer is empty so that the
    /// descriptor matches heap-AM convention (`rs_key == NULL` when
    /// `rs_nkeys == 0`); `Vec::as_mut_ptr()` would otherwise return a
    /// non-null `NonNull::dangling()` sentinel.
    ///
    /// The pointer is only valid while this `OwnedScanKeys` is not
    /// modified in a way that may reallocate (e.g. `replace_with` with a
    /// larger length); after such a call it must be re-fetched. Visibility
    /// is intentionally limited to the crate: AMs interact with the keys
    /// through the safe `iter()` / [`ScanKeyEntry`] surface and should
    /// never need a raw mutable pointer.
    #[inline]
    pub(crate) fn rs_key_ptr(&mut self) -> *mut pg_sys::ScanKeyData {
        if self.keys.is_empty() {
            std::ptr::null_mut()
        } else {
            self.keys.as_mut_ptr()
        }
    }

    /// Iterate over the keys as safe `ScanKeyEntry` views.
    #[inline]
    pub fn iter(&self) -> ScanKeyIter<'_> {
        ScanKeyIter {
            inner: self.keys.iter(),
        }
    }
}

impl<'a> IntoIterator for &'a OwnedScanKeys {
    type Item = ScanKeyEntry<'a>;
    type IntoIter = ScanKeyIter<'a>;
    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator over [`OwnedScanKeys`].
pub struct ScanKeyIter<'a> {
    inner: std::slice::Iter<'a, pg_sys::ScanKeyData>,
}

impl<'a> Iterator for ScanKeyIter<'a> {
    type Item = ScanKeyEntry<'a>;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|raw| ScanKeyEntry { raw })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ExactSizeIterator for ScanKeyIter<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.inner.len()
    }
}

/// Borrowed view over a single PostgreSQL `ScanKeyData`.
///
/// Provides typed accessors for the fields a table AM needs to translate
/// the key into its native predicate language. Reading a `Datum` argument
/// still requires the AM to interpret it through the appropriate
/// `pgrx::datum::*` conversion based on the column type, since `Datum` is a
/// union; this type does not attempt that interpretation.
#[derive(Debug, Clone, Copy)]
pub struct ScanKeyEntry<'a> {
    raw: &'a pg_sys::ScanKeyData,
}

impl<'a> ScanKeyEntry<'a> {
    /// Bitmap of `SK_*` flags (see PostgreSQL `skey.h`).
    #[inline]
    pub fn flags(&self) -> i32 {
        self.raw.sk_flags
    }

    /// 1-based attribute number into the heap tuple, or one of the
    /// `InvalidAttrNumber`-like sentinels for non-column keys (e.g. row
    /// comparison headers). Match the heap AM's behavior and skip entries
    /// whose attno is not a real column.
    #[inline]
    pub fn attno(&self) -> pg_sys::AttrNumber {
        self.raw.sk_attno
    }

    /// Strategy number (`BT*StrategyNumber`, `RT*StrategyNumber`, etc.).
    #[inline]
    pub fn strategy(&self) -> pg_sys::StrategyNumber {
        self.raw.sk_strategy
    }

    /// Subtype OID of the right-hand side of the comparison, or
    /// `InvalidOid` when not relevant.
    #[inline]
    pub fn subtype(&self) -> pg_sys::Oid {
        self.raw.sk_subtype
    }

    /// Collation OID for collatable types.
    #[inline]
    pub fn collation(&self) -> pg_sys::Oid {
        self.raw.sk_collation
    }

    /// Right-hand-side argument as a raw `Datum`. The AM is responsible for
    /// interpreting this value according to the column's type.
    #[inline]
    pub fn argument(&self) -> pg_sys::Datum {
        self.raw.sk_argument
    }

    /// Escape hatch: borrow the underlying `ScanKeyData` for callers that
    /// need to inspect fields not surfaced by this wrapper (for example,
    /// `sk_func` for `SK_BT_DESC`-style operator family lookups).
    #[inline]
    pub fn as_raw(&self) -> &'a pg_sys::ScanKeyData {
        self.raw
    }
}

/// Safe wrapper for PostgreSQL TBMIterateResult.
#[derive(Debug)]
pub struct TBMIterateResultHandle<'a> {
    inner: PgBorrowed<'a, pg_sys::TBMIterateResult>,
}

impl<'a> TBMIterateResultHandle<'a> {
    /// # Safety
    ///
    /// `ptr` must be non-null and valid for `'a`.
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::TBMIterateResult) -> Self {
        Self {
            inner: unsafe { PgBorrowed::from_raw(ptr) },
        }
    }

    #[inline]
    pub fn as_raw(&self) -> *mut pg_sys::TBMIterateResult {
        self.inner.as_ptr()
    }
}

/// Safe wrapper for PostgreSQL SampleScanState.
#[derive(Debug)]
pub struct SampleScanStateHandle<'a> {
    inner: PgBorrowed<'a, pg_sys::SampleScanState>,
}

impl<'a> SampleScanStateHandle<'a> {
    /// # Safety
    ///
    /// `ptr` must be non-null and valid for `'a`.
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::SampleScanState) -> Self {
        Self {
            inner: unsafe { PgBorrowed::from_raw(ptr) },
        }
    }

    #[inline]
    pub fn as_raw(&self) -> *mut pg_sys::SampleScanState {
        self.inner.as_ptr()
    }
}

/// Safe wrapper for PostgreSQL ReadStream.
#[derive(Debug)]
pub struct ReadStreamHandle<'a> {
    inner: PgBorrowed<'a, pg_sys::ReadStream>,
}

impl<'a> ReadStreamHandle<'a> {
    /// # Safety
    ///
    /// `ptr` must be non-null and valid for `'a`.
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::ReadStream) -> Self {
        Self {
            inner: unsafe { PgBorrowed::from_raw(ptr) },
        }
    }

    #[inline]
    pub fn as_raw(&self) -> *mut pg_sys::ReadStream {
        self.inner.as_ptr()
    }
}

/// Safe wrapper for PostgreSQL ParallelTableScanDesc.
#[derive(Debug)]
pub struct ParallelTableScanDescHandle<'a> {
    inner: PgBorrowed<'a, pg_sys::ParallelTableScanDescData>,
}

impl<'a> ParallelTableScanDescHandle<'a> {
    /// # Safety
    ///
    /// `ptr` must be non-null and valid for `'a`.
    #[inline]
    pub unsafe fn from_raw(ptr: pg_sys::ParallelTableScanDesc) -> Self {
        Self {
            inner: unsafe { PgBorrowed::from_raw(ptr) },
        }
    }

    #[inline]
    pub fn as_raw(&self) -> pg_sys::ParallelTableScanDesc {
        self.inner.as_ptr()
    }
}

/// Safe wrapper for PostgreSQL ScanDirection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Backward,
    NoMovement,
}

/// Raw PostgreSQL scan direction value outside the supported enum set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidScanDirection {
    raw: pg_sys::ScanDirection::Type,
}

impl InvalidScanDirection {
    #[inline]
    pub fn raw(&self) -> pg_sys::ScanDirection::Type {
        self.raw
    }
}

impl fmt::Display for InvalidScanDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid PostgreSQL ScanDirection: {}", self.raw)
    }
}

impl std::error::Error for InvalidScanDirection {}

impl From<InvalidScanDirection> for ErrorReport {
    fn from(value: InvalidScanDirection) -> Self {
        ErrorReport::new(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            value.to_string(),
            "",
        )
    }
}

impl ScanDirection {
    #[inline]
    pub fn try_from_raw(
        direction: pg_sys::ScanDirection::Type,
    ) -> Result<Self, InvalidScanDirection> {
        match direction {
            pg_sys::ScanDirection::ForwardScanDirection => Ok(ScanDirection::Forward),
            pg_sys::ScanDirection::BackwardScanDirection => {
                Ok(ScanDirection::Backward)
            }
            pg_sys::ScanDirection::NoMovementScanDirection => {
                Ok(ScanDirection::NoMovement)
            }
            raw => Err(InvalidScanDirection { raw }),
        }
    }

    #[inline]
    pub fn to_raw(&self) -> pg_sys::ScanDirection::Type {
        match self {
            ScanDirection::Forward => pg_sys::ScanDirection::ForwardScanDirection,
            ScanDirection::Backward => pg_sys::ScanDirection::BackwardScanDirection,
            ScanDirection::NoMovement => {
                pg_sys::ScanDirection::NoMovementScanDirection
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_key(attno: pg_sys::AttrNumber, strategy: u16) -> pg_sys::ScanKeyData {
        pg_sys::ScanKeyData {
            sk_attno: attno,
            sk_strategy: strategy,
            ..pg_sys::ScanKeyData::default()
        }
    }

    #[test]
    fn empty_owns_no_allocation() {
        let keys = OwnedScanKeys::empty();
        assert!(keys.is_empty());
        assert_eq!(keys.len(), 0);
        assert_eq!(keys.iter().count(), 0);
    }

    #[test]
    fn copy_from_raw_null_or_zero_yields_empty() {
        let keys = unsafe { OwnedScanKeys::copy_from_raw(std::ptr::null(), 0) };
        assert!(keys.is_empty());

        let keys = unsafe { OwnedScanKeys::copy_from_raw(std::ptr::null(), 5) };
        assert!(keys.is_empty());

        let buf = [make_key(1, 3)];
        let keys = unsafe { OwnedScanKeys::copy_from_raw(buf.as_ptr(), 0) };
        assert!(keys.is_empty());
    }

    #[test]
    fn copy_from_raw_copies_values() {
        let buf = [make_key(2, 5), make_key(7, 1)];
        let keys = unsafe { OwnedScanKeys::copy_from_raw(buf.as_ptr(), 2) };
        assert_eq!(keys.len(), 2);
        let collected: Vec<_> =
            keys.iter().map(|e| (e.attno(), e.strategy())).collect();
        assert_eq!(collected, vec![(2, 5), (7, 1)]);
    }

    #[test]
    fn replace_with_overwrites_buffer() {
        let buf1 = [make_key(1, 1), make_key(2, 2), make_key(3, 3)];
        let mut keys = unsafe { OwnedScanKeys::copy_from_raw(buf1.as_ptr(), 3) };
        assert_eq!(keys.len(), 3);

        let buf2 = [make_key(9, 9)];
        unsafe { keys.replace_with(buf2.as_ptr(), 1) };
        assert_eq!(keys.len(), 1);
        let only = keys.iter().next().unwrap();
        assert_eq!(only.attno(), 9);
        assert_eq!(only.strategy(), 9);
    }

    #[test]
    fn replace_with_null_clears() {
        let buf = [make_key(4, 4)];
        let mut keys = unsafe { OwnedScanKeys::copy_from_raw(buf.as_ptr(), 1) };
        assert_eq!(keys.len(), 1);

        unsafe { keys.replace_with(std::ptr::null(), 0) };
        assert!(keys.is_empty());
    }
}
