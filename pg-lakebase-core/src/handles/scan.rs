use super::borrowed::PgBorrowed;
use pgrx::pg_sys;

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

/// Safe wrapper for PostgreSQL ScanKey.
#[derive(Debug)]
pub struct ScanKeyHandle<'a> {
    inner: PgBorrowed<'a, pg_sys::ScanKeyData>,
    nkeys: i32,
}

impl<'a> ScanKeyHandle<'a> {
    /// # Safety
    ///
    /// `ptr` must be non-null and valid for `nkeys` entries for `'a`.
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::ScanKeyData, nkeys: i32) -> Self {
        Self {
            inner: unsafe { PgBorrowed::from_raw(ptr) },
            nkeys,
        }
    }

    #[inline]
    pub fn as_raw(&self) -> *mut pg_sys::ScanKeyData {
        self.inner.as_ptr()
    }

    #[inline]
    pub fn nkeys(&self) -> i32 {
        self.nkeys
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

impl ScanDirection {
    #[inline]
    pub fn from_raw(direction: pg_sys::ScanDirection::Type) -> Self {
        match direction {
            pg_sys::ScanDirection::ForwardScanDirection => ScanDirection::Forward,
            pg_sys::ScanDirection::BackwardScanDirection => ScanDirection::Backward,
            pg_sys::ScanDirection::NoMovementScanDirection => {
                ScanDirection::NoMovement
            }
            _ => panic!("invalid PostgreSQL ScanDirection: {direction}"),
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
