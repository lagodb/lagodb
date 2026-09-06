//! Dense identities shared across query runtime boundaries.

/// Backend-lifetime identity of one registered provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProviderId(usize);

impl ProviderId {
    #[inline]
    pub const fn from_index(index: usize) -> Self {
        Self(index)
    }

    #[inline]
    pub const fn index(self) -> usize {
        self.0
    }
}

/// Identity of one table-scan instance inside a query fragment.
///
/// The identity is fragment-local and zero-based. It identifies a table scan
/// instance rather than a relation OID, so two leaves of a self join receive
/// distinct values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ScanId(usize);

impl ScanId {
    #[inline]
    pub const fn from_index(index: usize) -> Self {
        Self(index)
    }

    #[inline]
    pub const fn index(self) -> usize {
        self.0
    }
}

/// Identity of one value produced inside a query fragment.
///
/// Like [`ScanId`], this is semantic identity rather than a PostgreSQL target
/// list position.  The physical slot mapping is owned by the query tuple
/// layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OutputId(usize);

impl OutputId {
    #[inline]
    pub const fn from_index(index: usize) -> Self {
        Self(index)
    }

    #[inline]
    pub const fn index(self) -> usize {
        self.0
    }
}
