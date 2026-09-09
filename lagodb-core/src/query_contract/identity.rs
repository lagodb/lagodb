//! Dense query identities and stable table-scan routes.

use std::ffi::CStr;

/// PostgreSQL catalog class that owns a table-scan source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TableScanRouteKind {
    AccessMethod,
    ForeignDataWrapper,
}

impl TableScanRouteKind {
    pub const ACCESS_METHOD_CODE: i32 = 1;
    pub const FOREIGN_DATA_WRAPPER_CODE: i32 = 2;

    #[inline]
    pub const fn code(self) -> i32 {
        match self {
            Self::AccessMethod => Self::ACCESS_METHOD_CODE,
            Self::ForeignDataWrapper => Self::FOREIGN_DATA_WRAPPER_CODE,
        }
    }

    #[inline]
    pub const fn from_code(code: i32) -> Option<Self> {
        match code {
            Self::ACCESS_METHOD_CODE => Some(Self::AccessMethod),
            Self::FOREIGN_DATA_WRAPPER_CODE => Some(Self::ForeignDataWrapper),
            _ => None,
        }
    }
}

/// Stable identity of the PostgreSQL storage object routed to a table scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableScanRoute<'a> {
    kind: TableScanRouteKind,
    name: &'a CStr,
}

impl<'a> TableScanRoute<'a> {
    #[inline]
    pub const fn access_method(name: &'a CStr) -> Self {
        Self {
            kind: TableScanRouteKind::AccessMethod,
            name,
        }
    }

    #[inline]
    pub const fn foreign_data_wrapper(name: &'a CStr) -> Self {
        Self {
            kind: TableScanRouteKind::ForeignDataWrapper,
            name,
        }
    }

    #[inline]
    pub const fn new(kind: TableScanRouteKind, name: &'a CStr) -> Self {
        Self { kind, name }
    }

    #[inline]
    pub const fn kind(self) -> TableScanRouteKind {
        self.kind
    }

    #[inline]
    pub const fn name(self) -> &'a CStr {
        self.name
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
