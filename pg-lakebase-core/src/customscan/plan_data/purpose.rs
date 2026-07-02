//! Stable semantic purpose of a provider base-scan CustomScan.

/// Why the planner created a provider CustomScan.
///
/// `Modify` is not a separate scan implementation. It selects the extended
/// row-identity tuple layout and the outer-node binding lifecycle while
/// retaining the provider's ordinary planning and execution machinery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScanPurpose {
    Query,
    Modify,
}

impl ScanPurpose {
    pub(crate) const QUERY_WIRE: i32 = 0;
    pub(crate) const MODIFY_WIRE: i32 = 1;

    pub(crate) const fn to_wire(self) -> i32 {
        match self {
            Self::Query => Self::QUERY_WIRE,
            Self::Modify => Self::MODIFY_WIRE,
        }
    }

    pub(crate) const fn from_wire(value: i32) -> Option<Self> {
        match value {
            Self::QUERY_WIRE => Some(Self::Query),
            Self::MODIFY_WIRE => Some(Self::Modify),
            _ => None,
        }
    }

    pub const fn label(self) -> &'static core::ffi::CStr {
        match self {
            Self::Query => c"Query",
            Self::Modify => c"Modify",
        }
    }

    pub const fn is_modify(self) -> bool {
        matches!(self, Self::Modify)
    }
}
