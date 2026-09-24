//! Stable semantic purpose of a provider base-scan CustomScan.

/// Why the planner created a provider CustomScan.
///
/// `ModifyTarget` is not a separate scan implementation. It selects the extended
/// row-identity tuple layout and the outer-node binding lifecycle while
/// retaining the provider's ordinary planning and execution machinery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScanPurpose {
    Read,
    ModifyTarget,
}

impl ScanPurpose {
    pub(crate) const READ_WIRE: i32 = 0;
    pub(crate) const MODIFY_TARGET_WIRE: i32 = 1;

    pub(crate) const fn to_wire(self) -> i32 {
        match self {
            Self::Read => Self::READ_WIRE,
            Self::ModifyTarget => Self::MODIFY_TARGET_WIRE,
        }
    }

    pub(crate) const fn from_wire(value: i32) -> Option<Self> {
        match value {
            Self::READ_WIRE => Some(Self::Read),
            Self::MODIFY_TARGET_WIRE => Some(Self::ModifyTarget),
            _ => None,
        }
    }

    pub const fn label(self) -> &'static core::ffi::CStr {
        match self {
            Self::Read => c"Read",
            Self::ModifyTarget => c"ModifyTarget",
        }
    }

    pub const fn is_modify_target(self) -> bool {
        matches!(self, Self::ModifyTarget)
    }
}
