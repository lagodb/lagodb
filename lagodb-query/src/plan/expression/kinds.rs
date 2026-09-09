//! Exhaustive identities for native expression shapes.

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BooleanTestKind {
    IsTrue,
    IsNotTrue,
    IsFalse,
    IsNotFalse,
    IsUnknown,
    IsNotUnknown,
}

impl BooleanTestKind {
    pub(crate) const fn wire_id(self) -> i32 {
        self as i32 + 1
    }

    pub(crate) const fn from_wire_id(id: i32) -> Option<Self> {
        Some(match id {
            1 => Self::IsTrue,
            2 => Self::IsNotTrue,
            3 => Self::IsFalse,
            4 => Self::IsNotFalse,
            5 => Self::IsUnknown,
            6 => Self::IsNotUnknown,
            _ => return None,
        })
    }
}

impl fmt::Display for BooleanTestKind {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(match self {
            Self::IsTrue => "TRUE",
            Self::IsNotTrue => "NOT TRUE",
            Self::IsFalse => "FALSE",
            Self::IsNotFalse => "NOT FALSE",
            Self::IsUnknown => "UNKNOWN",
            Self::IsNotUnknown => "NOT UNKNOWN",
        })
    }
}
