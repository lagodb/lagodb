//! Exhaustive identities for native expression shapes.

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
