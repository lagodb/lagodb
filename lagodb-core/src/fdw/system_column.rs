//! PostgreSQL system-column ownership at the FDW boundary.

use pgrx::pg_sys;

/// Classifies a system column by the component that can produce its value.
///
/// `tableoid` is filled by PostgreSQL after a foreign callback returns.  The
/// provider must return `ctid` when the plan needs physical tuple identity;
/// other system columns are not part of this framework's provider contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SystemColumnRequirement {
    CoreSynthesizedTableOid,
    ProviderReturnedItemPointer,
    UnsupportedHeaderField(pg_sys::AttrNumber),
}

impl SystemColumnRequirement {
    #[inline]
    pub(crate) const fn from_attno(attno: pg_sys::AttrNumber) -> Self {
        if attno == pg_sys::TableOidAttributeNumber as pg_sys::AttrNumber {
            Self::CoreSynthesizedTableOid
        } else if attno
            == pg_sys::SelfItemPointerAttributeNumber as pg_sys::AttrNumber
        {
            Self::ProviderReturnedItemPointer
        } else {
            Self::UnsupportedHeaderField(attno)
        }
    }

    #[inline]
    pub(crate) const fn is_unsupported(self) -> bool {
        matches!(self, Self::UnsupportedHeaderField(_))
    }

    #[inline]
    pub(crate) const fn requires_item_pointer(self) -> bool {
        matches!(self, Self::ProviderReturnedItemPointer)
    }
}
