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

/// Identity of one source instance inside a query fragment.
///
/// The identity is fragment-local and zero-based. It identifies a source
/// instance rather than a relation OID, so two leaves of a self join receive
/// distinct values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SourceId(usize);

impl SourceId {
    #[inline]
    pub const fn from_index(index: usize) -> Self {
        Self(index)
    }

    #[inline]
    pub const fn index(self) -> usize {
        self.0
    }

    /// Reconstruct an identity after the containing source table has been
    /// decoded and its length is known.
    #[inline]
    pub const fn from_plan_data(index: usize, source_count: usize) -> Option<Self> {
        if index < source_count {
            Some(Self(index))
        } else {
            None
        }
    }
}
