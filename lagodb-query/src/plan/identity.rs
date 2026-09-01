//! Dense identities owned by the semantic query plan.

/// Identity of one result produced by a query fragment.
///
/// Output identities are dense and zero-based. Physical PostgreSQL slot
/// positions are derived from the query tuple layout rather than being stored
/// as raw `resno` values.
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

    /// Reconstruct an identity after the containing output table has been
    /// decoded and its length is known.
    #[inline]
    pub const fn from_plan_data(index: usize, output_count: usize) -> Option<Self> {
        if index < output_count {
            Some(Self(index))
        } else {
            None
        }
    }
}
