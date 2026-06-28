//! Plan-stage expression inspection, normalization, classification, and split.

pub(crate) mod inspect;
pub mod predicate;
pub(crate) mod relation;
pub(crate) mod rewrite;
pub mod split;
pub mod walker;
