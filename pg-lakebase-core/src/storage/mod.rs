//! Storage-facing APIs grouped by ownership boundary.
//!
//! [`service`] exposes the runtime-owned storage service to consumers, while
//! [`volume`] contains catalog and routing types for configured storage
//! volumes. The two domains share a parent namespace but do not share
//! implementation state.

pub mod foreign;
pub mod service;
pub mod volume;
