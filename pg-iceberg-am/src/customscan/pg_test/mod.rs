//! pgrx backend tests for Iceberg customscan.
//!
//! The suite is partitioned by test concern rather than by production file:
//!
//! - [`support`]: reusable PG-node fixtures and observation harnesses
//! - [`predicate`]: backend-only classifier / translator pipeline coverage
//! - [`custom_private`]: Iceberg `custom_private` payload and envelope behavior
//! - [`decode`]: backend datum-decoding paths that cannot run in host `#[test]`

mod custom_private;
mod decode;
mod predicate;
pub(crate) mod support;
