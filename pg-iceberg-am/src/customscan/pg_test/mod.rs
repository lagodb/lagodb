//! pgrx backend tests for Iceberg customscan.
//!
//! The suite is partitioned by test concern rather than by production file:
//!
//! - [`support`]: reusable PG-node fixtures and observation harnesses
//! - [`predicate`]: backend-only classifier / translator pipeline coverage
//! - [`decode`]: backend datum-decoding paths that cannot run in host `#[test]`
//! - [`scan`]: end-to-end CustomScan execution over a live Iceberg table

mod decode;
mod predicate;
mod scan;
pub(crate) mod support;
