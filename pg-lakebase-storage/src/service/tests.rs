//! Service-level integration tests, split by cache surface.
//!
//! * [`fixtures`]        — shared helpers, constants, and re-exports of test doubles.
//! * [`test_doubles`]    — hand-written test doubles (blocking backends, counting index).
//! * [`direct_io`]       — `CompleteFile` direct-IO open/read path and associated invalidation.
//! * [`large_objects`]   — large-fill lease lifecycle, aborts, partial-file semantics.
//! * [`limits`]          — open-handle capacity enforcement.
//! * [`registry`]        — routing opens to named backends through [`crate::backend::StoreRegistry`].
//! * [`small_objects`]   — `SmallKV` open/read/invalidate semantics.

mod fixtures;
mod test_doubles;

mod direct_io;
mod establish_single_flight;
mod kv_contract;
mod large_objects;
mod limits;
mod registry;
mod small_objects;
