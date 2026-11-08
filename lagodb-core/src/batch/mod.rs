//! Batch buffering abstractions for the mutation write path.
//!
//! The core crate defines the buffering *contract* without committing to a
//! concrete storage layout or naming a columnar library — the Arrow
//! implementations live in `lagodb-arrow`. core stays Arrow-agnostic and the
//! per-row path stays `dyn`-free.
//!
//! A provider-owned relation-bound writer consumes `TupleSlotRow` views
//! *during* the callback and copies what it needs straight into its column
//! builders, so no owned row outlives the callback. `lagodb-arrow` provides the
//! `BoundWriteBuffer` implementation, which validates source codecs during
//! planning, and `BoundDatumBuffer` for slot-free datum streams. Core does not
//! provide a `Cell`-based columnar buffer: on a mutation hot path, materializing
//! `Cell` first would add the intermediate allocations the slot/datum path is
//! designed to avoid.
//!
//! The read path has no core contract: the provider that produces column
//! batches also owns the slot layout its decoder writes.

mod write;

pub use write::BatchBuffer;
