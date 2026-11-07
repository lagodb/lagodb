//! Batch buffering and transport abstractions for the mutation write path and the
//! columnar scan read path.
//!
//! The core crate defines the buffering/transport *contracts* without
//! committing to a concrete storage layout or naming a columnar library — the
//! Arrow implementations live in `lagodb-arrow`. core stays Arrow-agnostic and
//! the per-row path stays `dyn`-free (generics + enum dispatch), so the
//! abstraction is zero-cost.
//!
//! Write path: a provider-owned relation-bound writer consumes `TupleSlotRow`
//! views *during* the callback and copies what it needs straight into its
//! column builders, so no owned row outlives the callback. `lagodb-arrow`
//! provides the `BoundWriteBuffer` implementation, which validates source
//! codecs during planning, and `BoundDatumBuffer` for slot-free datum streams.
//! Core does not provide a `Cell`-based columnar buffer: on a mutation hot path,
//! materializing `Cell` first would add the intermediate allocations the
//! slot/datum path is designed to avoid.
//!
//! Read path: an [`AmScanBatchSource`] yields AM-defined column
//! batches and a [`BatchRowDecoder`] writes one batch row into a slot, paired by
//! the core-provided [`BatchRowCursor`]. Their Arrow implementations
//! (`ArrowBatchSource` / `ArrowColumnDecoder`) also live in `lagodb-arrow`.

mod read;
mod write;

pub use read::{AmScanBatchSource, BatchRowCursor, BatchRowDecoder, ScanBatchDriver};
pub use write::BatchBuffer;
