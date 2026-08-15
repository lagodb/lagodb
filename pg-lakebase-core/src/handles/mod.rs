//! Safe wrapper types for PostgreSQL FFI types.
//!
//! PostgreSQL access-method callbacks expose raw C pointers. This module keeps
//! those raw hook types out of access-method trait implementations by presenting
//! typed Rust handles at the trait boundary.
//!
//! The public surface follows these ownership categories:
//!
//! - Borrowed PostgreSQL-owned objects use handle structs backed by
//!   `PgBorrowed` or `PgNullable`. These handles validate nullability and bind a
//!   Rust lifetime to the PostgreSQL object, but they do not claim Rust-level
//!   exclusive access. `RelationHandle`, `SnapshotHandle`, and
//!   `BulkInsertStateHandle` are examples.
//! - Opaque pass-through state may expose a raw mutable pointer when PostgreSQL
//!   APIs require one, but AM trait methods should borrow the handle immutably
//!   unless the Rust API itself provides mutable access to the object.
//! - Callable handles wrap PostgreSQL function pointers plus a lifetime marker.
//!   Function pointers are not object borrows, so `PgBorrowed` does not apply.
//!   `IndexBuildCallbackHandle` is an example.
//! - Exclusive mutable handles are reserved for callback-owned output or state
//!   objects that the AM is expected to update directly. These handles store
//!   `&mut T` or `&mut [T]`, and trait methods receive `&mut Handle`.
//!   `TMIndexDeleteOpHandle` and `AttrWidthsHandle` are examples.
//! - Owning guards manage resources this crate must close or free, such as
//!   `RelationGuard` and `HeapTupleGuard`.

mod borrowed;
mod index;
mod mutation;
mod relation;
mod relation_column;
mod scan;
mod tuple;

pub use index::{
    CallbackStateHandle, IndexBuildCallbackHandle, IndexInfoHandle,
    ValidateIndexStateHandle,
};
pub use mutation::{BulkInsertStateHandle, TM_FailureData, TMIndexDeleteOpHandle};
pub use relation::{
    AttrWidthsHandle, BufferAccessStrategyHandle, RelFileLocator, RelationGuard,
    RelationHandle, SnapshotHandle, VacuumParamsHandle, VarlenaHandle,
};
pub use relation_column::RelationColumn;
pub use scan::AnalyzeSamplerState;
pub use scan::{
    AnalyzeReadStreamHandle, OwnedScanKeys, ParallelTableScanDescHandle,
    SampleScanStateHandle, ScanDirection, ScanKeyEntry, ScanKeyIter,
    TBMIterateResultHandle, TableScanDescHandle,
};
pub use tuple::{
    HeapTupleGuard, HeapTupleRef, ItemPointer, TupleTableSlotHandle, ValidItemPointer,
};
