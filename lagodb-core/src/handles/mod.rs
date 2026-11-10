//! Scoped PostgreSQL FFI handles and owning guards.
//!
//! PostgreSQL access-method callbacks expose raw C pointers. This module keeps
//! those raw hook types out of access-method trait implementations by presenting
//! typed Rust handles at the trait boundary.
//!
//! The public surface follows these ownership categories:
//!
//! - Borrowed PostgreSQL-owned objects use handle structs backed by
//!   `PgBorrowed` or `PgNullable`. These handles bind a Rust lifetime to the
//!   PostgreSQL object, but they do not claim Rust-level exclusive access.
//!   `RelationHandle` and `SnapshotHandle` are examples.
//!   Explicit `as_raw` interop methods remain escape hatches: once copied out,
//!   a raw pointer no longer carries the handle's callback-scoped lifetime.
//! - Opaque pass-through state may expose a raw mutable pointer when PostgreSQL
//!   APIs require one, but AM trait methods should borrow the handle immutably
//!   unless the Rust API itself provides mutable access to the object.
//! - Callback handles bind a PostgreSQL function pointer to its callback-scoped
//!   relation identity and opaque state. `IndexBuildCallbackHandle` is an
//!   example.
//! - Exclusive mutable handles are reserved for callback-owned output or state
//!   objects that the AM is expected to update directly. These handles store
//!   `&mut T` or `&mut [T]`, and trait methods receive `&mut Handle`.
//!   `TMIndexDeleteOpHandle` and `AttrWidthsHandle` are examples.
//! - Owning guards manage resources this crate must close or free, such as
//!   `RelationGuard` and `HeapTupleGuard`.

#[cfg(any(feature = "pg17", feature = "pg18"))]
mod analyze_stream;
#[cfg(feature = "pg18")]
mod bitmap;
mod borrowed;
mod executor;
mod index;
mod mutation;
mod partition_key;
mod relation;
#[cfg(any(feature = "pg15", feature = "pg16"))]
mod relation_analyze;
mod relation_column;
mod relation_guard;
mod relation_name;
mod relation_storage;
mod relation_tablespace;
mod scan;
mod syscache;
mod tuple;
mod tuple_desc;

#[cfg(any(feature = "pg17", feature = "pg18"))]
pub use analyze_stream::{AnalyzeReadStreamHandle, AnalyzeTickets};
#[cfg(feature = "pg18")]
pub use bitmap::{
    BitmapPage, BitmapPageOffsets, BitmapScanOutputHandle, TBMIteratorHandle,
};
pub use executor::ExecutorHandle;
pub use index::{
    IndexBuildCallbackHandle, IndexInfoHandle, ValidateIndexStateHandle,
};
pub use mutation::{TM_FailureData, TMIndexDeleteOpHandle};
pub use partition_key::{
    PartitionKey, PartitionKeyField, PartitionKeySource, PartitionStrategy,
};
pub use relation::{
    AttrWidthsHandle, BufferAccessStrategyHandle, PgRelFileLocator, RelFileLocator,
    RelationHandle, SnapshotHandle, VacuumParamsHandle, VarlenaHandle,
};
pub use relation_column::RelationColumn;
pub use relation_guard::RelationGuard;
pub use relation_name::RelationName;
pub use relation_tablespace::RelationTablespace;
pub use scan::{
    OwnedScanKeys, ParallelTableScanDescHandle, SampleScanStateHandle, ScanDirection,
    ScanKeyEntry, ScanKeyIter, TBMIterateResultHandle, TableScanDescHandle,
};
pub use syscache::{
    SysCacheTuple, SysCacheTupleCopy, search_syscache_copy, search_syscache1,
};
pub use tuple::{
    HeapTupleGuard, HeapTupleRef, ItemPointer, TupleTableSlotHandle, ValidItemPointer,
};
pub use tuple_desc::TupleDescHandle;
