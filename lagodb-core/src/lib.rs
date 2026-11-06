//! lagodb-core: Rust framework for PostgreSQL table access methods and FDWs
//!
//! This library provides safe lifecycle adapters for implementing custom table
//! access methods and foreign data wrappers for PostgreSQL using pgrx.
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use lagodb_core::prelude::*;
//!
//! #[pg_table_am(
//!     version = "0.1.0",
//!     author = "Your Name",
//!     website = "https://github.com/your/repo"
//! )]
//! pub struct MyTableAm;
//!
//! impl TableAccessMethod for MyTableAm {
//!     type ScanSession = MyScan;
//!     type IndexFetchSession = MyIndexFetch;
//!     type ModifyQueryState = MyModifyQueryState;
//!     type ModifyState = MyModify;
//!     type CopySession = MyCopy;
//!
//!     fn access_method_oid() -> Option<pg_sys::Oid> {
//!         let name = c"my_table_am";
//!         let oid = unsafe { pg_sys::get_table_am_oid(name.as_ptr(), true) };
//!         (oid != pg_sys::InvalidOid).then_some(oid)
//!     }
//! }
//! ```
//!
//! # Unsafe boundary
//!
//! PostgreSQL exposes backend-owned objects as raw C pointers, so `unsafe`
//! is not eliminated; it is kept at the domain that owns each object. Raw
//! pointer operations stay crate-private next to the guard or handle type that
//! encodes their ownership and lifetime (`RelationGuard`, `CatalogRelation`,
//! `CatalogScan`, `SysCacheTuple`, ...). A public `unsafe fn` is acceptable
//! only when callers must uphold PostgreSQL invariants the type system cannot
//! express.

// Foundation: PostgreSQL errors, FFI handles, values, rows, and expressions.
pub mod catalog;
pub mod datum;
pub mod diag;
pub mod expr;
pub mod handles;
pub mod plan_data;
pub mod tuple;

// Backend runtime: process, transaction, ResourceOwner, WAL, and runtime ABI.
pub mod backend;
pub mod resource;
pub mod runtime_api;
pub mod transaction;
pub mod wal;

// Provider seams: table AM, FDW, CustomScan, COPY, and provider registration.
pub mod batch;
pub mod copy;
pub mod customscan;
pub mod fdw;
pub mod parallel_scan;
pub mod query_contract;
mod scan_explain;
pub mod table_am;
pub mod table_provider;

// Cross-command services built on the seams.
pub mod extension_worker;
pub mod hooks;
pub(crate) mod maintenance_config;
pub mod object_cleanup;
pub mod options;
pub mod storage;
pub mod table_maintenance;

/// The prelude includes all necessary imports to make lagodb_core work
pub mod prelude {
    pub use crate::batch::*;
    pub use crate::datum::*;
    pub use crate::diag::{
        PgError, PgErrorReport, PgErrorSource, PgReportError, ReportableError,
        SqlStateError,
    };
    pub use crate::handles::*;
    pub use crate::table_am::*;
    pub use crate::tuple::{
        PgDatumRef, Row, RowDatumCodec, RowSlotWriter, SlotDatumIndex, SlotDatumView,
        SlotDatumWriter, TupleSlotBatch, TupleSlotRow,
    };
    pub use crate::{pg_fdw, pg_table_am};
}

use pgrx::AllocatedByPostgres;
use pgrx::prelude::*;

/// PgBox'ed `TableAmRoutine`, returned by
/// [`am_routine`](table_am::TableAccessMethod::am_routine).
pub type TableAmRoutine<A = AllocatedByPostgres> = PgBox<pg_sys::TableAmRoutine, A>;

/// Procedural macro for generating table access method boilerplate
pub use lagodb_macros::{pg_fdw, pg_table_am};
