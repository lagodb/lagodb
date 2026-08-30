#![allow(clippy::too_many_arguments)]

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
//! }
//! ```

/// Core trait definitions for table access methods
pub mod api;

/// Mutation batch buffering abstractions and default row buffer.
pub mod batch;

/// Scoped PostgreSQL FFI handles and owning guards
pub mod handles;

/// PostgreSQL tuple value abstractions (Cell, Row)
pub mod tuple;

/// Table access implementation modules (scan, index, mutation, ddl, relation)
pub mod access;

/// Typed PG-expression views, runtime parameters, and planned filter pushdown.
pub mod expr;

/// Generic CustomScan filter-pushdown framework: planner-and-executor seam
/// that turns SQL `WHERE` predicates into provider-native scan predicates.
pub mod customscan;

/// Generic PostgreSQL Foreign Data Wrapper planning and scan framework.
pub mod fdw;

/// PostgreSQL COPY execution primitives shared by utility consumers.
pub mod copy;

/// Registration logic for Table Access Method routines
pub mod registry;

/// PostgreSQL catalog option extraction, persistence, and caches
pub mod options;

/// PostgreSQL hooks framework (utility, object access, etc.)
pub mod hooks;

/// Custom WAL Resource Manager framework
pub mod wal;

/// ResourceOwner-scoped cleanup callbacks.  Distinct from transaction events.
pub mod resource;

/// Exact-build C ABI published by `lagodb-base` through rendezvous.
pub mod runtime_api;

/// Transaction lifecycle callbacks.  Distinct from ResourceOwner cleanup.
pub mod transaction;

/// Format-neutral durable physical object-cleanup queue and worker framework.
pub mod object_cleanup;

/// Runtime-backed settings shared by maintenance domains.
pub(crate) mod maintenance_config;

/// Format-neutral logical table-maintenance provider SPI and VACUUM routing.
pub mod table_maintenance;

/// Helper functions and diagnostics
pub mod diag;

/// Shared `copyObject`-safe plan-data primitives.
pub mod plan_data;

/// Internal wrapper for PostgreSQL functions
mod wrapper;

/// Catalog access and caching
pub mod catalog;

/// PostgreSQL backend latch primitives.
pub mod pg_latch;

/// Typed PostgreSQL injection points with version-compatible no-op fallback.
pub mod injection_point;

/// Database-local extension worker protocol.
pub mod extension_worker;

/// Storage service and storage-volume APIs.
pub mod storage;

/// The prelude includes all necessary imports to make lagodb_core work
pub mod prelude {
    pub use crate::api::*;
    pub use crate::batch::*;
    pub use crate::diag::{
        PgError, PgErrorReport, PgErrorSource, PgReportError, ReportableError,
        SqlStateError,
    };
    pub use crate::handles::*;
    pub use crate::tuple::*;
    pub use crate::{pg_fdw, pg_table_am};
}

use pgrx::AllocatedByPostgres;
use pgrx::prelude::*;

/// PgBox'ed `TableAmRoutine`, used in [`am_routine`](api::TableAccessMethod::am_routine)
pub type TableAmRoutine<A = AllocatedByPostgres> = PgBox<pg_sys::TableAmRoutine, A>;

/// Procedural macro for generating table access method boilerplate
pub use lagodb_macros::{pg_fdw, pg_table_am};
