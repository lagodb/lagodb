#![allow(clippy::too_many_arguments)]

//! pg-lakebase-core: A framework for building PostgreSQL Table Access Methods in Rust
//!
//! This library provides a safe, ergonomic API for implementing custom table access
//! methods for PostgreSQL using the pgrx framework.
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use pg_lakebase_core::prelude::*;
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

/// Safe wrapper types for PostgreSQL FFI types
pub mod handles;

/// PostgreSQL tuple value abstractions (Cell, Row)
pub mod tuple;

/// Table access implementation modules (scan, index, mutation, ddl, relation)
pub mod access;

/// Typed PG-`Expr` views, walkers, classification, and the runtime predicate
/// translator surface used by the CustomScan framework.
pub mod expr;

/// Generic CustomScan filter-pushdown framework: planner-and-executor seam
/// that turns SQL `WHERE` predicates into provider-native scan predicates.
pub mod customscan;

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

/// Versioned C ABI published by `pg-lakebase-runtime` through rendezvous.
pub mod runtime_api;

/// Transaction lifecycle callbacks.  Distinct from ResourceOwner cleanup.
pub mod transaction;

/// Format-neutral durable maintenance queue and worker framework.
pub mod maintenance;

/// Format-neutral logical table-maintenance provider SPI and VACUUM routing.
pub mod table_maintenance;

/// Helper functions and diagnostics
pub mod diag;

/// Internal wrapper for PostgreSQL functions
mod wrapper;

/// Catalog access and caching
pub mod catalog;

/// PostgreSQL bgworker/backend process primitives.
pub mod bgworker;

/// Database-local extension worker protocol.
pub mod extension_worker;

/// Consumer API for the runtime-owned storage service.
pub mod storage_service;

/// The prelude includes all necessary imports to make pg_lakebase_core work
pub mod prelude {
    pub use crate::api::*;
    pub use crate::batch::*;
    pub use crate::diag::{
        PgError, PgErrorReport, PgErrorSource, PgReportError, ReportableError,
        SqlStateError,
    };
    pub use crate::handles::*;
    pub use crate::pg_table_am;
    pub use crate::tuple::*;
}

use pgrx::AllocatedByPostgres;
use pgrx::prelude::*;

/// PgBox'ed `TableAmRoutine`, used in [`am_routine`](api::TableAccessMethod::am_routine)
pub type TableAmRoutine<A = AllocatedByPostgres> = PgBox<pg_sys::TableAmRoutine, A>;

/// Procedural macro for generating table access method boilerplate
pub use pg_lakebase_macros::pg_table_am;
