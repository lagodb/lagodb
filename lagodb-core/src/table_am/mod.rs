//! Table access method contract and its PostgreSQL `TableAmRoutine` adapter.
//!
//! # Trait organization: session vs stateless
//!
//! The table-AM surface is split along a single axis: whether a callback owns
//! per-operation runtime state.
//!
//! - **Stateless facets** ([`AmScan`], [`AmRelation`], [`AmIndexCallbacks`],
//!   [`AmDdl`]) collect callbacks PostgreSQL invokes without an existing
//!   per-operation handle. Their methods take no `&self`, are implemented
//!   directly on the AM identity type (e.g. `IcebergTableAm`), and act as
//!   namespaced free functions. They are aggregated as supertraits of
//!   [`TableAccessMethod`] so the routine builder only needs a single
//!   `A: TableAccessMethod` bound.
//! - **Session traits** ([`AmScanSession`], [`AmIndexFetchSession`],
//!   [`AmModifyQueryState`], [`AmModifyState`], [`AmCopySession`]) describe
//!   stateful callbacks and lifecycles tied to a scan, an index fetch, a Modify
//!   query, one result relation, or a COPY frame.
//!   They surface as associated
//!   types on [`TableAccessMethod`] so the framework can construct, store,
//!   and drop one instance per active operation.
//!
//! ## Why this shape
//!
//! An earlier iteration modeled every callback group as an associated state
//! type (`type RelationState`, `type DdlState`, ...). Because relation-level
//! and DDL callbacks are stateless, consumers had to define empty marker
//! structs purely to satisfy the type system. The current split removes those
//! markers: only callbacks with real runtime state are associated types, and
//! everything else is a supertrait facet implemented on the AM identity type.
//!
//! ## Design tradeoffs considered
//!
//! - *Single root trait with all methods inlined* (DataFusion `TableProvider`
//!   style): fewest traits, but Rust does not allow splitting
//!   a single `impl` across files. A consumer would either accumulate one
//!   large `impl TableAccessMethod` block or thunk through helper functions in
//!   `relation.rs`/`ddl.rs`. Rejected to preserve module boundaries.
//! - *Facet traits + associated state types* (the original shape): forces
//!   empty marker types for stateless facets. Rejected for the reasons above.
//! - *Facet traits as supertraits + associated session types* (current
//!   choice): consumers implement each facet directly on the AM type, keeping
//!   per-domain code in its own file, while session traits remain associated
//!   types so the framework can own their lifetimes. Selected.
//!
//! ## `AmIndex` was split deliberately
//!
//! `index_build_range_scan` / `index_validate_scan` / `index_delete_tuples`
//! are stateless callbacks and live on [`AmIndexCallbacks`]. The `&mut self`
//! `index_fetch_*` lifecycle lives on [`AmIndexFetchSession`]. Keeping them
//! together would have forced one of them onto the wrong side of the
//! session/stateless boundary.
//!
//! ## Error boundary invariant
//!
//! Table-AM trait methods are PostgreSQL callback boundaries, so they return
//! [`AmResult<T>`], whose error type owns a PostgreSQL [`ErrorReport`]. This is
//! a deliberate API boundary: framework errors such as "callback not supported"
//! are reported here, and AM-specific domain errors should convert into
//! `ErrorReport` at this edge.
//!
//! Do not reintroduce a generic AM error parameter such as `E: AmError`.
//! Concrete AM implementations should keep their internal business logic on
//! their own domain result type, implement `From<DomainError> for ErrorReport`,
//! and use normal `?` propagation from callback methods. This preserves natural
//! Rust error flow inside the AM while keeping the public callback API aligned
//! with PostgreSQL.
//!
//! # Module layout
//!
//! The contract is split by callback domain (`scan`, `scan_ops`, `relation`,
//! `index`, `modify`, `copy`, `ddl`) and re-exported flat from this module.
//! `modify` also owns the query-level state handle and the core-owned
//! trigger-row store it carries. `callbacks` holds the `extern "C-unwind"`
//! functions installed in PostgreSQL's `TableAmRoutine`; it adapts PostgreSQL
//! to the contract and is not part of the provider-facing surface.

mod callbacks;
mod copy;
mod ddl;
mod index;
mod modify;
mod relation;
mod scan;
mod scan_ops;

use std::fmt::{Display, Formatter};

use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use pgrx::{AllocatedByPostgres, PgBox};

pub use callbacks::virtual_slot_callbacks_with_tid;
pub(crate) use callbacks::{
    RelationTriggerPolicy, TableAmScanDesc, acquire_modify_query_state,
};
pub use copy::AmCopySession;
pub use ddl::AmDdl;
pub use index::{AmIndexCallbacks, AmIndexFetchSession, IndexBuildContext};
pub(crate) use modify::ModifyQueryShared;
pub use modify::{
    AmModifyQueryState, AmModifyState, ModifyActions, ModifyQueryState,
    ModifyScanBinding, ModifyStateContext, MutationDeleteContext, MutationOutcome,
    MutationUpdateContext, MutationWriteContext, TRIGGER_ROW_BLOCK_BASE,
};
pub use relation::AmRelation;
pub use scan::{AmScan, AmScanSession, AnalyzeTupleOutcome, ScanFlags};
pub use scan_ops::{BitmapScanOps, TidRangeOps};

use crate::diag::{PgReportError, SqlStateError};

/// PgBox'ed `TableAmRoutine`, returned by [`TableAccessMethod::am_routine`].
pub type TableAmRoutine<A = AllocatedByPostgres> = PgBox<pg_sys::TableAmRoutine, A>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AmFrameworkError {
    NotImplemented(&'static str),
}

impl AmFrameworkError {
    #[inline]
    pub fn not_implemented(method: &'static str) -> Self {
        Self::NotImplemented(method)
    }
}

impl Display for AmFrameworkError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotImplemented(method) => {
                write!(f, "{method} is not supported by this access method")
            }
        }
    }
}

impl std::error::Error for AmFrameworkError {}

impl SqlStateError for AmFrameworkError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
    }
}

impl From<AmFrameworkError> for ErrorReport {
    fn from(value: AmFrameworkError) -> Self {
        ErrorReport::new(
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            value.to_string(),
            "",
        )
    }
}

/// Result type for table-AM callbacks.
///
/// This is intentionally fixed to a small PostgreSQL error handle. Code below
/// this API boundary may use richer domain errors, but callbacks should expose
/// PostgreSQL errors directly and rely on `From<DomainError> for ErrorReport`
/// plus `?` for conversion.
pub type AmError = PgReportError;
pub type AmResult<T> = Result<T, AmError>;

/// Return PostgreSQL `FEATURE_NOT_SUPPORTED` for a table-AM callback that the
/// implementing AM explicitly does not support.
pub fn unsupported_callback<T>(method: &'static str) -> AmResult<T> {
    Err(AmFrameworkError::not_implemented(method).into())
}

/// Root trait identifying a Postgres table access method implementation.
///
/// Stateless callback groups are pulled in via supertraits ([`AmScan`],
/// [`AmRelation`], [`AmIndexCallbacks`], [`AmDdl`]) and implemented directly
/// on the AM identity type. Per-operation state lives behind associated
/// types: [`Self::ScanSession`], [`Self::IndexFetchSession`],
/// [`Self::ModifyQueryState`], [`Self::ModifyState`], [`Self::CopySession`].
/// See the [module-level docs](self) for the full rationale.
pub trait TableAccessMethod:
    AmScan + AmRelation + AmIndexCallbacks + AmDdl + 'static
{
    type ScanSession: AmScanSession + 'static;
    type IndexFetchSession: AmIndexFetchSession + 'static;
    type ModifyQueryState: AmModifyQueryState;
    type ModifyState: AmModifyState<QueryState = Self::ModifyQueryState> + 'static;
    type CopySession: AmCopySession + 'static;

    /// Resolve this access method's current catalog OID.
    ///
    /// Extensions must not cache this value: an access method can be created,
    /// dropped, and recreated while a backend remains alive.
    fn access_method_oid() -> Option<pg_sys::Oid>;

    fn am_routine() -> TableAmRoutine
    where
        Self: Sized,
    {
        callbacks::build_routine::<Self>()
    }
}
