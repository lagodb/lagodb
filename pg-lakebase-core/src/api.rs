//! Provides interface types and trait to develop Postgres table access method.
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
//!   [`TableAccessMethod`] so the registry only needs a single `A: TableAccessMethod`
//!   bound.
//! - **Session traits** ([`AmScanSession`], [`AmIndexFetchSession`],
//!   [`AmDmlSession`]) describe `&mut self` lifecycles tied to a scan, an
//!   index fetch, or a statement-scoped DML batch. They surface as associated
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
//! - *Single root trait with all methods inlined* (DuckDB / DataFusion
//!   `TableProvider` style): fewest traits, but Rust does not allow splitting
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

use crate::TableAmRoutine;
use crate::diag::PgReportError;
use crate::handles::{
    AttrWidthsHandle, BufferAccessStrategyHandle, BulkInsertStateHandle,
    CallbackStateHandle, IndexBuildCallbackHandle, IndexInfoHandle, ItemPointer,
    ParallelTableScanDescHandle, ReadStreamHandle, RelFileLocator, RelationHandle,
    SampleScanStateHandle, ScanDirection, ScanKeyHandle, SnapshotHandle,
    TBMIterateResultHandle, TM_FailureData, TMIndexDeleteOpHandle,
    TableScanDescHandle, TupleTableSlotHandle, VacuumParamsHandle,
    ValidateIndexStateHandle, VarlenaHandle,
};
use crate::tuple::{Row, TupleSlotBatch, TupleSlotRow};
use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use std::fmt::{Display, Formatter};

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
/// [`Self::DmlSession`]. See the [module-level docs](self) for the full
/// rationale.
pub trait TableAccessMethod:
    AmScan + AmRelation + AmIndexCallbacks + AmDdl + 'static
{
    type ScanSession: AmScanSession;
    type IndexFetchSession: AmIndexFetchSession;
    type DmlSession: AmDmlSession + 'static;

    fn am_routine() -> TableAmRoutine
    where
        Self: Sized,
    {
        crate::registry::build_table_am_routine::<Self>()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ScanCapabilities {
    pub tid_range: bool,
    pub bitmap: bool,
}

impl ScanCapabilities {
    pub const NONE: Self = Self {
        tid_range: false,
        bitmap: false,
    };

    pub const TID_RANGE_AND_BITMAP: Self = Self {
        tid_range: true,
        bitmap: true,
    };
}

#[derive(Debug, Clone, Copy)]
pub struct DmlTarget {
    pub rel_oid: pg_sys::Oid,
    pub namespace_oid: pg_sys::Oid,
    pub locator: RelFileLocator,
    pub relation_needs_wal: bool,
    pub cmd_type: pg_sys::CmdType::Type,
}

impl DmlTarget {
    pub fn from_relation_with_cmd_type(
        rel: &RelationHandle,
        cmd_type: pg_sys::CmdType::Type,
    ) -> Self {
        let raw = rel.as_raw();
        Self {
            rel_oid: unsafe { (*raw).rd_id },
            namespace_oid: rel.namespace_oid(),
            locator: unsafe {
                RelFileLocator::from_raw_unchecked(&(*raw).rd_locator)
            },
            relation_needs_wal: rel.needs_wal(),
            cmd_type,
        }
    }
}

pub trait AmScan {
    const SCAN_CAPABILITIES: ScanCapabilities = ScanCapabilities::NONE;

    fn slot_callbacks() -> *const pg_sys::TupleTableSlotOps {
        unsafe { &pg_sys::TTSOpsVirtual }
    }

    fn parallelscan_estimate(rel: &RelationHandle) -> AmResult<pg_sys::Size>
    where
        Self: Sized,
    {
        let _ = rel;
        Ok(0)
    }

    fn parallelscan_initialize(
        rel: &RelationHandle,
        pscan: &ParallelTableScanDescHandle,
    ) -> AmResult<pg_sys::Size>
    where
        Self: Sized,
    {
        let _ = (rel, pscan);
        Ok(0)
    }

    fn parallelscan_reinitialize(
        rel: &RelationHandle,
        pscan: &ParallelTableScanDescHandle,
    ) -> AmResult<()>
    where
        Self: Sized,
    {
        let _ = (rel, pscan);
        Ok(())
    }
}

pub trait AmScanSession {
    fn new(
        rel: &RelationHandle,
        snapshot: &SnapshotHandle,
        key: Option<&ScanKeyHandle>,
        pscan: Option<&ParallelTableScanDescHandle>,
        flags: u32,
    ) -> AmResult<Self>
    where
        Self: Sized;

    fn scan_begin(&mut self) -> AmResult<()>;

    fn scan_getnextslot(
        &mut self,
        direction: ScanDirection,
        row: &mut Row,
    ) -> AmResult<bool>;

    fn scan_rescan(
        &mut self,
        key: Option<&ScanKeyHandle>,
        set_params: bool,
        allow_strat: bool,
        allow_sync: bool,
        allow_pagemode: bool,
    ) -> AmResult<()>;

    fn scan_end(&mut self) -> AmResult<()>;

    fn scan_set_tidrange(
        &mut self,
        mintid: &ItemPointer,
        maxtid: &ItemPointer,
    ) -> AmResult<()> {
        let _ = (mintid, maxtid);
        unsupported_callback("scan_set_tidrange")
    }

    fn scan_getnextslot_tidrange(
        &mut self,
        direction: ScanDirection,
        row: &mut Row,
    ) -> AmResult<bool> {
        let _ = (direction, row);
        unsupported_callback("scan_getnextslot_tidrange")
    }

    fn scan_bitmap_next_block(
        &mut self,
        tbmres: &TBMIterateResultHandle,
    ) -> AmResult<bool> {
        let _ = tbmres;
        unsupported_callback("scan_bitmap_next_block")
    }

    fn scan_bitmap_next_tuple(
        &mut self,
        tbmres: &TBMIterateResultHandle,
        row: &mut Row,
    ) -> AmResult<bool> {
        let _ = (tbmres, row);
        unsupported_callback("scan_bitmap_next_tuple")
    }

    fn scan_sample_next_block(
        &mut self,
        scanstate: &SampleScanStateHandle,
    ) -> AmResult<bool> {
        let _ = scanstate;
        unsupported_callback("scan_sample_next_block")
    }

    fn scan_sample_next_tuple(
        &mut self,
        scanstate: &SampleScanStateHandle,
        row: &mut Row,
    ) -> AmResult<bool> {
        let _ = (scanstate, row);
        unsupported_callback("scan_sample_next_tuple")
    }

    fn scan_analyze_next_block(
        &mut self,
        stream: &ReadStreamHandle,
    ) -> AmResult<bool> {
        let _ = stream;
        unsupported_callback("scan_analyze_next_block")
    }

    fn scan_analyze_next_tuple(
        &mut self,
        oldest_xmin: pg_sys::TransactionId,
        row: &mut Row,
    ) -> AmResult<(bool, f64, f64)> {
        let _ = (oldest_xmin, row);
        unsupported_callback("scan_analyze_next_tuple")
    }

    fn tuple_tid_valid(&mut self, tid: &ItemPointer) -> AmResult<bool> {
        let _ = tid;
        Ok(false)
    }

    fn tuple_get_latest_tid(&mut self, tid: &mut ItemPointer) -> AmResult<()> {
        let _ = tid;
        Ok(())
    }
}

pub trait AmRelation {
    fn relation_estimate_size(
        rel: &RelationHandle,
        attr_widths: Option<&mut AttrWidthsHandle>,
    ) -> AmResult<(pg_sys::BlockNumber, f64, f64)>
    where
        Self: Sized;

    fn relation_size(
        rel: &RelationHandle,
        fork_number: pg_sys::ForkNumber::Type,
    ) -> AmResult<u64>
    where
        Self: Sized;

    fn relation_needs_toast_table(rel: &RelationHandle) -> AmResult<bool>
    where
        Self: Sized,
    {
        let _ = rel;
        Ok(false)
    }

    fn relation_toast_am(rel: &RelationHandle) -> AmResult<pg_sys::Oid>
    where
        Self: Sized,
    {
        let _ = rel;
        Ok(pg_sys::HEAP_TABLE_AM_OID)
    }

    fn relation_fetch_toast_slice(
        toastrel: &RelationHandle,
        valueid: pg_sys::Oid,
        attrsize: i32,
        sliceoffset: i32,
        slicelength: i32,
        result: &VarlenaHandle,
    ) -> AmResult<()>
    where
        Self: Sized,
    {
        let _ = (
            toastrel,
            valueid,
            attrsize,
            sliceoffset,
            slicelength,
            result,
        );
        unsupported_callback("relation_fetch_toast_slice")
    }

    fn tuple_fetch_row_version(
        rel: &RelationHandle,
        tid: &ItemPointer,
        snapshot: &SnapshotHandle,
        row: &mut Row,
    ) -> AmResult<bool>
    where
        Self: Sized,
    {
        let _ = (rel, tid, snapshot, row);
        unsupported_callback("tuple_fetch_row_version")
    }

    fn tuple_satisfies_snapshot(
        rel: &RelationHandle,
        slot: &TupleTableSlotHandle,
        snapshot: &SnapshotHandle,
    ) -> AmResult<bool>
    where
        Self: Sized,
    {
        let _ = (rel, slot, snapshot);
        unsupported_callback("tuple_satisfies_snapshot")
    }

    fn relation_vacuum(
        rel: &RelationHandle,
        params: &VacuumParamsHandle,
        bstrategy: &BufferAccessStrategyHandle,
    ) -> AmResult<()>
    where
        Self: Sized,
    {
        let _ = (rel, params, bstrategy);
        Ok(())
    }
}

pub trait AmIndexFetchSession {
    fn new(rel: &RelationHandle) -> AmResult<Self>
    where
        Self: Sized;

    fn index_fetch_begin(&mut self) -> AmResult<()>;
    fn index_fetch_reset(&mut self) -> AmResult<()> {
        Ok(())
    }

    fn index_fetch_tuple(
        &mut self,
        tid: &ItemPointer,
        snapshot: &SnapshotHandle,
        row: &mut Row,
        call_again: &mut bool,
        all_dead: &mut bool,
    ) -> AmResult<bool>;

    fn index_fetch_end(&mut self) -> AmResult<()>;
}

/// Index-related callbacks in PostgreSQL's table AM vtable.
///
/// These are not callbacks for PostgreSQL index access methods such as B-tree,
/// GiST, or GIN. Relation arguments may refer to the table relation and, where
/// present, the index relation.
pub trait AmIndexCallbacks {
    fn index_build_range_scan(
        table_rel: &RelationHandle,
        index_rel: &RelationHandle,
        index_info: &IndexInfoHandle,
        allow_sync: bool,
        anyvisible: bool,
        progress: bool,
        start_blockno: pg_sys::BlockNumber,
        numblocks: pg_sys::BlockNumber,
        callback: &IndexBuildCallbackHandle,
        callback_state: &CallbackStateHandle,
        scan: &TableScanDescHandle,
    ) -> AmResult<f64>
    where
        Self: Sized,
    {
        let _ = (
            table_rel,
            index_rel,
            index_info,
            allow_sync,
            anyvisible,
            progress,
            start_blockno,
            numblocks,
            callback,
            callback_state,
            scan,
        );
        Ok(0.0)
    }

    fn index_validate_scan(
        table_rel: &RelationHandle,
        index_rel: &RelationHandle,
        index_info: &IndexInfoHandle,
        snapshot: &SnapshotHandle,
        state: &ValidateIndexStateHandle,
    ) -> AmResult<()>
    where
        Self: Sized,
    {
        let _ = (table_rel, index_rel, index_info, snapshot, state);
        Ok(())
    }

    fn index_delete_tuples(
        rel: &RelationHandle,
        delstate: &mut TMIndexDeleteOpHandle,
    ) -> AmResult<pg_sys::TransactionId>
    where
        Self: Sized,
    {
        let _ = (rel, delstate);
        unsupported_callback("index_delete_tuples")
    }
}

/// Relation-local AM state for one managed PostgreSQL DML frame.
///
/// This trait is the AM-facing `dml_init`/`dml_fini` abstraction.  PostgreSQL
/// does not expose such callbacks directly in `TableAmRoutine`; it exposes
/// tuple-level callbacks instead.  The framework creates an `AmDmlSession` on
/// the first tuple callback for a relation inside a managed frame, calls
/// [`begin_modify`](Self::begin_modify), dispatches all tuple callbacks for that
/// relation to the same session, and then calls [`end_modify`](Self::end_modify)
/// exactly once when the frame completes successfully.
///
/// Abort paths are separate.  If PostgreSQL raises ERROR, aborts the
/// transaction, or rolls back to a savepoint, ResourceOwner cleanup drops the
/// frame and unfinalized sessions receive [`abort_modify`](Self::abort_modify)
/// instead of `end_modify`.
///
/// `DmlTarget::cmd_type` describes the PostgreSQL frame operation.  For MERGE it
/// is `CMD_MERGE`, while the actual physical action for each row is still
/// expressed by the callback being invoked (`tuple_insert`, `tuple_update`, or
/// `tuple_delete`).
pub trait AmDmlSession {
    fn new(target: DmlTarget) -> AmResult<Self>
    where
        Self: Sized;

    /// Opens relation-local resources for the current DML frame.
    ///
    /// Typical implementations load table metadata, create file writers, and
    /// allocate buffers here.  This is frame-scoped, not transaction-scoped: a
    /// single transaction may create many sessions across multiple statements,
    /// partitions, nested SPI calls, COPY frames, or MERGE frames.
    fn begin_modify(&mut self) -> AmResult<()>;

    /// Finishes the relation-local write session for a successful DML frame.
    ///
    /// A single PostgreSQL DML frame can host multiple AM sessions, for example
    /// partition routing or COPY into a partitioned table. Implementations
    /// should not make per-session writes externally visible here unless that
    /// publication can remain correct when a later session in the same frame
    /// fails. Prefer staging publication and deferring the externally visible
    /// commit to a transaction-scoped resource, with matching cleanup such as
    /// pending-delete registration, so all sessions in the frame become visible
    /// atomically.
    fn end_modify(&mut self) -> AmResult<()>;

    /// Cleans up relation-local resources after ERROR/abort/rollback.
    ///
    /// This may run during ResourceOwner cleanup after PostgreSQL has unwound
    /// past normal Rust control flow.  Keep it best-effort and idempotent; it
    /// must not assume `end_modify()` ran.
    fn abort_modify(&mut self) {}

    fn tuple_insert(
        &mut self,
        row: Row,
        cid: pg_sys::CommandId,
        options: i32,
        bistate: Option<&BulkInsertStateHandle>,
    ) -> AmResult<()> {
        let _ = (row, cid, options, bistate);
        unsupported_callback("tuple_insert")
    }

    /// Insert from a PostgreSQL tuple-slot view.
    ///
    /// This is the first method reached by the TableAM callback. The default
    /// implementation materializes an owned [`Row`] and calls
    /// [`Self::tuple_insert`], which is appropriate for row-oriented AMs.
    /// Columnar AMs should override this method and append `PgDatumRef` values
    /// from `row` directly into their column builders.
    fn tuple_insert_slot(
        &mut self,
        row: TupleSlotRow<'_>,
        cid: pg_sys::CommandId,
        options: i32,
        bistate: Option<&BulkInsertStateHandle>,
    ) -> AmResult<()> {
        self.tuple_insert(row.to_owned_row(), cid, options, bistate)
    }

    fn tuple_insert_speculative(
        &mut self,
        row: Row,
        cid: pg_sys::CommandId,
        options: i32,
        bistate: Option<&BulkInsertStateHandle>,
        spec_token: u32,
    ) -> AmResult<()> {
        let _ = (row, cid, options, bistate, spec_token);
        unsupported_callback("tuple_insert_speculative")
    }

    fn tuple_insert_speculative_slot(
        &mut self,
        row: TupleSlotRow<'_>,
        cid: pg_sys::CommandId,
        options: i32,
        bistate: Option<&BulkInsertStateHandle>,
        spec_token: u32,
    ) -> AmResult<()> {
        self.tuple_insert_speculative(
            row.to_owned_row(),
            cid,
            options,
            bistate,
            spec_token,
        )
    }

    fn multi_insert(
        &mut self,
        rows: Vec<Row>,
        cid: pg_sys::CommandId,
        options: i32,
        bistate: Option<&BulkInsertStateHandle>,
    ) -> AmResult<()> {
        let _ = (rows, cid, options, bistate);
        unsupported_callback("multi_insert")
    }

    /// Multi-insert from PostgreSQL tuple-slot views.
    ///
    /// The default implementation materializes a `Vec<Row>` and calls
    /// [`Self::multi_insert`]. Columnar AMs should override this method to
    /// append every slot row directly into their columnar batch buffer.
    fn multi_insert_slots(
        &mut self,
        rows: TupleSlotBatch<'_>,
        cid: pg_sys::CommandId,
        options: i32,
        bistate: Option<&BulkInsertStateHandle>,
    ) -> AmResult<()> {
        self.multi_insert(rows.to_owned_rows(), cid, options, bistate)
    }

    fn tuple_delete(
        &mut self,
        tid: &ItemPointer,
        cid: pg_sys::CommandId,
        snapshot: &SnapshotHandle,
        crosscheck: Option<&SnapshotHandle>,
        wait: bool,
        tmfd: &mut TM_FailureData,
        changing_part: bool,
    ) -> AmResult<pg_sys::TM_Result::Type> {
        let _ = (tid, cid, snapshot, crosscheck, wait, tmfd, changing_part);
        unsupported_callback("tuple_delete")
    }

    fn tuple_update(
        &mut self,
        otid: &ItemPointer,
        row: Row,
        cid: pg_sys::CommandId,
        snapshot: &SnapshotHandle,
        crosscheck: Option<&SnapshotHandle>,
        wait: bool,
        tmfd: &mut TM_FailureData,
        lockmode: &mut pg_sys::LockTupleMode::Type,
        update_indexes: &mut pg_sys::TU_UpdateIndexes::Type,
    ) -> AmResult<pg_sys::TM_Result::Type> {
        let _ = (
            otid,
            row,
            cid,
            snapshot,
            crosscheck,
            wait,
            tmfd,
            lockmode,
            update_indexes,
        );
        unsupported_callback("tuple_update")
    }

    /// Update from a PostgreSQL tuple-slot view.
    ///
    /// This mirrors [`Self::tuple_insert_slot`]: the default row-mode fallback
    /// materializes an owned [`Row`], while columnar AMs can override it to
    /// avoid intermediate row/cell allocation.
    fn tuple_update_slot(
        &mut self,
        otid: &ItemPointer,
        row: TupleSlotRow<'_>,
        cid: pg_sys::CommandId,
        snapshot: &SnapshotHandle,
        crosscheck: Option<&SnapshotHandle>,
        wait: bool,
        tmfd: &mut TM_FailureData,
        lockmode: &mut pg_sys::LockTupleMode::Type,
        update_indexes: &mut pg_sys::TU_UpdateIndexes::Type,
    ) -> AmResult<pg_sys::TM_Result::Type> {
        self.tuple_update(
            otid,
            row.to_owned_row(),
            cid,
            snapshot,
            crosscheck,
            wait,
            tmfd,
            lockmode,
            update_indexes,
        )
    }

    fn tuple_lock(
        &mut self,
        tid: &ItemPointer,
        snapshot: &SnapshotHandle,
        row: &mut Row,
        cid: pg_sys::CommandId,
        mode: pg_sys::LockTupleMode::Type,
        wait_policy: pg_sys::LockWaitPolicy::Type,
        flags: u8,
        tmfd: &mut TM_FailureData,
    ) -> AmResult<pg_sys::TM_Result::Type> {
        let _ = (tid, snapshot, row, cid, mode, wait_policy, flags, tmfd);
        unsupported_callback("tuple_lock")
    }

    fn finish_bulk_insert(&mut self, options: i32) -> AmResult<()> {
        let _ = options;
        Ok(())
    }

    fn tuple_complete_speculative(
        &mut self,
        row: Row,
        spec_token: u32,
        succeeded: bool,
    ) -> AmResult<()> {
        let _ = (row, spec_token, succeeded);
        unsupported_callback("tuple_complete_speculative")
    }

    fn tuple_complete_speculative_slot(
        &mut self,
        row: TupleSlotRow<'_>,
        spec_token: u32,
        succeeded: bool,
    ) -> AmResult<()> {
        self.tuple_complete_speculative(row.to_owned_row(), spec_token, succeeded)
    }
}

pub trait AmDdl {
    fn relation_set_new_filelocator(
        rel: &RelationHandle,
        newrlocator: &RelFileLocator,
        persistence: u8,
    ) -> AmResult<(pg_sys::TransactionId, pg_sys::MultiXactId)>
    where
        Self: Sized;

    fn relation_nontransactional_truncate(rel: &RelationHandle) -> AmResult<()>
    where
        Self: Sized;

    fn relation_copy_data(
        rel: &RelationHandle,
        newrlocator: &RelFileLocator,
    ) -> AmResult<()>
    where
        Self: Sized,
    {
        let _ = (rel, newrlocator);
        unsupported_callback("relation_copy_data")
    }

    fn relation_copy_for_cluster(
        old_table: &RelationHandle,
        new_table: &RelationHandle,
        old_index: Option<&RelationHandle>,
        use_sort: bool,
        oldest_xmin: pg_sys::TransactionId,
    ) -> AmResult<(pg_sys::TransactionId, pg_sys::MultiXactId, f64, f64, f64)>
    where
        Self: Sized,
    {
        let _ = (old_table, new_table, old_index, use_sort, oldest_xmin);
        unsupported_callback("relation_copy_for_cluster")
    }
}
