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

use crate::TableAmRoutine;
use crate::batch::ScanBatchDriver;
use crate::diag::{PgReportError, SqlStateError};
use crate::handles::{
    AttrWidthsHandle, BufferAccessStrategyHandle, BulkInsertStateHandle,
    CallbackStateHandle, IndexBuildCallbackHandle, IndexInfoHandle, ItemPointer,
    OwnedScanKeys, ParallelTableScanDescHandle, ReadStreamHandle, RelFileLocator,
    RelationHandle, SampleScanStateHandle, ScanDirection, SnapshotHandle,
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
    /// Concrete slot-filling scan driver (implements [`ScanBatchDriver`]).
    /// A columnar AM sets this to its cursor type (e.g. an Arrow batch
    /// cursor). Binding a concrete type (rather than `dyn ScanBatchDriver`)
    /// keeps the per-row decode a static call.
    type BatchDriver: ScanBatchDriver;

    /// Create a new scan session.
    ///
    /// `new()` runs before any storage IO and therefore before the AM has
    /// resolved its physical schema. Predicate keys are deliberately not
    /// passed here: many access methods can only translate them once the
    /// schema is known. They are surfaced to [`Self::scan_begin`] instead,
    /// where the AM is expected to do schema-aware work.
    fn new(
        rel: &RelationHandle,
        snapshot: &SnapshotHandle,
        pscan: Option<&ParallelTableScanDescHandle>,
        flags: u32,
    ) -> AmResult<Self>
    where
        Self: Sized;

    /// Begin the scan.
    ///
    /// `keys` is the dispatcher-owned, dispatcher-copied set of effective
    /// scan keys for the *initial* scan. The reference is only valid for
    /// the duration of this call; the AM must consume the keys (e.g.
    /// translate them into its native predicate language) here rather than
    /// retaining the borrow. The owned buffer behind it lives in the FFI
    /// session container and is what later [`Self::scan_rescan`] calls will
    /// see updated in place; AMs that need to refer back to "the current
    /// effective keys" outside of these callbacks should re-translate from
    /// the keys argument they receive on the next callback.
    fn scan_begin(&mut self, keys: &OwnedScanKeys) -> AmResult<()>;

    /// The scan's slot-filling driver for this session.
    ///
    /// The framework drives every TableAM scan through the one uniform
    /// [`ScanBatchDriver::next_into_slot`] path: the C shim calls this once per
    /// `scan_getnextslot` and asks the returned driver to fill the slot with
    /// the next tuple. row-vs-column is an implementation detail *inside* the
    /// driver (a columnar AM decodes an Arrow batch straight into the slot),
    /// not a branch in the framework. There is no separate row callback: a
    /// TableAM is columnar by contract, and a row-at-a-time source (FDW) is a
    /// different framework, not an `AmScanSession`.
    ///
    /// The driver must be ready by the time the executor fetches a row, i.e.
    /// after [`Self::scan_begin`]; sessions typically build it there.
    fn scan_driver(&mut self) -> &mut Self::BatchDriver;

    /// Restart the scan.
    ///
    /// `keys` reflects the dispatcher's PostgreSQL-aligned semantics: a
    /// non-null `key` argument from `scan_rescan` *replaces* the previously
    /// stored keys (PostgreSQL's heap AM does the same with `memcpy` in
    /// `initscan`); a null `key` keeps the prior keys unchanged. The
    /// dispatcher has already applied that rule before calling this method,
    /// so `keys` is always the *effective* key set for the upcoming scan.
    /// Like in `scan_begin`, the reference is only valid for this call.
    ///
    /// `set_params` and the `allow_*` flags are heap-AM scan strategy
    /// hints (BufferAccessStrategy, sync scan, page mode). AMs that do not
    /// implement those strategies may safely ignore them.
    fn scan_rescan(
        &mut self,
        keys: &OwnedScanKeys,
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

/// Whether a relation-local row-level DML session logically depends on its
/// ModifyTable target scan.
///
/// This is deliberately independent of whether a scan callback happened. The
/// optimizer can prove a MERGE target unreachable (for example `ON FALSE`) and
/// remove that scan, making its insert action an independent append. Conversely,
/// a required read that was not observed is an AM/executor invariant failure,
/// not an independent write. Arbitrary source-subquery reads and PostgreSQL SSI
/// dependencies are outside this context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmlTargetReadRequirement {
    /// The write does not depend on a row-level scan of its target relation.
    Independent,
    /// The row-level operation depends on a target scan and its read snapshot.
    ReadRequired,
}

/// Immutable frame context supplied when a relation-local DML session is
/// created.
#[derive(Debug, Clone, Copy)]
pub struct DmlSessionContext {
    cmd_type: pg_sys::CmdType::Type,
    target_read: DmlTargetReadRequirement,
}

impl DmlSessionContext {
    pub(crate) fn new(
        cmd_type: pg_sys::CmdType::Type,
        target_read: DmlTargetReadRequirement,
    ) -> Self {
        Self {
            cmd_type,
            target_read,
        }
    }

    /// PostgreSQL command type for the owning DML frame.
    pub fn cmd_type(self) -> pg_sys::CmdType::Type {
        self.cmd_type
    }

    /// Logical target-read requirement resolved from the owning frame's plan.
    pub fn target_read(self) -> DmlTargetReadRequirement {
        self.target_read
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
/// The [`DmlSessionContext`] passed to [`new`](Self::new) describes the
/// PostgreSQL frame operation and whether that relation-local write logically
/// depends on reading the target. For MERGE the command is `CMD_MERGE`, while
/// the actual physical action for each row is still expressed by the callback
/// being invoked (`tuple_insert`, `tuple_update`, or `tuple_delete`).
///
/// # Reentrancy contract
///
/// A tuple callback ([`tuple_insert_slot`](Self::tuple_insert_slot),
/// [`multi_insert_slots`](Self::multi_insert_slots), the update/delete/lock
/// variants, etc.) MUST NOT synchronously re-enter the table-AM write path for
/// the *same* relation in the *same* DML frame before it returns. Concretely, a
/// callback must not call `table_tuple_insert` / `table_multi_insert` /
/// `table_tuple_update` / `table_tuple_delete` (or SPI / executor entry points
/// that do) against the relation it is currently writing.
///
/// The framework relies on this to hand each callback a unique
/// `&mut` to the per-relation session without a per-row runtime guard: the hot
/// path resolves the session once and reuses a cached pointer, which is sound
/// only while no second `&mut` to the same session can be created concurrently.
/// PostgreSQL's standard executor already upholds the contract — it runs
/// `table_tuple_*` to completion before index maintenance and AFTER triggers,
/// and nested trigger / SPI DML runs in a *new* ModifyTable frame — so normal
/// AMs need do nothing special. Nested DML against *other* relations is fine
/// when routed through the executor (it opens a new frame); doing raw table-AM
/// writes from inside a callback is what the contract forbids.
pub trait AmDmlSession {
    /// Construct the write session for a DML frame.
    ///
    /// `rel` is borrowed only for the duration of construction (symmetric with
    /// [`AmScanSession::new`]); the AM derives everything it needs from the
    /// handle (relation OID, file locator, WAL requirement, and any column
    /// layout) and must capture it into owned fields rather than retaining the
    /// handle. `context` carries the frame's PostgreSQL command type and target
    /// read requirement (see the trait docs for the MERGE note).
    fn new(rel: &RelationHandle, context: DmlSessionContext) -> AmResult<Self>
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
