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
//!   [`AmModifyQueryState`], [`AmModifyState`]) describe `&mut self` lifecycles
//!   tied to a scan, an index fetch, a Modify query, or one result relation.
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

use crate::TableAmRoutine;
use crate::access::mutation::trigger_rows::TriggerQueryState;
use crate::batch::ScanBatchDriver;
use crate::diag::{PgReportError, SqlStateError};
use crate::handles::{
    AttrWidthsHandle, BufferAccessStrategyHandle, BulkInsertStateHandle,
    CallbackStateHandle, IndexBuildCallbackHandle, IndexInfoHandle, ItemPointer,
    AnalyzeReadStreamHandle, OwnedScanKeys, ParallelTableScanDescHandle,
    RelFileLocator, RelationHandle, SampleScanStateHandle, ScanDirection,
    SnapshotHandle, TBMIterateResultHandle, TMIndexDeleteOpHandle,
    TableScanDescHandle, TupleTableSlotHandle, VacuumParamsHandle,
    ValidateIndexStateHandle, VarlenaHandle,
};
use crate::tuple::{Row, SlotColumns, TupleSlotBatch, TupleSlotRow};
use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use std::cell::RefCell;
use std::fmt::{Display, Formatter};
use std::rc::Rc;

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

/// First `ItemPointer` block number reserved for core-owned temporary rows.
/// Provider physical row-identity codecs must remain below this boundary.
pub const TRIGGER_ROW_BLOCK_BASE: u32 = 0xC000_0000;

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
    type ScanSession: AmScanSession;
    type IndexFetchSession: AmIndexFetchSession;
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
        crate::registry::build_table_am_routine::<Self>()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ScanCapabilities {
    pub tid_range: bool,
    pub bitmap: bool,
}

/// Type-safe view of PostgreSQL's `ScanOptions` bitmask.
///
/// Keeping the raw value private prevents access methods from scattering
/// bit arithmetic and PostgreSQL constants through their scan state machines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanFlags(u32);

impl ScanFlags {
    #[inline]
    pub const fn from_bits(bits: u32) -> Self {
        Self(bits)
    }

    #[inline]
    pub const fn bits(self) -> u32 {
        self.0
    }

    #[inline]
    pub const fn is_analyze(self) -> bool {
        self.0 & pg_sys::ScanOptions::SO_TYPE_ANALYZE != 0
    }

    #[inline]
    pub const fn is_seqscan(self) -> bool {
        self.0 & pg_sys::ScanOptions::SO_TYPE_SEQSCAN != 0
    }
}

/// Result of inspecting one candidate row during `ANALYZE`.
///
/// `live_delta` and `dead_delta` are contributions to PostgreSQL-owned
/// running totals, not replacement totals.  A visible row normally uses
/// `visible(1.0)`; storage engines using unequal scan units may provide a
/// statistically justified weight.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AnalyzeTupleOutcome {
    pub found: bool,
    pub live_delta: f64,
    pub dead_delta: f64,
}

impl AnalyzeTupleOutcome {
    #[inline]
    pub const fn visible(live_delta: f64) -> Self {
        Self {
            found: true,
            live_delta,
            dead_delta: 0.0,
        }
    }

    #[inline]
    pub const fn end_of_block() -> Self {
        Self {
            found: false,
            live_delta: 0.0,
            dead_delta: 0.0,
        }
    }
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

    /// Slot operations used after an ANALYZE callback has produced a sample.
    ///
    /// PostgreSQL allocates the slot using [`Self::slot_callbacks`], so an
    /// override must use the same slot layout and may only customize behavior
    /// that does not affect allocation or initialization.
    fn analyze_slot_callbacks() -> *const pg_sys::TupleTableSlotOps {
        Self::slot_callbacks()
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
        snapshot: Option<&SnapshotHandle>,
        pscan: Option<&ParallelTableScanDescHandle>,
        flags: ScanFlags,
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
        stream: &AnalyzeReadStreamHandle,
    ) -> AmResult<bool> {
        let _ = stream;
        unsupported_callback("scan_analyze_next_block")
    }

    fn scan_analyze_next_tuple(
        &mut self,
        oldest_xmin: pg_sys::TransactionId,
        out: &mut SlotColumns<'_>,
    ) -> AmResult<AnalyzeTupleOutcome> {
        let _ = (oldest_xmin, out);
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

/// Per-row INSERT context passed by the forked ModifyTable executor.
#[derive(Debug, Clone, Copy)]
pub struct MutationWriteContext {
    pub cid: pg_sys::CommandId,
    pub options: i32,
}

/// Per-row UPDATE context passed by the forked ModifyTable executor.
pub struct MutationUpdateContext<'a> {
    pub cid: pg_sys::CommandId,
    pub snapshot: &'a SnapshotHandle<'a>,
    pub crosscheck: Option<&'a SnapshotHandle<'a>>,
    pub wait: bool,
}

/// Per-row DELETE context passed by the forked ModifyTable executor.
pub struct MutationDeleteContext<'a> {
    pub cid: pg_sys::CommandId,
    pub snapshot: &'a SnapshotHandle<'a>,
    pub crosscheck: Option<&'a SnapshotHandle<'a>>,
    pub wait: bool,
    pub changing_partition: bool,
}

/// Storage-format-neutral result of a row mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationOutcome {
    Applied,
    /// A ModifyState in the current PostgreSQL transaction already processed
    /// this physical row.
    ///
    /// Providers report the physical-row fact and the modifying command's ID.
    /// The PostgreSQL adapter owns the conversion to `TM_SelfModified` and its
    /// `TM_FailureData`.
    AlreadyModifiedInCurrentTransaction {
        modifying_command_id: pg_sys::CommandId,
    },
    Deleted,
}

/// Row-level operations that one PostgreSQL ModifyTable node may execute.
///
/// A plain INSERT/UPDATE/DELETE contains exactly one operation. MERGE carries
/// the union of its concrete actions so providers can allocate only the sinks
/// and validation state that the planned statement can actually use.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ModifyActions(u8);

impl ModifyActions {
    pub const NONE: Self = Self(0);
    pub const INSERT: Self = Self(1 << 0);
    pub const UPDATE: Self = Self(1 << 1);
    pub const DELETE: Self = Self(1 << 2);

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub const fn may_insert(self) -> bool {
        self.0 & Self::INSERT.0 != 0
    }

    pub const fn may_update(self) -> bool {
        self.0 & Self::UPDATE.0 != 0
    }

    pub const fn may_delete(self) -> bool {
        self.0 & Self::DELETE.0 != 0
    }

    pub const fn writes_rows(self) -> bool {
        self.may_insert() || self.may_update()
    }

    pub const fn writes_position_deletes(self) -> bool {
        self.may_update() || self.may_delete()
    }
}

/// Typed, single-backend handle to AM state shared by every Lakebase
/// ModifyTable node in one PostgreSQL executor query.
pub(crate) struct ModifyQueryShared<Q: AmModifyQueryState> {
    provider: RefCell<Q>,
    trigger_rows: RefCell<TriggerQueryState>,
}

impl<Q: AmModifyQueryState> ModifyQueryShared<Q> {
    pub(crate) fn new() -> AmResult<Self> {
        Ok(Self {
            provider: RefCell::new(Q::new()?),
            trigger_rows: RefCell::new(TriggerQueryState::default()),
        })
    }
}

pub struct ModifyQueryState<Q: AmModifyQueryState> {
    inner: Rc<ModifyQueryShared<Q>>,
}

impl<Q: AmModifyQueryState> Clone for ModifyQueryState<Q> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl<Q: AmModifyQueryState> ModifyQueryState<Q> {
    /// Create an independent query-state owner.
    ///
    /// Normal ModifyTable execution acquires this handle from core's `EState`
    /// registry so sibling nodes share it. Independent write paths such as
    /// COPY can use this constructor because they have no target scan identity
    /// to share.
    pub fn new() -> AmResult<Self> {
        Ok(Self {
            inner: Rc::new(ModifyQueryShared::new()?),
        })
    }

    pub(crate) fn from_shared(inner: Rc<ModifyQueryShared<Q>>) -> Self {
        Self { inner }
    }

    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
    }

    /// Mutably borrow the AM query state for one short, non-reentrant operation.
    pub fn update<R>(
        &self,
        operation: impl FnOnce(&mut Q) -> AmResult<R>,
    ) -> AmResult<R> {
        let mut state = self.inner.provider.try_borrow_mut().map_err(|_| {
            PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "Modify query state is already borrowed",
            )
        })?;
        operation(&mut state)
    }

    /// Preserve one complete relation-shaped row for a queued AFTER ROW
    /// trigger in the core-owned query state.
    ///
    /// # Safety
    ///
    /// PostgreSQL must keep `tuple_desc` live for the query and `slot` live for
    /// this call.
    pub(crate) unsafe fn preserve_trigger_row<A: TableAccessMethod>(
        &self,
        relation_oid: pg_sys::Oid,
        tuple_desc: pg_sys::TupleDesc,
        slot: *mut pg_sys::TupleTableSlot,
    ) -> AmResult<ItemPointer> {
        let mut trigger_rows =
            self.inner.trigger_rows.try_borrow_mut().map_err(|_| {
                PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    "Modify query trigger-row state is already borrowed",
                )
            })?;
        unsafe { trigger_rows.preserve::<A>(relation_oid, tuple_desc, slot) }
    }
}

/// Storage-specific state shared by all ModifyTable nodes belonging to one
/// PostgreSQL executor query.
pub trait AmModifyQueryState: 'static {
    /// Storage source shared by a group of physical row identities, such as an
    /// Iceberg data-file path. The source is registered once per physical scan
    /// run, never once per logical row.
    type ScanIdentitySource<'a>;

    /// Compact source token returned to the scan after query-level registration.
    type RegisteredScanIdentity: Copy + 'static;

    /// Storage-specific per-row identity within a registered source.
    type ScanIdentity<'a>;

    fn new() -> AmResult<Self>
    where
        Self: Sized;

    /// Register a storage identity source in the query-level namespace for one
    /// result relation.
    fn register_scan_identity_source(
        &mut self,
        relation_oid: pg_sys::Oid,
        source: &Self::ScanIdentitySource<'_>,
    ) -> AmResult<Self::RegisteredScanIdentity>;

    /// Encode one row within a registered source into PostgreSQL's row-id
    /// carrier. This is deliberately stateless and remains on the per-row scan
    /// hot path. Core reserves `ItemPointer::block_number >= 0xC0000000` for
    /// query-local AFTER-trigger row identities; provider physical identities
    /// must stay below that range.
    fn encode_row_identity(
        source: Self::RegisteredScanIdentity,
        identity: &Self::ScanIdentity<'_>,
    ) -> AmResult<ItemPointer>;
}

/// Immutable context supplied when a relation-local modify state is created.
#[derive(Clone)]
pub struct ModifyStateContext<Q: AmModifyQueryState, C> {
    query_state: ModifyQueryState<Q>,
    cmd_type: pg_sys::CmdType::Type,
    actions: ModifyActions,
    scan_context: Option<C>,
}

impl<Q: AmModifyQueryState, C> ModifyStateContext<Q, C> {
    /// Construct a session whose writes do not depend on a target scan.
    pub fn independent(
        query_state: ModifyQueryState<Q>,
        cmd_type: pg_sys::CmdType::Type,
        actions: ModifyActions,
    ) -> Self {
        Self {
            query_state,
            cmd_type,
            actions,
            scan_context: None,
        }
    }

    /// Construct a session tied to the supplied Modify-scan context.
    pub fn target_read(
        query_state: ModifyQueryState<Q>,
        cmd_type: pg_sys::CmdType::Type,
        actions: ModifyActions,
        scan_context: C,
    ) -> Self {
        Self {
            query_state,
            cmd_type,
            actions,
            scan_context: Some(scan_context),
        }
    }

    /// PostgreSQL command type for the owning ModifyTable execution.
    pub fn cmd_type(&self) -> pg_sys::CmdType::Type {
        self.cmd_type
    }

    /// Concrete row operations present in the owning plan.
    pub fn actions(&self) -> ModifyActions {
        self.actions
    }

    /// The target-scan context, absent for independent INSERT/COPY.
    pub fn scan_context(&self) -> Option<&C> {
        self.scan_context.as_ref()
    }

    /// Consume this context into its independently owned parts.
    pub fn into_parts(
        self,
    ) -> (
        ModifyQueryState<Q>,
        pg_sys::CmdType::Type,
        ModifyActions,
        Option<C>,
    ) {
        (
            self.query_state,
            self.cmd_type,
            self.actions,
            self.scan_context,
        )
    }
}

/// Relation-local state owned directly by one Custom ModifyTable execution.
///
/// Abort paths are separate.  If PostgreSQL raises ERROR, aborts the
/// transaction, or rolls back to a savepoint, ResourceOwner cleanup drops the
/// execution state and unfinalized sessions receive [`abort_modify`](Self::abort_modify)
/// instead of `end_modify`.
pub trait AmModifyState {
    type QueryState: AmModifyQueryState;

    /// Storage-specific metadata captured by the Modify-purpose target scan.
    type ScanContext: Clone + PartialEq + 'static;

    /// Construct the write session for one result relation.
    ///
    /// `rel` is borrowed only for the duration of construction (symmetric with
    /// [`AmScanSession::new`]); the AM derives everything it needs from the
    /// handle (relation OID, file locator, WAL requirement, and any column
    /// layout) and must capture it into owned fields rather than retaining the
    /// handle. `context` carries the PostgreSQL command and, for target-reading
    /// mutation, the provider context captured by the Modify scan.
    fn new(
        rel: &RelationHandle,
        context: ModifyStateContext<Self::QueryState, Self::ScanContext>,
    ) -> AmResult<Self>
    where
        Self: Sized;

    /// Opens relation-local resources for the current execution.
    ///
    /// Typical implementations load table metadata, create file writers, and
    /// allocate buffers here. This is execution-scoped, not transaction-scoped: a
    /// single transaction may create many sessions across multiple statements,
    /// partitions, nested SPI calls, COPY frames, or MERGE frames.
    fn begin_modify(&mut self) -> AmResult<()>;

    /// Finishes the relation-local write session for a successful execution.
    ///
    /// A single PostgreSQL ModifyTable execution can host multiple AM sessions, for example
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

    /// Insert the final relation-shaped tuple produced by PostgreSQL.
    fn insert_slot(
        &mut self,
        new: TupleSlotRow<'_>,
        context: MutationWriteContext,
    ) -> AmResult<()> {
        let _ = (new, context);
        unsupported_callback("insert_slot")
    }

    /// Update a row using explicit physical identity plus OLD and final NEW
    /// relation-shaped slots.
    fn update_slot(
        &mut self,
        row_id: ItemPointer,
        old: TupleSlotRow<'_>,
        new: TupleSlotRow<'_>,
        context: MutationUpdateContext<'_>,
    ) -> AmResult<MutationOutcome> {
        let _ = (row_id, old, new, context);
        unsupported_callback("update_slot")
    }

    /// Delete a row using explicit physical identity.
    ///
    /// PostgreSQL executor consumers such as row triggers, transition tables,
    /// and `RETURNING` obtain OLD from the planner-visible `wholerow`; storage
    /// deletion does not receive an unconditional copy of the business row.
    fn delete_slot(
        &mut self,
        row_id: ItemPointer,
        context: MutationDeleteContext<'_>,
    ) -> AmResult<MutationOutcome> {
        let _ = (row_id, context);
        unsupported_callback("delete_slot")
    }
}

/// Relation-local COPY FROM state. COPY bypasses ModifyTable and therefore has
/// a separate utility-scoped lifecycle and callback surface.
pub trait AmCopySession {
    fn new(rel: &RelationHandle) -> AmResult<Self>
    where
        Self: Sized;

    fn begin_copy(&mut self) -> AmResult<()>;

    fn end_copy(&mut self) -> AmResult<()>;

    fn abort_copy(&mut self) {}

    fn tuple_insert_slot(
        &mut self,
        row: TupleSlotRow<'_>,
        cid: pg_sys::CommandId,
        options: i32,
        bistate: Option<&BulkInsertStateHandle>,
    ) -> AmResult<()> {
        let _ = (row, cid, options, bistate);
        unsupported_callback("copy tuple_insert_slot")
    }

    fn multi_insert_slots(
        &mut self,
        rows: TupleSlotBatch<'_>,
        cid: pg_sys::CommandId,
        options: i32,
        bistate: Option<&BulkInsertStateHandle>,
    ) -> AmResult<()> {
        let _ = (rows, cid, options, bistate);
        unsupported_callback("copy multi_insert_slots")
    }

    fn finish_bulk_insert(&mut self, options: i32) -> AmResult<()> {
        let _ = options;
        Ok(())
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
