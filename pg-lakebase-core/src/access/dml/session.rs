//! DML frame and per-relation session lifecycle.
//!
//! PostgreSQL's table-AM vtable only exposes per-tuple operations, so the
//! framework scopes DML state to a write "frame" (a ModifyTable node or a COPY
//! FROM) rather than to a transaction or statement. One frame can touch several
//! relations (partition routing, MERGE). A [`DmlFrame`] owns the relation-local
//! [`ModifySession`]s: the first table-AM callback for a relation lazily creates
//! the session and calls `begin_modify()`; successful completion calls
//! `end_modify()` once per touched relation. ERROR / abort /
//! rollback-to-savepoint instead run `abort_modify()` via ResourceOwner cleanup
//! (the non-local exits Rust cannot observe directly). A process-global "last
//! used session" cannot model nested SPI / data-modifying CTEs / partitioned
//! writes / COPY, so the current frame is tracked explicitly on a stack.
//!
//! ## Per-row fast path
//!
//! [`with_current_relation_session`] is on the INSERT hot path. It keeps a
//! frame-scoped memo ([`HotState`]) of the last `(frame, relation, session)` and
//! reuses it directly when the next callback targets the same frame + relation —
//! no HashMap lookup, no frame move, one TLS access for both the current frame
//! and the memo. The memo is keyed on the *current* frame and cleared by every
//! frame teardown, so it never aliases another frame's session for the same
//! relid or outlives the frame it points into.
//!
//! ## Reentrancy contract (no per-row guard)
//!
//! Session access hands the callback a `&mut ModifySession` from a pointer into
//! the relation's `Box<ModifySession>` (heap-stable across `Vec`/`FRAMES` growth;
//! the frame is not torn down mid-callback). Uniqueness of that `&mut` rests on a
//! *contract*, not a runtime check: an [`AmDmlSession`] tuple callback must not
//! synchronously re-enter the table-AM write path for the same frame.
//! PostgreSQL's executor upholds this — it completes `table_tuple_*` before
//! indexes / AFTER triggers, and nested trigger / SPI DML runs in a new frame —
//! so the hot path spends nothing defending a case the contract rules out.

use crate::api::{
    AmDmlSession, DmlSessionContext, DmlTargetReadRequirement, TableAccessMethod,
};
use crate::diag::PgReportError;
use crate::handles::RelationHandle;
use crate::resource::{self, ResourceHandle};
use crate::tuple::Row;
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::ffi::c_void;
use std::marker::PhantomData;
use std::ptr::NonNull;

use super::erased_session::{ErasedModifySession, ErasedModifySessionAdapter};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum FrameKey {
    ModifyTable(NonNull<pg_sys::PlanState>),
    CopyFrom(u64),
}

/// Opaque identifier for the current PostgreSQL DML frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DmlFrameId {
    tag: u8,
    value: u64,
}

impl DmlFrameId {
    fn from_key(key: FrameKey) -> Self {
        match key {
            FrameKey::ModifyTable(planstate) => Self {
                tag: 0,
                value: planstate.as_ptr() as usize as u64,
            },
            FrameKey::CopyFrom(copy_id) => Self {
                tag: 1,
                value: copy_id,
            },
        }
    }
}

/// Scoped view of the target scan plan for the current DML frame.
///
/// Values of this type are only provided to the callback passed to
/// [`with_current_dml_target_plan`]. The frame lifetime prevents the view from
/// escaping while keeping PostgreSQL-owned expression pointers private.
#[derive(Debug)]
pub struct DmlTargetPlan<'frame> {
    rel_oid: pg_sys::Oid,
    scan_relid: core::ffi::c_int,
    qual: *mut pg_sys::List,
    _frame: PhantomData<&'frame DmlTargetPlanScope>,
}

impl<'frame> DmlTargetPlan<'frame> {
    #[inline]
    fn new(target: TargetScanMatch, _scope: &'frame DmlTargetPlanScope) -> Self {
        Self {
            rel_oid: target.rel_oid,
            scan_relid: target.scan_relid,
            qual: target.qual,
            _frame: PhantomData,
        }
    }

    #[inline]
    pub(crate) fn rel_oid(&self) -> pg_sys::Oid {
        self.rel_oid
    }

    #[inline]
    pub(crate) fn scan_relid(&self) -> core::ffi::c_int {
        self.scan_relid
    }

    #[inline]
    pub(crate) fn qual(&self) -> *mut pg_sys::List {
        self.qual
    }
}

struct DmlTargetPlanScope;

#[derive(Debug, Clone, Copy)]
struct TargetScanMatch {
    rel_oid: pg_sys::Oid,
    scan_relid: core::ffi::c_int,
    qual: *mut pg_sys::List,
}

impl TargetScanMatch {
    #[inline]
    fn new(
        rel_oid: pg_sys::Oid,
        scan_relid: core::ffi::c_int,
        qual: *mut pg_sys::List,
    ) -> Self {
        Self {
            rel_oid,
            scan_relid,
            qual,
        }
    }
}

/// A frame on the current write-frame stack: its key plus the command type
/// resolved when the frame was pushed (the ModifyTable node's `operation`, or
/// `CMD_INSERT` for COPY). Carrying `cmd_type` with the push lets the per-row
/// slow path lazily create the `DmlFrame` without querying the executor
/// wrapper, keeping the `session` → `modifytable_wrapper` dependency
/// one-directional.
#[derive(Clone, Copy)]
struct FrameStackEntry {
    key: FrameKey,
    cmd_type: pg_sys::CmdType::Type,
}

impl FrameStackEntry {
    #[inline]
    fn modifies_rows_in_place(self) -> bool {
        matches!(
            self.cmd_type,
            pg_sys::CmdType::CMD_UPDATE
                | pg_sys::CmdType::CMD_DELETE
                | pg_sys::CmdType::CMD_MERGE
        )
    }

    /// The frame id when this frame modifies `rel_oid`'s rows in place
    /// (`UPDATE`/`DELETE`/`MERGE`) and `rel_oid` is one of its result
    /// relations; otherwise `None`.
    ///
    /// A scan of such a relation must carry stable per-row identity (`ctid`)
    /// because the matching tuple callbacks address rows by it. `INSERT` and
    /// `COPY FROM` frames never scan a row-identity target, and source-only
    /// relations of an `UPDATE ... FROM` are not result relations, so both are
    /// rejected here.
    fn row_identity_target(self, rel_oid: pg_sys::Oid) -> Option<DmlFrameId> {
        if !self.modifies_rows_in_place() {
            return None;
        }

        let FrameKey::ModifyTable(node) = self.key else {
            return None;
        };

        // SAFETY: this entry is on the current frame stack, so PostgreSQL is
        // executing its ModifyTable node and the `ModifyTableState` is live.
        let targets = unsafe { ModifyTableNode(node).targets_relation(rel_oid) };
        targets.then(|| DmlFrameId::from_key(self.key))
    }

    fn target_plan(self, rel_oid: pg_sys::Oid) -> Option<TargetScanMatch> {
        if !self.modifies_rows_in_place() {
            return None;
        }

        let FrameKey::ModifyTable(node) = self.key else {
            return None;
        };

        // SAFETY: this entry is on the current frame stack, so PostgreSQL is
        // executing its ModifyTable node and the child PlanState tree is live.
        unsafe { ModifyTableNode(node).target_plan(rel_oid) }
    }

    /// Resolve the logical target-read requirement before constructing a
    /// relation-local AM session.
    ///
    /// INSERT/COPY are independent appends. UPDATE and DELETE always require a
    /// target read. MERGE is plan-sensitive: PostgreSQL may remove an
    /// unreachable target scan (for example `ON FALSE`), leaving only an
    /// independent insert action.
    fn session_context(
        self,
        rel_oid: pg_sys::Oid,
    ) -> Result<DmlSessionContext, PgReportError> {
        let target_read = match self.cmd_type {
            pg_sys::CmdType::CMD_INSERT => DmlTargetReadRequirement::Independent,
            pg_sys::CmdType::CMD_UPDATE | pg_sys::CmdType::CMD_DELETE => {
                DmlTargetReadRequirement::ReadRequired
            }
            pg_sys::CmdType::CMD_MERGE => {
                let FrameKey::ModifyTable(node) = self.key else {
                    return Err(internal_error(
                        "MERGE DML session is not owned by a ModifyTable frame",
                    ));
                };
                // SAFETY: this entry is the current frame, so its initialized
                // ModifyTableState and child PlanState tree remain live.
                let target_scan =
                    unsafe { ModifyTableNode(node).target_scan(rel_oid) }
                        .ok_or_else(|| {
                            internal_error(
                                "MERGE DML relation is not a unique result relation",
                            )
                        })?;
                match target_scan {
                    TargetScanSearch::Missing => {
                        DmlTargetReadRequirement::Independent
                    }
                    TargetScanSearch::Unique(_)
                    | TargetScanSearch::Present
                    | TargetScanSearch::Ambiguous => {
                        DmlTargetReadRequirement::ReadRequired
                    }
                }
            }
            _ if self.modifies_rows_in_place() => {
                DmlTargetReadRequirement::ReadRequired
            }
            _ => DmlTargetReadRequirement::Independent,
        };
        Ok(DmlSessionContext::new(self.cmd_type, target_read))
    }
}

/// Read-only view over a live `ModifyTableState` reached through a frame key.
#[derive(Clone, Copy)]
struct ModifyTableNode(NonNull<pg_sys::PlanState>);

impl ModifyTableNode {
    /// Whether `rel_oid` is one of the node's result (target) relations.
    ///
    /// # Safety
    ///
    /// The node must be live: only valid while its frame is on the stack and
    /// PostgreSQL is executing the node, so the `resultRelInfo` array it points
    /// to is initialized.
    unsafe fn targets_relation(self, rel_oid: pg_sys::Oid) -> bool {
        let mtstate = self.0.as_ptr() as *const pg_sys::ModifyTableState;
        let result_rel_info = unsafe { (*mtstate).resultRelInfo };
        if result_rel_info.is_null() {
            return false;
        }
        let nrels = usize::try_from(unsafe { (*mtstate).mt_nrels }).unwrap_or(0);

        // SAFETY: `resultRelInfo` points to `mt_nrels` contiguous, initialized
        // `ResultRelInfo`s for a live ModifyTable node.
        let result_rels =
            unsafe { std::slice::from_raw_parts(result_rel_info, nrels) };
        result_rels.iter().any(|info| {
            let relation = info.ri_RelationDesc;
            // SAFETY: each result relation is open (non-null `Relation`) during
            // execution; `rd_id` is a plain field read.
            !relation.is_null() && unsafe { (*relation).rd_id } == rel_oid
        })
    }

    /// Restriction quals from the unique target scan for `rel_oid`.
    ///
    /// # Safety
    ///
    /// The node and its initialized child PlanState tree must be live.
    unsafe fn target_plan(self, rel_oid: pg_sys::Oid) -> Option<TargetScanMatch> {
        match unsafe { self.target_scan(rel_oid) }? {
            TargetScanSearch::Unique(target) => Some(target),
            TargetScanSearch::Missing
            | TargetScanSearch::Present
            | TargetScanSearch::Ambiguous => None,
        }
    }

    /// Search the initialized child plan for scans of one unique result
    /// relation. `Missing` is meaningful for MERGE because PostgreSQL can prove
    /// the target unreachable and replace it with a dummy relation.
    ///
    /// # Safety
    ///
    /// The node and its initialized child PlanState tree must be live.
    unsafe fn target_scan(self, rel_oid: pg_sys::Oid) -> Option<TargetScanSearch> {
        let target = unsafe { self.target_identity(rel_oid) }?;
        let planstate = self.0.as_ptr();
        let outer = unsafe { (*planstate).lefttree };
        let mut finder = TargetScanFinder::new(target);
        unsafe { finder.visit_tree(outer) };
        Some(finder.finish())
    }

    /// Resolve one result relation to both its physical OID and range-table
    /// index. Matching both values distinguishes a DML target from a self-join
    /// source that scans the same physical relation.
    ///
    /// # Safety
    ///
    /// The wrapped `ModifyTableState`, its plan, and its `resultRelInfo` array
    /// must be initialized and live for this call.
    unsafe fn target_identity(
        self,
        rel_oid: pg_sys::Oid,
    ) -> Option<DmlTargetIdentity> {
        let mtstate = self.0.as_ptr().cast::<pg_sys::ModifyTableState>();
        let result_rel_info = unsafe { (*mtstate).resultRelInfo };
        let plan = unsafe { (*self.0.as_ptr()).plan }.cast::<pg_sys::ModifyTable>();
        if result_rel_info.is_null() || plan.is_null() {
            return None;
        }

        let result_relations = unsafe { (*plan).resultRelations };
        let nrels = usize::try_from(unsafe { (*mtstate).mt_nrels }).ok()?;
        if result_relations.is_null()
            || usize::try_from(unsafe { pg_sys::list_length(result_relations) })
                .ok()?
                != nrels
        {
            return None;
        }

        let mut found = None;
        for index in 0..nrels {
            let info = unsafe { &*result_rel_info.add(index) };
            let relation = info.ri_RelationDesc;
            if relation.is_null() || unsafe { (*relation).rd_id } != rel_oid {
                continue;
            }
            if found.is_some() {
                return None;
            }
            let list_index = core::ffi::c_int::try_from(index).ok()?;
            let raw_scan_relid =
                unsafe { pg_sys::list_nth_int(result_relations, list_index) };
            let scan_relid = pg_sys::Index::try_from(raw_scan_relid).ok()?;
            if scan_relid == 0 {
                return None;
            }
            found = Some(DmlTargetIdentity {
                rel_oid,
                scan_relid,
            });
        }
        found
    }
}

#[derive(Debug, Clone, Copy)]
struct DmlTargetIdentity {
    rel_oid: pg_sys::Oid,
    scan_relid: pg_sys::Index,
}

struct TargetScanFinder {
    target: DmlTargetIdentity,
    present: bool,
    found: Option<TargetScanMatch>,
    ambiguous: bool,
}

#[derive(Debug, Clone, Copy)]
enum TargetScanSearch {
    Missing,
    Present,
    Unique(TargetScanMatch),
    Ambiguous,
}

#[derive(Debug, Clone, Copy)]
enum TargetScanCandidate {
    Usable(TargetScanMatch),
    Unsupported,
}

impl TargetScanFinder {
    #[inline]
    fn new(target: DmlTargetIdentity) -> Self {
        Self {
            target,
            present: false,
            found: None,
            ambiguous: false,
        }
    }

    /// Visit one node and all descendants through PostgreSQL's PlanState walker.
    ///
    /// # Safety
    ///
    /// `planstate` must be NULL or part of the live target ModifyTable tree.
    unsafe fn visit_tree(&mut self, planstate: *mut pg_sys::PlanState) {
        if planstate.is_null() || self.ambiguous {
            return;
        }

        if let Some(target) = unsafe { self.scan_target(planstate) } {
            self.record(target);
            if self.ambiguous {
                return;
            }
        }

        // SAFETY: `planstate` belongs to the live ModifyTable child tree and
        // `self` remains valid for the synchronous PostgreSQL walker call.
        unsafe {
            pg_sys::planstate_tree_walker_impl(
                planstate,
                Some(target_scan_walker),
                (self as *mut Self).cast::<c_void>(),
            );
        }
    }

    /// # Safety
    ///
    /// `planstate` must point to a live PlanState whose tag matches its concrete
    /// allocation.
    unsafe fn scan_target(
        &self,
        planstate: *mut pg_sys::PlanState,
    ) -> Option<TargetScanCandidate> {
        let tag = unsafe { (*planstate).type_ };
        match tag {
            pg_sys::NodeTag::T_SeqScanState => unsafe {
                Self::seq_scan_target(planstate, self.target)
                    .map(TargetScanCandidate::Usable)
            },
            pg_sys::NodeTag::T_CustomScanState => unsafe {
                Self::custom_scan_target(planstate, self.target)
            },
            pg_sys::NodeTag::T_IndexScanState
            | pg_sys::NodeTag::T_IndexOnlyScanState
            | pg_sys::NodeTag::T_BitmapHeapScanState
            | pg_sys::NodeTag::T_TidScanState
            | pg_sys::NodeTag::T_TidRangeScanState
            | pg_sys::NodeTag::T_SampleScanState => unsafe {
                Self::non_projectable_scan_target(planstate, self.target)
            },
            _ => None,
        }
    }

    /// # Safety
    ///
    /// `planstate` must point to a live `SeqScanState`.
    unsafe fn seq_scan_target(
        planstate: *mut pg_sys::PlanState,
        target: DmlTargetIdentity,
    ) -> Option<TargetScanMatch> {
        let state = planstate.cast::<pg_sys::SeqScanState>();
        let scan_state = unsafe { &(*state).ss };
        let plan = unsafe { (*planstate).plan };
        if plan.is_null() {
            return None;
        }
        let scan = plan.cast::<pg_sys::SeqScan>();
        let scan_relid = unsafe { (*scan).scan.scanrelid };
        unsafe { Self::target_from_scan_state(scan_state, target, scan_relid, plan) }
    }

    /// # Safety
    ///
    /// `planstate` must point to a live `CustomScanState`.
    unsafe fn custom_scan_target(
        planstate: *mut pg_sys::PlanState,
        target: DmlTargetIdentity,
    ) -> Option<TargetScanCandidate> {
        let state = planstate.cast::<pg_sys::CustomScanState>();
        let scan_state = unsafe { &(*state).ss };
        let plan = unsafe { (*planstate).plan };
        if plan.is_null() {
            return None;
        }
        let scan = plan.cast::<pg_sys::CustomScan>();
        let scan_relid = unsafe { (*scan).scan.scanrelid };
        let matches_target = unsafe {
            Self::scan_state_matches_target(scan_state, target, scan_relid)
        };
        if scan_relid == 0 || !matches_target {
            return None;
        }
        if unsafe { !(*scan).custom_scan_tlist.is_null() } {
            return Some(TargetScanCandidate::Unsupported);
        }
        unsafe {
            Self::target_from_scan_state(scan_state, target, scan_relid, plan)
                .map(TargetScanCandidate::Usable)
        }
    }

    /// Detect target scans whose quals cannot currently be exposed through
    /// [`DmlTargetPlan`]. They still prove that the DML depends on a physical
    /// target read and must never be mistaken for a planner-elided target.
    ///
    /// # Safety
    ///
    /// `planstate` must be one of the standard scan-state tags matched by
    /// [`Self::scan_target`].
    unsafe fn non_projectable_scan_target(
        planstate: *mut pg_sys::PlanState,
        target: DmlTargetIdentity,
    ) -> Option<TargetScanCandidate> {
        let scan_state = unsafe { &*planstate.cast::<pg_sys::ScanState>() };
        let plan = unsafe { (*planstate).plan };
        if plan.is_null() {
            return None;
        }
        let scan_relid = unsafe { (*plan.cast::<pg_sys::Scan>()).scanrelid };
        let matches_target = unsafe {
            Self::scan_state_matches_target(scan_state, target, scan_relid)
        };
        matches_target.then_some(TargetScanCandidate::Unsupported)
    }

    /// # Safety
    ///
    /// `scan_state` and its current relation must belong to the scan identified
    /// by `scan_relid`.
    unsafe fn scan_state_matches_target(
        scan_state: &pg_sys::ScanState,
        target: DmlTargetIdentity,
        scan_relid: pg_sys::Index,
    ) -> bool {
        let relation = scan_state.ss_currentRelation;
        !relation.is_null()
            && unsafe { (*relation).rd_id } == target.rel_oid
            && scan_relid == target.scan_relid
    }

    /// # Safety
    ///
    /// `scan_state`, `plan`, and its current relation must belong to the same
    /// live executor scan node.
    unsafe fn target_from_scan_state(
        scan_state: &pg_sys::ScanState,
        target: DmlTargetIdentity,
        scan_relid: pg_sys::Index,
        plan: *mut pg_sys::Plan,
    ) -> Option<TargetScanMatch> {
        let matches_target = unsafe {
            Self::scan_state_matches_target(scan_state, target, scan_relid)
        };
        if !matches_target {
            return None;
        }
        let scan_relid = core::ffi::c_int::try_from(scan_relid).ok()?;
        Some(TargetScanMatch::new(target.rel_oid, scan_relid, unsafe {
            (*plan).qual
        }))
    }

    fn record(&mut self, candidate: TargetScanCandidate) {
        if self.present {
            self.ambiguous = true;
        } else {
            self.present = true;
            if let TargetScanCandidate::Usable(target) = candidate {
                self.found = Some(target);
            }
        }
    }

    #[inline]
    fn finish(self) -> TargetScanSearch {
        if self.ambiguous {
            TargetScanSearch::Ambiguous
        } else if let Some(target) = self.found {
            TargetScanSearch::Unique(target)
        } else if self.present {
            TargetScanSearch::Present
        } else {
            TargetScanSearch::Missing
        }
    }
}

/// PostgreSQL PlanState walker callback.
///
/// # Safety
///
/// `planstate` must be a live executor node and `context` must be the
/// `TargetScanFinder` supplied by [`TargetScanFinder::visit_tree`].
unsafe extern "C-unwind" fn target_scan_walker(
    planstate: *mut pg_sys::PlanState,
    context: *mut c_void,
) -> bool {
    // SAFETY: `context` is the live TargetScanFinder supplied by visit_tree;
    // PostgreSQL invokes the callback synchronously.
    let finder = unsafe { &mut *context.cast::<TargetScanFinder>() };
    unsafe { finder.visit_tree(planstate) };
    finder.ambiguous
}

struct DmlFrame {
    key: FrameKey,
    cmd_type: pg_sys::CmdType::Type,
    resource_handle: ResourceHandle,
    rel_index: HashMap<pg_sys::Oid, usize>,
    sessions: Vec<(pg_sys::Oid, Box<ModifySession>)>,
    cleanup_callbacks: Vec<Box<dyn FnOnce() + 'static>>,
}

// Object-safe wrapper over the AM session. `finalized` records whether the
// success path ran `end_modify()`; dropping an unfinalized session aborts it
// (the ERROR / abort / rollback path).
pub(super) struct ModifySession {
    pub(super) state: Box<dyn ErasedModifySession>,
    pub(super) row_buffer: Row,
    finalized: bool,
}

impl ModifySession {
    fn new<T>(state: T) -> Self
    where
        T: AmDmlSession + 'static,
    {
        Self {
            state: Box::new(ErasedModifySessionAdapter::<T>::new(state)),
            row_buffer: Row::new(),
            finalized: false,
        }
    }

    pub(super) fn finish_bulk_insert(
        &mut self,
        options: ::core::ffi::c_int,
    ) -> Result<(), PgReportError> {
        self.state.finish_bulk_insert(options)
    }

    fn finalize_success(&mut self) -> Result<(), PgReportError> {
        self.state.end_modify()?;
        self.finalized = true;
        Ok(())
    }

    fn abort_cleanup(&mut self) {
        self.state.abort_modify();
    }
}

impl Drop for ModifySession {
    fn drop(&mut self) {
        if !self.finalized {
            self.abort_cleanup();
        }
    }
}

impl DmlFrame {
    fn new(key: FrameKey, cmd_type: pg_sys::CmdType::Type) -> Self {
        // Each frame owns a ResourceOwner entry.  PostgreSQL calls resource
        // release callbacks for abort and subtransaction rollback even when
        // control leaves Rust via ERROR/longjmp.  On success `finish_frame`
        // explicitly forgets this handle before calling `end_modify()`.
        let resource_handle =
            resource::remember_resource(move || abort_frame_and_remove_stack(key));

        Self {
            key,
            cmd_type,
            resource_handle,
            rel_index: HashMap::new(),
            sessions: Vec::new(),
            cleanup_callbacks: Vec::new(),
        }
    }

    fn session_index(&self, relid: pg_sys::Oid) -> Option<usize> {
        self.rel_index.get(&relid).copied()
    }
}

impl Drop for DmlFrame {
    fn drop(&mut self) {
        // Relation sessions own the state that consumes frame-scoped auxiliary
        // data. Finalize or abort them before releasing that data. `clear`
        // makes the ordering explicit instead of relying on struct field order.
        self.sessions.clear();
        for cleanup in self.cleanup_callbacks.drain(..).rev() {
            cleanup();
        }
    }
}

/// Per-row hot-path state, merged into one thread-local so the fast path reads
/// the current frame and the cached session in a single TLS access.
#[derive(Clone, Copy)]
struct HotState {
    /// Shadow of `CURRENT_FRAME_STACK`'s top, resynced on every push/pop, so the
    /// per-row path resolves the current frame (and its command type) without
    /// borrowing the `Vec`.
    frame_top: Option<FrameStackEntry>,
    /// Memo of the last resolved `(frame, relation, session)`; see the module
    /// "Per-row fast path" / "Reentrancy contract" sections for why it is sound.
    last_session: Option<(FrameKey, pg_sys::Oid, NonNull<ModifySession>)>,
}

impl HotState {
    const EMPTY: Self = Self {
        frame_top: None,
        last_session: None,
    };
}

thread_local! {
    // Active DML frames. Tuple callbacks create them lazily; frame-cleanup
    // registration may create them eagerly. Removing a frame drops its sessions
    // and then runs its auxiliary cleanup callbacks.
    static FRAMES: RefCell<HashMap<FrameKey, DmlFrame>> =
        RefCell::new(HashMap::new());
    // Current write-frame stack.  Nested SPI DML and trigger DML naturally push
    // another ModifyTable frame while the outer frame is suspended.
    static CURRENT_FRAME_STACK: RefCell<Vec<FrameStackEntry>> =
        const { RefCell::new(Vec::new()) };
    // COPY FROM frames are created by the utility hook rather than a PlanState,
    // so a separate stack identifies which COPY frame should finish in on_post.
    static COPY_FRAME_STACK: RefCell<Vec<FrameKey>> = const { RefCell::new(Vec::new()) };
    // Merged per-row hot-path state (current frame top + last-session memo).
    static HOT_STATE: Cell<HotState> = const { Cell::new(HotState::EMPTY) };
    static NEXT_COPY_ID: Cell<u64> = const { Cell::new(1) };
}

pub(super) fn internal_error(message: impl Into<String>) -> PgReportError {
    PgReportError::from_message(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, message)
}

fn feature_not_supported(message: impl Into<String>) -> PgReportError {
    PgReportError::from_message(
        PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
        message,
    )
}

fn next_copy_id() -> u64 {
    NEXT_COPY_ID.with(|next| {
        let id = next.get();
        next.set(id.wrapping_add(1).max(1));
        id
    })
}

/// Resync the hot-path frame top from the stack's current top. Called after
/// every `CURRENT_FRAME_STACK` mutation.
fn publish_current_top(stack: &[FrameStackEntry]) {
    HOT_STATE.with(|hot| {
        let mut state = hot.get();
        state.frame_top = stack.last().copied();
        hot.set(state);
    });
}

fn remove_key_from_stacks(key: FrameKey) {
    CURRENT_FRAME_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        stack.retain(|entry| entry.key != key);
        publish_current_top(&stack);
    });
    COPY_FRAME_STACK.with(|stack| stack.borrow_mut().retain(|k| *k != key));
}

pub(super) fn abort_frame_and_remove_stack(key: FrameKey) {
    // ResourceOwner uses this path for ERROR/abort/rollback-to-savepoint.  A
    // dropped frame drops every unfinalized ModifySession, whose Drop calls
    // abort_modify().  Stack removal is by retain rather than pop because COPY
    // errors and subtransaction rollback can unwind non-locally through nested
    // frames.
    last_session_invalidate(key);
    let frame = FRAMES.with(|frames| frames.borrow_mut().remove(&key));
    drop(frame);
    remove_key_from_stacks(key);
}

fn ensure_frame_exists(
    key: FrameKey,
    cmd_type: pg_sys::CmdType::Type,
) -> Result<(), PgReportError> {
    FRAMES.with(|frames| {
        let mut frames = frames.borrow_mut();
        let frame = frames
            .entry(key)
            .or_insert_with(|| DmlFrame::new(key, cmd_type));
        debug_assert_eq!(frame.cmd_type, cmd_type);
        Ok(())
    })
}

/// Pointer to the `key` frame's session for `relid`, or `None` if the frame has
/// no session for it yet. The pointer targets the session's `Box` allocation,
/// whose address is stable across `sessions` `Vec` growth and `FRAMES`
/// rehashing, so it stays valid after this `FRAMES` borrow is released.
fn frame_session_ptr(
    key: FrameKey,
    relid: pg_sys::Oid,
) -> Result<Option<NonNull<ModifySession>>, PgReportError> {
    FRAMES.with(|frames| {
        let mut frames = frames.borrow_mut();
        let frame = frames
            .get_mut(&key)
            .ok_or_else(|| internal_error("DML frame missing"))?;
        Ok(frame
            .session_index(relid)
            .map(|index| NonNull::from(frame.sessions[index].1.as_mut())))
    })
}

fn insert_session(
    key: FrameKey,
    relid: pg_sys::Oid,
    session: Box<ModifySession>,
) -> Result<(), PgReportError> {
    FRAMES.with(|frames| {
        let mut frames = frames.borrow_mut();
        let frame = frames.get_mut(&key).ok_or_else(|| {
            internal_error("DML frame missing while inserting session")
        })?;

        if frame.rel_index.contains_key(&relid) {
            return Ok(());
        }

        let index = frame.sessions.len();
        frame.rel_index.insert(relid, index);
        frame.sessions.push((relid, session));
        Ok(())
    })
}

/// Record the resolved session in the memo. The matching read is inline in
/// [`with_current_relation_session`]'s fast path; `last_session_invalidate`
/// clears it on frame teardown so it never outlives its frame.
fn last_session_store(
    key: FrameKey,
    relid: pg_sys::Oid,
    session: NonNull<ModifySession>,
) {
    HOT_STATE.with(|hot| {
        let mut state = hot.get();
        state.last_session = Some((key, relid, session));
        hot.set(state);
    });
}

fn last_session_invalidate(key: FrameKey) {
    HOT_STATE.with(|hot| {
        let mut state = hot.get();
        if matches!(state.last_session, Some((cached_key, _, _)) if cached_key == key)
        {
            state.last_session = None;
            hot.set(state);
        }
    });
}

/// Dispatch `f` with `&mut` access to the cached `session` pointer.
///
/// # Safety
///
/// `session` must point to the live `Box<ModifySession>` owned by the current
/// frame (address-stable across `Vec`/`FRAMES` growth; not torn down during the
/// callback). Uniqueness of the `&mut` relies on the module-level reentrancy
/// contract — no synchronous same-frame re-entry — so no second `&mut` to this
/// session can exist while `f` runs.
unsafe fn dispatch_to_session<R>(
    mut session: NonNull<ModifySession>,
    f: impl FnOnce(&mut ModifySession) -> Result<R, PgReportError>,
) -> Result<R, PgReportError> {
    f(unsafe { session.as_mut() })
}

fn create_session<A>(
    rel: pg_sys::Relation,
    context: DmlSessionContext,
) -> Result<Box<ModifySession>, PgReportError>
where
    A: TableAccessMethod,
{
    unsafe {
        // MERGE passes CMD_MERGE here even though later callbacks may be
        // insert/update/delete depending on the matched source row.
        let rel_handle = RelationHandle::from_raw(rel);
        let mut instance =
            <A::DmlSession as AmDmlSession>::new(&rel_handle, context)?;
        instance.begin_modify()?;

        Ok(Box::new(ModifySession::new::<A::DmlSession>(instance)))
    }
}

/// Resolve the `key` frame's session for `rel`, creating it (and running the
/// AM's `begin_modify`) on first touch. The AM construction runs outside any
/// `FRAMES` borrow so it can re-enter the registry safely.
fn resolve_session_ptr<A>(
    key: FrameKey,
    rel: pg_sys::Relation,
    relid: pg_sys::Oid,
    context: DmlSessionContext,
) -> Result<NonNull<ModifySession>, PgReportError>
where
    A: TableAccessMethod,
{
    if let Some(ptr) = frame_session_ptr(key, relid)? {
        return Ok(ptr);
    }
    let session = create_session::<A>(rel, context)?;
    insert_session(key, relid, session)?;
    frame_session_ptr(key, relid)?
        .ok_or_else(|| internal_error("DML session missing after insert"))
}

pub(super) fn with_current_relation_session<A, R>(
    rel: pg_sys::Relation,
    f: impl FnOnce(&mut ModifySession) -> Result<R, PgReportError>,
) -> Result<R, PgReportError>
where
    A: TableAccessMethod,
{
    unsafe {
        // Only valid inside a managed frame; unsupported paths (CTAS /
        // DestReceiver writes) fail here rather than creating unscoped state.
        let relid = (*rel).rd_id;

        // One TLS access for the whole fast path: read the merged hot state once.
        let hot = HOT_STATE.with(|hot| hot.get());
        let entry = hot.frame_top.ok_or_else(|| {
            feature_not_supported(
                "DML called outside a managed ModifyTable or COPY FROM frame",
            )
        })?;
        let key = entry.key;

        // Fast path: the previous callback resolved the same frame + relation.
        if let Some((cached_key, cached_relid, session)) = hot.last_session
            && cached_key == key
            && cached_relid == relid
        {
            return dispatch_to_session(session, f);
        }

        // Slow path: first row for this (frame, relation) — resolve (creating the
        // session and running `begin_modify` on first touch), then memoize. The
        // command type travels with the frame push, so the slow path never has
        // to query the executor wrapper for it.
        let cmd_type = entry.cmd_type;
        ensure_frame_exists(key, cmd_type)?;
        let context = entry.session_context(relid)?;
        let session = resolve_session_ptr::<A>(key, rel, relid, context)?;
        last_session_store(key, relid, session);
        dispatch_to_session(session, f)
    }
}

pub(super) fn push_current_frame(key: FrameKey, cmd_type: pg_sys::CmdType::Type) {
    CURRENT_FRAME_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        stack.push(FrameStackEntry { key, cmd_type });
        publish_current_top(&stack);
    });
}

pub(super) fn pop_current_frame(key: FrameKey) {
    CURRENT_FRAME_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        if stack.last().map(|entry| entry.key) == Some(key) {
            stack.pop();
        } else {
            debug_assert!(
                !stack.iter().any(|entry| entry.key == key),
                "current frame stack popped out of order"
            );
            stack.retain(|entry| entry.key != key);
        }
        publish_current_top(&stack);
    });
}

/// The current frame stack's ModifyTable PlanState keys in stack order,
/// bottom-to-top (outermost frame first; `Vec::iter` is push order). The only
/// consumer checks membership, so the order is not significant — but it is not
/// top-first, despite "current frame" suggesting the top. Lets the executor
/// hook check its own per-node invariants against the active frames without
/// owning the stack.
pub(super) fn current_modifytable_frames() -> Vec<NonNull<pg_sys::PlanState>> {
    CURRENT_FRAME_STACK.with(|stack| {
        stack
            .borrow()
            .iter()
            .filter_map(|entry| match entry.key {
                FrameKey::ModifyTable(ps) => Some(ps),
                FrameKey::CopyFrom(_) => None,
            })
            .collect()
    })
}

/// Return the current managed DML frame id, if a table-AM callback is inside one.
pub fn current_dml_frame_id() -> Option<DmlFrameId> {
    HOT_STATE.with(|hot| {
        hot.get()
            .frame_top
            .map(|entry| DmlFrameId::from_key(entry.key))
    })
}

/// Register cleanup owned by the current managed DML frame.
///
/// Registration eagerly materializes the otherwise lazy frame, so the cleanup
/// also runs when the statement modifies no tuples. Callbacks run in reverse
/// registration order after all relation-local sessions have either completed
/// successfully or received abort cleanup.
///
/// # Errors
///
/// Returns an error when called outside a managed DML frame or when the current
/// frame cannot be materialized.
pub fn register_current_dml_frame_cleanup(
    cleanup: impl FnOnce() + 'static,
) -> Result<(), PgReportError> {
    let entry = HOT_STATE
        .with(|hot| hot.get().frame_top)
        .ok_or_else(|| {
            feature_not_supported(
                "DML frame cleanup registered outside a managed ModifyTable or COPY FROM frame",
            )
        })?;

    ensure_frame_exists(entry.key, entry.cmd_type)?;
    FRAMES.with(|frames| {
        let mut frames = frames.borrow_mut();
        let frame = frames.get_mut(&entry.key).ok_or_else(|| {
            internal_error("DML frame missing while registering cleanup")
        })?;
        frame.cleanup_callbacks.push(Box::new(cleanup));
        Ok(())
    })
}

/// Return the current DML frame id only when `rel_oid` is a relation whose rows
/// the active `ModifyTable` frame rewrites in place (`UPDATE`/`DELETE`/`MERGE`).
///
/// A scan driving such a frame must synthesize per-row identity (`ctid`); other
/// scans in the same statement (an `UPDATE ... FROM` source, an `INSERT`
/// target, a subquery relation) get `None` and skip that work. Returns `None`
/// outside any DML frame.
pub fn current_dml_target_frame(rel_oid: pg_sys::Oid) -> Option<DmlFrameId> {
    HOT_STATE
        .with(|hot| hot.get().frame_top)
        .and_then(|entry| entry.row_identity_target(rel_oid))
}

/// Use the unique target scan plan for `rel_oid` within the active DML frame.
///
/// The callback is higher-ranked over the frame lifetime, so the opaque plan
/// view and its PostgreSQL-owned expression tree cannot escape this call.
pub fn with_current_dml_target_plan<R>(
    rel_oid: pg_sys::Oid,
    use_plan: impl for<'frame> FnOnce(DmlTargetPlan<'frame>) -> R,
) -> Option<R> {
    let target = HOT_STATE
        .with(|hot| hot.get().frame_top)
        .and_then(|entry| entry.target_plan(rel_oid))?;
    let scope = DmlTargetPlanScope;
    Some(use_plan(DmlTargetPlan::new(target, &scope)))
}

pub(crate) fn finish_frame(key: FrameKey) -> Result<(), PgReportError> {
    // Take local ownership of the frame before running AM code, forgetting its
    // ResourceOwner handle first: if `end_modify()` ERRORs, the local `frame`
    // drops and its still-unfinalized sessions abort — no commit-time leak
    // warning. This does not, and is not meant to, stop a stray same-frame
    // callback during `end_modify()`: the current frame top stays set until the
    // wrapper pops it, so such a callback would resolve the same key and, the
    // frame now gone, lazily recreate a shadow frame. That synchronous
    // same-frame re-entry is an unsupported contract violation (see the module
    // "Reentrancy contract"), not a case this path guards against.
    last_session_invalidate(key);
    let frame = FRAMES.with(|frames| frames.borrow_mut().remove(&key));
    let Some(mut frame) = frame else {
        return Ok(());
    };

    resource::forget_resource(frame.resource_handle);

    debug_assert_eq!(frame.key, key);

    for (_, session) in frame.sessions.iter_mut() {
        session.finalize_success()?;
    }

    Ok(())
}

pub(crate) fn begin_copy_from_frame() {
    // COPY FROM has no PlanState key, so it gets a monotonic backend-local id.
    // The frame is created eagerly because COPY may call table-AM insert before
    // any other code has a chance to lazy-create a frame from executor state.
    let key = FrameKey::CopyFrom(next_copy_id());
    let cmd_type = pg_sys::CmdType::CMD_INSERT;
    FRAMES.with(|frames| {
        frames
            .borrow_mut()
            .insert(key, DmlFrame::new(key, cmd_type));
    });
    CURRENT_FRAME_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        stack.push(FrameStackEntry { key, cmd_type });
        publish_current_top(&stack);
    });
    COPY_FRAME_STACK.with(|stack| stack.borrow_mut().push(key));
}

pub(crate) fn finish_current_copy_frame() -> Result<(), PgReportError> {
    // The post-utility hook is a success path, so it finalizes the current COPY
    // frame.  Stack cleanup is retain-based because an ERROR path will skip this
    // function entirely and ResourceOwner cleanup may already have removed the
    // key.
    let key = COPY_FRAME_STACK
        .with(|stack| stack.borrow().last().copied())
        .ok_or_else(|| internal_error("COPY FROM frame stack is empty"))?;

    let result = finish_frame(key);
    remove_key_from_stacks(key);
    result
}
