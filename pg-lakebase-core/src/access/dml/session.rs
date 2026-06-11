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

use crate::api::{AmDmlSession, TableAccessMethod};
use crate::diag::PgReportError;
use crate::handles::RelationHandle;
use crate::resource::{self, ResourceHandle};
use crate::tuple::Row;
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::ptr::NonNull;

use super::erased_session::{ErasedModifySession, ErasedModifySessionAdapter};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum FrameKey {
    ModifyTable(NonNull<pg_sys::PlanState>),
    CopyFrom(u64),
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

struct DmlFrame {
    key: FrameKey,
    cmd_type: pg_sys::CmdType::Type,
    resource_handle: ResourceHandle,
    rel_index: HashMap<pg_sys::Oid, usize>,
    sessions: Vec<(pg_sys::Oid, Box<ModifySession>)>,
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
        }
    }

    fn session_index(&self, relid: pg_sys::Oid) -> Option<usize> {
        self.rel_index.get(&relid).copied()
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
    // Active or lazily-created DML frames.  Removing a frame drops its sessions;
    // unfinalized sessions abort.
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
    cmd_type: pg_sys::CmdType::Type,
) -> Result<Box<ModifySession>, PgReportError>
where
    A: TableAccessMethod,
{
    unsafe {
        // MERGE passes CMD_MERGE here even though later callbacks may be
        // insert/update/delete depending on the matched source row.
        let rel_handle = RelationHandle::from_raw(rel);
        let mut instance =
            <A::DmlSession as AmDmlSession>::new(&rel_handle, cmd_type)?;
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
    cmd_type: pg_sys::CmdType::Type,
) -> Result<NonNull<ModifySession>, PgReportError>
where
    A: TableAccessMethod,
{
    if let Some(ptr) = frame_session_ptr(key, relid)? {
        return Ok(ptr);
    }
    let session = create_session::<A>(rel, cmd_type)?;
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
        let session = resolve_session_ptr::<A>(key, rel, relid, cmd_type)?;
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
