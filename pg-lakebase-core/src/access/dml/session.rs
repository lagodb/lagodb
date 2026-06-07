//! DML frame and per-relation session lifecycle.
//!
//! The design goal is to give AM implementations a clear `dml_init` /
//! `dml_fini` semantic boundary even though PostgreSQL's table-AM vtable only
//! exposes individual tuple operations.  A single SQL write frame can touch one
//! relation or many relations:
//!
//! - a plain INSERT usually touches one target relation,
//! - partition routing can touch several leaf relations in the same
//!   ModifyTable/COPY frame,
//! - MERGE uses one ModifyTable frame whose runtime callbacks may be a mix of
//!   insert, update, and delete actions.
//!
//! The framework therefore scopes DML state to a PostgreSQL "frame" rather than
//! to a transaction or a statement string.  A [`DmlFrame`] owns relation-local
//! [`ModifySession`] instances.  The first table-AM callback for a relation
//! lazily creates the session, calls the AM's `begin_modify()`, and then
//! dispatches the callback.  Successful frame completion calls
//! `end_modify()` once for every touched relation.  ERROR, abort, and
//! rollback-to-savepoint never call `end_modify()`; they are handled by
//! ResourceOwner cleanup, which drops the frame and lets unfinalized sessions
//! run `abort_modify()`.
//!
//! This intentionally replaced the previous statement-global session cache:
//! global "last used session" state cannot represent nested SPI, data-modifying
//! CTEs, cursor/portal execution, partitioned writes, or COPY FROM reliably.
//! The frame stack below records the current PostgreSQL write frame explicitly,
//! while ResourceOwner handles the non-local exits that Rust code cannot
//! observe directly.

use crate::api::{AmDmlSession, TableAccessMethod};
use crate::diag::{PgReportError, ReportableError};
use crate::handles::RelationHandle;
use crate::resource::{self, ResourceHandle};
use crate::tuple::Row;
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::ptr::NonNull;

use super::erased_session::{ErasedModifySession, ErasedModifySessionAdapter};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum FrameKey {
    ModifyTable(NonNull<pg_sys::PlanState>),
    CopyFrom(u64),
}

#[derive(Clone, Copy)]
struct NodeAdapter {
    estate: NonNull<pg_sys::EState>,
    original: unsafe extern "C-unwind" fn(
        pstate: *mut pg_sys::PlanState,
    ) -> *mut pg_sys::TupleTableSlot,
    cmd_type: pg_sys::CmdType::Type,
}

struct ExecutorAdapter {
    estate: NonNull<pg_sys::EState>,
    resource_handle: ResourceHandle,
    nodes: Vec<NonNull<pg_sys::PlanState>>,
}

struct DmlFrame {
    key: FrameKey,
    cmd_type: pg_sys::CmdType::Type,
    resource_handle: ResourceHandle,
    rel_index: HashMap<pg_sys::Oid, usize>,
    sessions: Vec<(pg_sys::Oid, Box<ModifySession>)>,
}

// Thin object-safe wrapper around the AM-provided DML session.  `finalized`
// records whether the success path has already called `end_modify()`.  Dropping
// an unfinalized session is always an abort cleanup, which is exactly what we
// want after ERROR, transaction abort, rollback to savepoint, or an unfinished
// frame being released by ResourceOwner.
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

thread_local! {
    // PlanState -> wrapper metadata installed by ExecutorStart.  This map is
    // separate from FRAMES because a ModifyTable node may exist before it has
    // produced any table-AM write callback.
    static NODE_ADAPTERS: RefCell<HashMap<NonNull<pg_sys::PlanState>, NodeAdapter>> =
        RefCell::new(HashMap::new());
    // EState -> all ModifyTable nodes wrapped for that executor.  The
    // ExecutorAdapter ResourceOwner cleanup removes stale wrappers if ERROR
    // prevents ExecutorEnd from running.
    static EXECUTOR_ADAPTERS: RefCell<HashMap<NonNull<pg_sys::EState>, ExecutorAdapter>> =
        RefCell::new(HashMap::new());
    // Active or lazily-created DML frames.  Removing a frame drops its sessions;
    // unfinalized sessions abort.
    static FRAMES: RefCell<HashMap<FrameKey, DmlFrame>> =
        RefCell::new(HashMap::new());
    // Current write-frame stack.  Nested SPI DML and trigger DML naturally push
    // another ModifyTable frame while the outer frame is suspended.
    static CURRENT_FRAME_STACK: RefCell<Vec<FrameKey>> = const { RefCell::new(Vec::new()) };
    // COPY FROM frames are created by the utility hook rather than a PlanState,
    // so a separate stack identifies which COPY frame should finish in on_post.
    static COPY_FRAME_STACK: RefCell<Vec<FrameKey>> = const { RefCell::new(Vec::new()) };
    // Frames currently in `finish_frame`; AM callbacks during finalization would
    // be a bug because `end_modify()` is supposed to close the relation-local
    // writer, not perform new table-AM writes into the same frame.
    static FINALIZING_KEYS: RefCell<HashSet<FrameKey>> = RefCell::new(HashSet::new());
    // A frame is temporarily removed from FRAMES while a callback gets mutable
    // access to one session.  This avoids RefCell borrow penetration across AM
    // code.  The borrowed set rejects same-frame reentrancy before it can
    // create a shadow frame for the same key.
    static BORROWED_KEYS: RefCell<HashSet<FrameKey>> = RefCell::new(HashSet::new());
    static NEXT_COPY_ID: Cell<u64> = const { Cell::new(1) };
}

fn internal_error(message: impl Into<String>) -> PgReportError {
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

fn current_frame_key() -> Result<FrameKey, PgReportError> {
    CURRENT_FRAME_STACK.with(|stack| {
        stack.borrow().last().copied().ok_or_else(|| {
            feature_not_supported(
                "DML called outside a managed ModifyTable or COPY FROM frame",
            )
        })
    })
}

fn remove_key_from_stacks(key: FrameKey) {
    CURRENT_FRAME_STACK.with(|stack| stack.borrow_mut().retain(|k| *k != key));
    COPY_FRAME_STACK.with(|stack| stack.borrow_mut().retain(|k| *k != key));
}

fn abort_frame_and_remove_stack(key: FrameKey) {
    // ResourceOwner uses this path for ERROR/abort/rollback-to-savepoint.  A
    // dropped frame drops every unfinalized ModifySession, whose Drop calls
    // abort_modify().  Stack removal is by retain rather than pop because COPY
    // errors and subtransaction rollback can unwind non-locally through nested
    // frames.
    let frame = FRAMES.with(|frames| frames.borrow_mut().remove(&key));
    drop(frame);
    remove_key_from_stacks(key);
    FINALIZING_KEYS.with(|keys| {
        keys.borrow_mut().remove(&key);
    });
    BORROWED_KEYS.with(|keys| {
        keys.borrow_mut().remove(&key);
    });
}

fn lookup_node_adapter(
    ps: NonNull<pg_sys::PlanState>,
) -> Result<NodeAdapter, PgReportError> {
    NODE_ADAPTERS.with(|adapters| {
        adapters.borrow().get(&ps).copied().ok_or_else(|| {
            internal_error("ModifyTable node adapter missing during DML execution")
        })
    })
}

fn cmd_type_for_key(key: FrameKey) -> Result<pg_sys::CmdType::Type, PgReportError> {
    match key {
        FrameKey::ModifyTable(ps) => {
            lookup_node_adapter(ps).map(|adapter| adapter.cmd_type)
        }
        FrameKey::CopyFrom(_) => Ok(pg_sys::CmdType::CMD_INSERT),
    }
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

fn is_finalizing(key: FrameKey) -> bool {
    FINALIZING_KEYS.with(|keys| keys.borrow().contains(&key))
}

fn is_borrowed(key: FrameKey) -> bool {
    BORROWED_KEYS.with(|keys| keys.borrow().contains(&key))
}

fn frame_has_session(
    key: FrameKey,
    relid: pg_sys::Oid,
) -> Result<bool, PgReportError> {
    FRAMES.with(|frames| {
        let frames = frames.borrow();
        let frame = frames
            .get(&key)
            .ok_or_else(|| internal_error("DML frame missing"))?;
        Ok(frame.session_index(relid).is_some())
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

struct BorrowedFrame {
    key: FrameKey,
    frame: Option<DmlFrame>,
}

impl BorrowedFrame {
    fn take(key: FrameKey) -> Result<Self, PgReportError> {
        // Closure-based session access is deliberately implemented by taking
        // the frame out of the TLS map.  Returning a long-lived raw pointer into
        // FRAMES would let RefCell borrows leak through arbitrary AM code and
        // would make reentrant table-AM calls unsound.
        let inserted = BORROWED_KEYS.with(|keys| keys.borrow_mut().insert(key));
        if !inserted {
            return Err(internal_error(
                "AM reentrancy is not supported in this DML frame",
            ));
        }

        let frame = FRAMES.with(|frames| frames.borrow_mut().remove(&key));
        frame
            .map(|frame| Self {
                key,
                frame: Some(frame),
            })
            .ok_or_else(|| {
                BORROWED_KEYS.with(|keys| {
                    keys.borrow_mut().remove(&key);
                });
                internal_error("DML frame missing during session access")
            })
    }

    fn session_mut(
        &mut self,
        relid: pg_sys::Oid,
    ) -> Result<&mut ModifySession, PgReportError> {
        let frame = self
            .frame
            .as_mut()
            .ok_or_else(|| internal_error("DML frame already returned"))?;
        let index = frame.session_index(relid).ok_or_else(|| {
            internal_error("DML session missing during session access")
        })?;
        Ok(frame.sessions[index].1.as_mut())
    }
}

impl Drop for BorrowedFrame {
    fn drop(&mut self) {
        let Some(frame) = self.frame.take() else {
            return;
        };

        let collision = FRAMES.with(|frames| {
            let previous = frames.borrow_mut().insert(self.key, frame);
            if let Some(previous) = previous {
                resource::forget_resource(previous.resource_handle);
                true
            } else {
                false
            }
        });
        BORROWED_KEYS.with(|keys| {
            keys.borrow_mut().remove(&self.key);
        });
        debug_assert!(!collision, "DML frame reinsert collision");
    }
}

fn with_frame_session<R>(
    key: FrameKey,
    relid: pg_sys::Oid,
    f: impl FnOnce(&mut ModifySession) -> Result<R, PgReportError>,
) -> Result<R, PgReportError> {
    let mut borrowed = BorrowedFrame::take(key)?;
    f(borrowed.session_mut(relid)?)
}

fn create_session<A>(
    rel: pg_sys::Relation,
    cmd_type: pg_sys::CmdType::Type,
) -> Result<Box<ModifySession>, PgReportError>
where
    A: TableAccessMethod,
{
    unsafe {
        // `cmd_type` is the PostgreSQL frame operation, not necessarily the
        // individual callback action.  In particular MERGE passes CMD_MERGE
        // here while later callbacks may be insert/update/delete depending on
        // the matched source row.
        let rel_handle = RelationHandle::from_raw(rel);
        let mut instance =
            <A::DmlSession as AmDmlSession>::new(&rel_handle, cmd_type)?;
        instance.begin_modify()?;

        Ok(Box::new(ModifySession::new::<A::DmlSession>(instance)))
    }
}

pub(super) fn with_current_session<A, R>(
    rel: pg_sys::Relation,
    f: impl FnOnce(&mut ModifySession) -> Result<R, PgReportError>,
) -> Result<R, PgReportError>
where
    A: TableAccessMethod,
{
    unsafe {
        // Table-AM callbacks are only valid while a lifecycle hook has pushed a
        // managed frame.  Unsupported v1 paths such as CTAS/DestReceiver writes
        // fail here rather than silently creating statement-global state with no
        // well-defined success boundary.
        let key = current_frame_key()?;

        if is_finalizing(key) {
            return Err(internal_error("AM callback during DML frame finalization"));
        }
        // Check before lazy frame/session creation. BorrowedFrame::take also
        // rejects reentrancy, but by then this path may have created a shadow
        // frame for the borrowed key.
        if is_borrowed(key) {
            return Err(internal_error(
                "AM reentrancy is not supported in this DML frame",
            ));
        }

        let cmd_type = cmd_type_for_key(key)?;
        ensure_frame_exists(key, cmd_type)?;

        let relid = (*rel).rd_id;
        if !frame_has_session(key, relid)? {
            let session = create_session::<A>(rel, cmd_type)?;
            insert_session(key, relid, session)?;
        }

        with_frame_session(key, relid, f)
    }
}

fn push_current_frame(key: FrameKey) {
    CURRENT_FRAME_STACK.with(|stack| stack.borrow_mut().push(key));
}

fn pop_current_frame(key: FrameKey) {
    CURRENT_FRAME_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        if stack.last().copied() == Some(key) {
            stack.pop();
        } else {
            debug_assert!(
                !stack.contains(&key),
                "current frame stack popped out of order"
            );
            stack.retain(|existing| *existing != key);
        }
    });
}

struct FinalizingGuard {
    key: FrameKey,
}

impl FinalizingGuard {
    fn insert(key: FrameKey) -> Self {
        FINALIZING_KEYS.with(|keys| {
            keys.borrow_mut().insert(key);
        });
        Self { key }
    }
}

impl Drop for FinalizingGuard {
    fn drop(&mut self) {
        FINALIZING_KEYS.with(|keys| {
            keys.borrow_mut().remove(&self.key);
        });
    }
}

pub(crate) fn finish_frame(key: FrameKey) -> Result<(), PgReportError> {
    // Success path: remove the frame first so any callback during finalize is
    // either rejected by FINALIZING_KEYS or treated as a separate, explicit
    // frame.  Forget the ResourceOwner handle before calling AM code; if
    // `end_modify()` ERRORs, the frame drops immediately and any remaining
    // unfinalized sessions abort without an additional commit-time leak warning.
    let frame = FRAMES.with(|frames| frames.borrow_mut().remove(&key));
    let Some(mut frame) = frame else {
        return Ok(());
    };

    resource::forget_resource(frame.resource_handle);

    let _finalizing = FinalizingGuard::insert(key);
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
    CURRENT_FRAME_STACK.with(|stack| stack.borrow_mut().push(key));
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

pub(crate) fn register_executor_adapter(
    estate: NonNull<pg_sys::EState>,
    nodes: Vec<NonNull<pg_sys::PlanState>>,
) -> Result<(), PgReportError> {
    if nodes.is_empty() {
        return Ok(());
    }

    let new_nodes: Vec<_> = nodes
        .into_iter()
        .filter(|ps| {
            !NODE_ADAPTERS.with(|adapters| adapters.borrow().contains_key(ps))
        })
        .collect();
    if new_nodes.is_empty() {
        return Ok(());
    }

    let estate_key = estate;
    let resource_handle =
        resource::remember_resource(move || cleanup_executor_adapter(estate_key));

    // Register the ExecutorAdapter before wrapping nodes.  If wrapping a later
    // node fails, ResourceOwner cleanup can still remove the nodes already
    // wrapped for this executor instead of leaving stale NODE_ADAPTERS entries.
    EXECUTOR_ADAPTERS.with(|adapters| {
        adapters.borrow_mut().insert(
            estate,
            ExecutorAdapter {
                estate,
                resource_handle,
                nodes: Vec::with_capacity(new_nodes.len()),
            },
        );
    });

    for ps in new_nodes {
        wrap_modifytable_node(estate, ps)?;
        EXECUTOR_ADAPTERS.with(|adapters| {
            let mut adapters = adapters.borrow_mut();
            let adapter = adapters
                .get_mut(&estate)
                .expect("ExecutorAdapter should exist while wrapping DML nodes");
            adapter.nodes.push(ps);
        });
    }

    Ok(())
}

fn wrap_modifytable_node(
    estate: NonNull<pg_sys::EState>,
    ps: NonNull<pg_sys::PlanState>,
) -> Result<(), PgReportError> {
    unsafe {
        if (*ps.as_ptr()).type_ != pg_sys::NodeTag::T_ModifyTableState {
            return Err(internal_error(
                "attempted to wrap a non-ModifyTable plan state",
            ));
        }

        let original = (*ps.as_ptr()).ExecProcNodeReal.ok_or_else(|| {
            internal_error(
                "ModifyTable ExecProcNodeReal is not initialized after ExecutorStart",
            )
        })?;

        // Wrap only ExecProcNodeReal.  PostgreSQL's ExecInitNode has already
        // chosen the real implementation by ExecutorStart time; replacing this
        // slot lets us bracket calls to exactly this ModifyTable node while
        // preserving executor instrumentation and outer dispatch machinery.
        let mtstate = ps.as_ptr() as *mut pg_sys::ModifyTableState;
        NODE_ADAPTERS.with(|adapters| {
            adapters.borrow_mut().insert(
                ps,
                NodeAdapter {
                    estate,
                    original,
                    cmd_type: (*mtstate).operation,
                },
            );
        });
        (*ps.as_ptr()).ExecProcNodeReal = Some(lakebase_modifytable_wrapper);
    }

    Ok(())
}

fn cleanup_executor_adapter(estate: NonNull<pg_sys::EState>) {
    // Abort/error cleanup for wrappers installed in ExecutorStart.  We do not
    // try to restore ExecProcNodeReal because the owning executor memory is
    // being released; removing our TLS references is the important part.  If a
    // direct ExecProcNodeReal call raised a PostgreSQL ERROR, Rust cleanup after
    // `push_current_frame` did not run, so remove the corresponding frame key
    // here as the ResourceOwner cleanup boundary.
    let adapter =
        EXECUTOR_ADAPTERS.with(|adapters| adapters.borrow_mut().remove(&estate));
    let Some(adapter) = adapter else {
        return;
    };

    debug_assert_eq!(adapter.estate, estate);
    NODE_ADAPTERS.with(|node_adapters| {
        let mut node_adapters = node_adapters.borrow_mut();
        for node in adapter.nodes.iter() {
            node_adapters.remove(node);
        }
    });
    for node in adapter.nodes {
        abort_frame_and_remove_stack(FrameKey::ModifyTable(node));
    }
}

pub(crate) fn end_executor_adapter(estate: NonNull<pg_sys::EState>) {
    // Normal ExecutorEnd path.  This is not a DML success boundary; PostgreSQL
    // calls ExecutorFinish before ExecutorEnd on normal portal cleanup, and
    // ERROR paths may skip ExecutorEnd entirely.  Therefore this function only
    // removes adapter state and forgets the adapter ResourceOwner handle.
    let adapter =
        EXECUTOR_ADAPTERS.with(|adapters| adapters.borrow_mut().remove(&estate));
    let Some(adapter) = adapter else {
        return;
    };

    resource::forget_resource(adapter.resource_handle);
    debug_assert_eq!(adapter.estate, estate);
    NODE_ADAPTERS.with(|node_adapters| {
        let mut node_adapters = node_adapters.borrow_mut();
        for node in adapter.nodes {
            node_adapters.remove(&node);
        }
    });
}

pub(crate) fn check_executor_finish_invariants(
    estate: NonNull<pg_sys::EState>,
) -> Result<(), PgReportError> {
    // ExecutorFinish can run executor post-processing and triggers.  If a
    // ModifyTable frame for this executor is still on the current stack at this
    // point, our wrapper push/pop accounting is inconsistent.  In release builds
    // the hook reports this as a warning so unusual but legal portal paths do
    // not become user-visible query failures.
    CURRENT_FRAME_STACK.with(|stack| {
        for key in stack.borrow().iter().copied() {
            let FrameKey::ModifyTable(ps) = key else {
                continue;
            };
            let adapter =
                NODE_ADAPTERS.with(|adapters| adapters.borrow().get(&ps).copied());
            if adapter.is_some_and(|adapter| adapter.estate == estate) {
                return Err(internal_error(
                    "ExecutorFinish reached while a ModifyTable frame is active",
                ));
            }
        }
        Ok(())
    })
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn lakebase_modifytable_wrapper(
    ps: *mut pg_sys::PlanState,
) -> *mut pg_sys::TupleTableSlot {
    let Some(ps_key) = NonNull::new(ps) else {
        return std::ptr::null_mut();
    };
    let key = FrameKey::ModifyTable(ps_key);

    let adapter = lookup_node_adapter(ps_key).report_unwrap();

    // This wrapper is the ModifyTable frame boundary.  While PostgreSQL is
    // executing the node, table-AM callbacks can resolve CURRENT_FRAME_STACK to
    // this key and lazily create per-relation AM sessions.  Returning a non-null
    // slot means the node still has RETURNING output to deliver; success
    // finalization waits until PostgreSQL reports end-of-node with mt_done set.
    push_current_frame(key);
    // `original` is the raw ExecProcNodeReal dispatcher saved from this
    // PlanState.  It can execute executor subtrees that re-enter pgrx
    // `#[pg_guard]` callbacks, so do not wrap it in `pg_guard_ffi_boundary`.
    // Keep Rust state crossing this direct call trivially deallocated; the
    // executor adapter ResourceOwner cleanup removes the frame key if a
    // PostgreSQL ERROR longjmps past the normal pop path.
    let slot = unsafe { (adapter.original)(ps) };

    let mtstate = ps as *mut pg_sys::ModifyTableState;
    let finalize_result = if slot.is_null() && unsafe { (*mtstate).mt_done } {
        finish_frame(key)
    } else {
        Ok(())
    };
    pop_current_frame(key);
    finalize_result.report_unwrap();

    slot
}
