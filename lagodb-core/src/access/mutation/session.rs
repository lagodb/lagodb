//! COPY FROM relation-session lifecycle.
//!
//! Normal INSERT/UPDATE/DELETE/MERGE is owned by the provider's ModifyTable
//! execution object; COPY FROM retains a small utility-scoped manager.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::ptr::NonNull;

use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::api::{AmCopySession, AmResult, TableAccessMethod};
use crate::diag::PgReportError;
use crate::handles::RelationHandle;
use crate::resource::{self, ResourceHandle};
use crate::tuple::{TupleSlotBatch, TupleSlotRow};

pub(super) struct CopyRelationSession {
    state: Box<dyn AmCopySession>,
    finalized: bool,
}

impl CopyRelationSession {
    fn new<T: AmCopySession + 'static>(state: T) -> Self {
        Self {
            state: Box::new(state),
            finalized: false,
        }
    }

    pub(super) fn finish_bulk_insert(&mut self, options: i32) -> AmResult<()> {
        self.state.finish_bulk_insert(options)
    }

    pub(super) fn tuple_insert_slot(
        &mut self,
        row: TupleSlotRow<'_>,
        cid: pg_sys::CommandId,
        options: i32,
    ) -> AmResult<()> {
        self.state.tuple_insert_slot(row, cid, options)
    }

    pub(super) fn multi_insert_slots(
        &mut self,
        rows: TupleSlotBatch<'_>,
        cid: pg_sys::CommandId,
        options: i32,
    ) -> AmResult<()> {
        self.state.multi_insert_slots(rows, cid, options)
    }

    fn finish(&mut self) -> Result<(), PgReportError> {
        self.state.end_copy()?;
        self.finalized = true;
        Ok(())
    }
}

impl Drop for CopyRelationSession {
    fn drop(&mut self) {
        if !self.finalized {
            self.state.abort_copy();
        }
    }
}

struct CopyFrame {
    id: u64,
    resource: ResourceHandle,
    sessions: Vec<(pg_sys::Oid, Box<CopyRelationSession>)>,
    session_by_oid: HashMap<pg_sys::Oid, usize>,
    last_session: Option<(pg_sys::Oid, usize)>,
}

thread_local! {
    static COPY_FRAMES: RefCell<Vec<CopyFrame>> = const { RefCell::new(Vec::new()) };
    static NEXT_COPY_ID: Cell<u64> = const { Cell::new(1) };
}

fn internal_error(message: &'static str) -> PgReportError {
    PgReportError::from_message(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, message)
}

fn unsupported(message: &'static str) -> PgReportError {
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

pub(super) fn abort_copy_frame(id: u64) {
    let frame = COPY_FRAMES.with(|frames| {
        let mut frames = frames.borrow_mut();
        frames
            .iter()
            .position(|frame| frame.id == id)
            .map(|index| frames.remove(index))
    });
    drop(frame);
}

pub(crate) fn begin_copy_from_frame() -> u64 {
    let id = next_copy_id();
    let resource = resource::remember_resource(move || abort_copy_frame(id));
    COPY_FRAMES.with(|frames| {
        frames.borrow_mut().push(CopyFrame {
            id,
            resource,
            sessions: Vec::new(),
            session_by_oid: HashMap::new(),
            last_session: None,
        });
    });
    id
}

pub(crate) fn finish_current_copy_frame() -> Result<(), PgReportError> {
    let mut frame = COPY_FRAMES
        .with(|frames| frames.borrow_mut().pop())
        .ok_or_else(|| internal_error("COPY FROM lifecycle stack is empty"))?;
    resource::forget_resource(frame.resource);
    for (_, session) in &mut frame.sessions {
        session.finish()?;
    }
    Ok(())
}

fn create_session<A: TableAccessMethod>(
    rel: pg_sys::Relation,
) -> Result<Box<CopyRelationSession>, PgReportError> {
    // SAFETY: the table-AM callback borrows a live relation for this call.
    let relation = unsafe { RelationHandle::from_raw(rel) };
    let state = A::CopySession::begin_copy(&relation)?;
    Ok(Box::new(CopyRelationSession::new(state)))
}

pub(super) fn with_current_relation_session<A, R>(
    rel: pg_sys::Relation,
    use_session: impl FnOnce(&mut CopyRelationSession) -> Result<R, PgReportError>,
) -> Result<R, PgReportError>
where
    A: TableAccessMethod,
{
    // SAFETY: PostgreSQL's table-AM callback contract supplies a live relation
    // and keeps it open for the duration of the callback.
    let rel_oid = unsafe { (*rel).rd_id };
    let existing = COPY_FRAMES.with(|frames| {
        let mut frames = frames.borrow_mut();
        let frame = frames.last_mut()?;
        let index = match frame.last_session {
            Some((last_oid, index)) if last_oid == rel_oid => Some(index),
            _ => frame.session_by_oid.get(&rel_oid).copied(),
        }?;
        frame.last_session = Some((rel_oid, index));
        Some(NonNull::from(frame.sessions[index].1.as_mut()))
    });
    let pointer = match existing {
        Some(pointer) => pointer,
        None => {
            let session = create_session::<A>(rel)?;
            COPY_FRAMES.with(|frames| {
                let mut frames = frames.borrow_mut();
                let frame = frames.last_mut().ok_or_else(|| {
                    unsupported(
                        "table-AM INSERT callback is only valid inside COPY FROM; ModifyTable mutation uses LagoDBModifyTable",
                    )
                })?;
                let index = frame.sessions.len();
                frame.sessions.push((rel_oid, session));
                frame.session_by_oid.insert(rel_oid, index);
                frame.last_session = Some((rel_oid, index));
                Ok::<NonNull<CopyRelationSession>, PgReportError>(NonNull::from(
                    frame.sessions[index].1.as_mut(),
                ))
            })?
        }
    };

    // SAFETY: sessions are boxed and remain owned by the current frame for the
    // duration of the serialized table-AM callback.
    use_session(unsafe { &mut *pointer.as_ptr() })
}
