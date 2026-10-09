//! Provider-local COPY command IDs, requested only by FDW providers that need them.
//! AM sessions belong to the explicit COPY execution owner, never this stack.

use crate::diag::PgReportError;
use crate::resource::{self, ResourceHandle};
use pgrx::{PgSqlErrorCode, pg_sys};
use std::cell::{Cell, RefCell};

struct CopyFrame {
    id: u64,
    command_id: pg_sys::CommandId,
    resource: ResourceHandle,
}

thread_local! {
    static COPY_FRAMES: RefCell<Vec<CopyFrame>> = const { RefCell::new(Vec::new()) };
    static NEXT_COPY_ID: Cell<u64> = const { Cell::new(1) };
}

fn internal_error(message: &'static str) -> PgReportError {
    PgReportError::from_message(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, message)
}

fn next_copy_id() -> u64 {
    NEXT_COPY_ID.with(|next| {
        let id = next.get();
        next.set(id.wrapping_add(1).max(1));
        id
    })
}

pub(super) fn abort_copy_frame(id: u64) {
    COPY_FRAMES.with(|frames| {
        let mut frames = frames.borrow_mut();
        if let Some(index) = frames.iter().position(|frame| frame.id == id) {
            frames.remove(index);
        }
    });
}

pub(crate) fn begin_copy_from_frame() -> u64 {
    let id = next_copy_id();
    let resource = resource::remember_resource(move || abort_copy_frame(id));
    // SAFETY: utility entry runs before COPY evaluates defaults or triggers.
    // Native CopyFrom captures the same CID as mycid. Keep it for foreign
    // partitions first opened after a nested SPI command has advanced the CID.
    let command_id = unsafe { pg_sys::GetCurrentCommandId(true) };
    COPY_FRAMES.with(|frames| {
        frames.borrow_mut().push(CopyFrame {
            id,
            command_id,
            resource,
        });
    });
    id
}

pub(crate) fn current_copy_command_id() -> Result<pg_sys::CommandId, PgReportError> {
    COPY_FRAMES.with(|frames| {
        frames
            .borrow()
            .last()
            .map(|frame| frame.command_id)
            .ok_or_else(|| internal_error("COPY FROM lifecycle stack is empty"))
    })
}

pub(crate) fn finish_current_copy_frame() -> Result<(), PgReportError> {
    let frame = COPY_FRAMES
        .with(|frames| frames.borrow_mut().pop())
        .ok_or_else(|| internal_error("COPY FROM lifecycle stack is empty"))?;
    resource::forget_resource(frame.resource);
    Ok(())
}
