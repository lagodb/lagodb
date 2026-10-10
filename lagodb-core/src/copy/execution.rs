//! Explicit, driver-owned COPY writes. C borrows stable relation bindings;
//! neither buffer eviction nor provider DSO boundaries transfer their ownership.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::c_void;
use std::ptr;
use std::rc::Rc;

use pgrx::pg_sys;

use crate::diag::{PgReportError, report_warning};
use crate::resource::{ResourceHandle, forget_resource, remember_resource};
use crate::runtime_api::{CallbackErrorReport, TableCopyCallbacks};
use crate::table_provider::TableProviderRouter;

use super::CopyError;

pub(super) struct CopyBinding {
    pub(super) callbacks: &'static TableCopyCallbacks,
    pub(super) handle: *mut c_void,
}

impl Drop for CopyBinding {
    fn drop(&mut self) {
        let mut error = CallbackErrorReport::default();
        // Release consumes a provider allocation in its own DSO. On abort it
        // detaches PG pointers instead of closing already reclaimed resources.
        let status = unsafe { (self.callbacks.release)(self.handle, &mut error) };
        if let Err(error) = unsafe { error.into_result(status, "COPY release") } {
            report_warning(format_args!("COPY binding cleanup failed: {error}"));
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Writing,
    Published,
    Completed,
}

pub(super) struct CopyExecution {
    // Boxes keep borrowed C handles stable across map growth and nested COPY.
    bindings: HashMap<pg_sys::Oid, Box<CopyBinding>>,
    command_id: Option<pg_sys::CommandId>,
    phase: Phase,
}

impl CopyExecution {
    pub(super) unsafe fn bind(
        &mut self,
        relation: pg_sys::Relation,
        cid: pg_sys::CommandId,
    ) -> Result<*mut c_void, CopyError> {
        // C supplies its fixed mycid, captured before defaults and triggers.
        let cid = *self.command_id.get_or_insert(cid);
        let oid = unsafe { (*relation).rd_id };
        if let Some(binding) = self.bindings.get_mut(&oid) {
            return Ok(ptr::from_mut(binding.as_mut()).cast());
        }
        let am = unsafe { (*(*relation).rd_rel).relam };
        let Some(provider) = TableProviderRouter::provider_for_am(am)? else {
            return Ok(ptr::null_mut());
        };
        let Some(callbacks) = (unsafe { provider.copy.as_ref() }) else {
            return Ok(ptr::null_mut());
        };
        let mut handle = ptr::null_mut();
        let mut error = CallbackErrorReport::default();
        let status =
            unsafe { (callbacks.bind)(relation, cid, &mut handle, &mut error) };
        unsafe { error.into_result(status, "COPY bind") }?;
        let mut binding = Box::new(CopyBinding { callbacks, handle });
        let pointer = ptr::from_mut(binding.as_mut()).cast();
        self.bindings.insert(oid, binding);
        Ok(pointer)
    }

    pub(super) fn publish(&mut self) -> Result<(), PgReportError> {
        // Exactly one executor completion event, after the final PG batch.
        debug_assert!(self.phase == Phase::Writing);
        for binding in self.bindings.values_mut() {
            let mut error = CallbackErrorReport::default();
            let status =
                unsafe { (binding.callbacks.publish)(binding.handle, &mut error) };
            unsafe { error.into_result(status, "COPY publish") }?;
        }
        self.phase = Phase::Published;
        Ok(())
    }

    fn complete(&mut self) -> Result<(), PgReportError> {
        debug_assert!(self.phase == Phase::Published);
        for binding in self.bindings.values_mut() {
            let mut error = CallbackErrorReport::default();
            let status =
                unsafe { (binding.callbacks.complete)(binding.handle, &mut error) };
            unsafe { error.into_result(status, "COPY complete") }?;
        }
        self.phase = Phase::Completed;
        self.bindings.clear();
        Ok(())
    }
}

pub(super) struct CopyOwner {
    execution: Rc<RefCell<CopyExecution>>,
    resource: ResourceHandle,
}

impl CopyOwner {
    pub(super) fn new() -> Self {
        let execution = Rc::new(RefCell::new(CopyExecution {
            bindings: HashMap::new(),
            command_id: None,
            phase: Phase::Writing,
        }));
        let cleanup = Rc::clone(&execution);
        let resource =
            remember_resource(move || cleanup.borrow_mut().bindings.clear());
        Self {
            execution,
            resource,
        }
    }

    pub(super) fn context(&self) -> *mut c_void {
        Rc::as_ptr(&self.execution).cast_mut().cast()
    }

    pub(super) fn complete(&mut self) -> Result<(), PgReportError> {
        self.execution.borrow_mut().complete()
    }
}

impl Drop for CopyOwner {
    fn drop(&mut self) {
        forget_resource(self.resource);
        self.execution.borrow_mut().bindings.clear();
    }
}
