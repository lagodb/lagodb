//! Shared EXPLAIN rendering of retained physical scan inventories.

use core::ptr;

use pgrx::pg_sys;

use crate::runtime_api::TableScanTaskMetrics;

impl TableScanTaskMetrics {
    /// Emit inventory facts after execution and before provider teardown.
    ///
    /// # Safety
    ///
    /// `es` must be the live PostgreSQL ExplainState for this scan.
    pub(crate) unsafe fn explain(&self, es: *mut pg_sys::ExplainState) {
        unsafe {
            pg_sys::ExplainPropertyUInteger(
                c"Scan Tasks".as_ptr(),
                ptr::null(),
                self.planned_tasks,
                es,
            );
            pg_sys::ExplainPropertyUInteger(
                c"Data Files Selected".as_ptr(),
                ptr::null(),
                self.planned_files,
                es,
            );
            pg_sys::ExplainPropertyUInteger(
                c"Selected File Bytes".as_ptr(),
                ptr::null(),
                self.planned_bytes,
                es,
            );
        }
    }
}
