//! Exact-build planner callback facets routed by `lagodb-base`.

use std::ffi::c_void;
use std::mem::size_of;
use std::panic::AssertUnwindSafe;
use std::{ptr, slice, str};

use pgrx::prelude::PgSqlErrorCode;
use pgrx::{PgMemoryContexts, PgTryBuilder, pg_sys};

use crate::diag::PgReportError;

pub const PLANNING_CALLBACK_OK: u32 = 0;
pub const PLANNING_CALLBACK_FAILED: u32 = 1;

#[repr(C)]
#[derive(Clone, Copy)]
struct PlanErrorText {
    data: *const u8,
    len: usize,
}

impl Default for PlanErrorText {
    fn default() -> Self {
        Self {
            data: ptr::null(),
            len: 0,
        }
    }
}

impl PlanErrorText {
    unsafe fn copy_from(value: &str, memory_context: pg_sys::MemoryContext) -> Self {
        if value.is_empty() {
            return Self::default();
        }
        let mut context = PgMemoryContexts::For(memory_context);
        Self {
            // SAFETY: `value` is live for this call; PostgreSQL copies exactly
            // `len` bytes into the active memory context.
            data: unsafe {
                context.copy_ptr_into(value.as_ptr().cast_mut(), value.len())
            },
            len: value.len(),
        }
    }

    fn is_valid(self) -> bool {
        self.len == 0 || !self.data.is_null()
    }

    unsafe fn to_owned(self) -> String {
        if self.len == 0 {
            return String::new();
        }
        // SAFETY: the exact-build provider copied bytes from a Rust `str` and
        // the caller guarantees that the allocation remains live.
        unsafe {
            str::from_utf8_unchecked(slice::from_raw_parts(self.data, self.len))
        }
        .to_owned()
    }
}

/// PostgreSQL-owned diagnostic payload returned by a planning descriptor.
///
/// The provider allocates text in the active PostgreSQL memory context. The
/// runtime consumes it synchronously before that context can be reset, so no
/// Rust allocation or error object crosses the DSO boundary.
#[repr(C)]
pub struct PlanErrorRecord {
    struct_size: u32,
    sql_error_code: i32,
    message: PlanErrorText,
    detail: PlanErrorText,
    hint: PlanErrorText,
}

impl Default for PlanErrorRecord {
    fn default() -> Self {
        Self {
            struct_size: size_of::<Self>() as u32,
            sql_error_code: 0,
            message: PlanErrorText::default(),
            detail: PlanErrorText::default(),
            hint: PlanErrorText::default(),
        }
    }
}

impl PlanErrorRecord {
    /// Run one provider callback without allowing a PostgreSQL error or Rust
    /// panic to cross the descriptor ABI.
    ///
    /// # Safety
    ///
    /// This must run on a PostgreSQL backend thread with a live current memory
    /// context. The runtime must consume the record synchronously.
    pub unsafe fn capture(
        &mut self,
        operation: impl FnOnce() -> Result<(), PgReportError>,
    ) -> u32 {
        // Preserve the caller's planner context across a caught PostgreSQL
        // ERROR; error handling may temporarily switch CurrentMemoryContext.
        let memory_context = unsafe { pg_sys::CurrentMemoryContext };
        let result = PgTryBuilder::new(AssertUnwindSafe(operation))
            .catch_others(|error| Err(PgReportError::from_caught(error)))
            .execute();
        match result {
            Ok(()) => PLANNING_CALLBACK_OK,
            Err(error) => {
                // SAFETY: `memory_context` was captured while live immediately
                // before the protected operation and this record is writable.
                unsafe { self.write(error, memory_context) };
                PLANNING_CALLBACK_FAILED
            }
        }
    }

    /// Reconstruct an owned error in the runtime DSO.
    ///
    /// # Safety
    ///
    /// Non-empty text slices must reference live UTF-8 bytes allocated by the
    /// provider callback in the current PostgreSQL context.
    pub unsafe fn to_error(&self, callback: &'static str) -> PgReportError {
        let expected_size = size_of::<Self>() as u32;
        if self.struct_size != expected_size
            || self.sql_error_code
                == PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION as i32
            || !self.message.is_valid()
            || !self.detail.is_valid()
            || !self.hint.is_valid()
        {
            return PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("{callback} returned an invalid planning error record"),
            );
        }
        // SAFETY: all three slices were validated above and are covered by the
        // method's exact-build callback contract.
        let message = unsafe { self.message.to_owned() };
        // SAFETY: the non-empty slice has the same validated lifetime and
        // provenance as `message`.
        let detail =
            (self.detail.len != 0).then(|| unsafe { self.detail.to_owned() });
        // SAFETY: the non-empty slice has the same validated lifetime and
        // provenance as `message`.
        let hint = (self.hint.len != 0).then(|| unsafe { self.hint.to_owned() });
        PgReportError::from_parts(self.sql_error_code.into(), message, detail, hint)
    }

    unsafe fn write(
        &mut self,
        error: PgReportError,
        memory_context: pg_sys::MemoryContext,
    ) {
        let sql_error_code = error.sql_error_code();
        let report = error.into_report();
        self.struct_size = size_of::<Self>() as u32;
        self.sql_error_code = sql_error_code as i32;
        // SAFETY: the caller guarantees that `memory_context` is live and this
        // method consumes each borrowed report string synchronously.
        self.message =
            unsafe { PlanErrorText::copy_from(report.message(), memory_context) };
        self.detail = report
            .detail()
            // SAFETY: same live context and synchronous copy as `message`.
            .map_or_else(PlanErrorText::default, |detail| unsafe {
                PlanErrorText::copy_from(detail, memory_context)
            });
        self.hint = report
            .hint()
            // SAFETY: same live context and synchronous copy as `message`.
            .map_or_else(PlanErrorText::default, |hint| unsafe {
                PlanErrorText::copy_from(hint, memory_context)
            });
    }
}

pub type RoutedRelationScanPlanner = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    rti: pg_sys::Index,
    rte: *mut pg_sys::RangeTblEntry,
    error: *mut PlanErrorRecord,
) -> u32;

/// Relation CustomScan planning facet owned by one provider DSO.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelationScanPlannerDescriptor {
    pub struct_size: u32,
    pub context: *mut c_void,
    pub plan_relation: Option<RoutedRelationScanPlanner>,
}

pub type RoutedModifyPlannerPre = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    parse: *mut pg_sys::Query,
    error: *mut PlanErrorRecord,
) -> u32;

pub type RoutedModifyPlannerPost = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    planned: *mut pg_sys::PlannedStmt,
    error: *mut PlanErrorRecord,
) -> u32;

pub type RoutedModifyUpperPlanner = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    root: *mut pg_sys::PlannerInfo,
    stage: pg_sys::UpperRelationKind::Type,
    input_rel: *mut pg_sys::RelOptInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    extra: *mut c_void,
    error: *mut PlanErrorRecord,
) -> u32;

/// Modify planning facet, kept distinct from relation and query planning.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ModifyPlannerDescriptor {
    pub struct_size: u32,
    pub context: *mut c_void,
    pub planner_pre: Option<RoutedModifyPlannerPre>,
    pub planner_post: Option<RoutedModifyPlannerPost>,
    pub create_upper_paths: Option<RoutedModifyUpperPlanner>,
}
