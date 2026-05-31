//! Runtime resolution of `PARAM_EXTERN` / `PARAM_EXEC` for `BeginCustomScan` /
//! `ReScanCustomScan` (mirrors `ExecEvalParamExtern` / `ExecEvalParamExec`).
//! Does not copy `Datum`s; values live in per-query executor storage.

use core::ffi::c_int;

use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::diag::error::PgReportError;
use crate::expr::nodes::PgParamValue;

/// `PARAM_EXTERN` slot to resolve (plan-time `Param.paramtype` / `paramcollid`).
#[derive(Clone, Copy, Debug)]
pub struct ExternParamRef {
    pub param_id: c_int,
    pub expected_type: pg_sys::Oid,
    pub collid: pg_sys::Oid,
}

/// `PARAM_EXEC` slot to resolve (`Param.paramid` indexes `es_param_exec_vals`).
#[derive(Clone, Copy, Debug)]
pub struct ExecParamRef {
    pub param_id: c_int,
    pub expected_type: pg_sys::Oid,
    pub collid: pg_sys::Oid,
}

/// Executor-side resolver for `PARAM_EXTERN` and `PARAM_EXEC` values.
#[derive(Debug, Clone, Copy)]
pub struct RuntimeParamResolver {
    estate: *mut pg_sys::EState,
    econtext: *mut pg_sys::ExprContext,
}

impl RuntimeParamResolver {
    /// Build a resolver for the current executor pass.
    ///
    /// # Safety
    ///
    /// `estate` and `econtext` must be valid for the current executor pass.
    #[inline]
    pub unsafe fn new(
        estate: *mut pg_sys::EState,
        econtext: *mut pg_sys::ExprContext,
    ) -> Self {
        debug_assert!(
            !estate.is_null(),
            "RuntimeParamResolver::new: estate must be non-null"
        );
        debug_assert!(
            !econtext.is_null(),
            "RuntimeParamResolver::new: econtext must be non-null"
        );
        Self { estate, econtext }
    }

    /// Resolve extern then exec params; output order matches input chain.
    ///
    /// # Safety
    ///
    /// Consume returned `Datum`s within the same `Begin` / `ReScan`.
    pub unsafe fn resolve(
        self,
        extern_params: &[ExternParamRef],
        exec_params: &[ExecParamRef],
    ) -> Result<Vec<PgParamValue>, PgReportError> {
        let mut out = Vec::with_capacity(extern_params.len() + exec_params.len());

        let param_list_info = unsafe { (*self.estate).es_param_list_info };
        for spec in extern_params {
            out.push(unsafe { self.resolve_extern(param_list_info, *spec) }?);
        }

        if !exec_params.is_empty() {
            unsafe { self.materialize_exec_pending(exec_params) };

            let exec_vals = unsafe { (*self.estate).es_param_exec_vals };
            for spec in exec_params {
                out.push(unsafe { self.resolve_exec(exec_vals, *spec) }?);
            }
        }

        debug_assert_param_key_invariant(&out);

        Ok(out)
    }

    /// # Safety
    ///
    /// `param_list_info` may be null; when non-null it must be valid for this invocation.
    unsafe fn resolve_extern(
        self,
        param_list_info: *mut pg_sys::ParamListInfoData,
        spec: ExternParamRef,
    ) -> Result<PgParamValue, PgReportError> {
        let param_id = spec.param_id;

        if param_list_info.is_null() {
            return Err(raise_no_value_found(param_id));
        }

        // `paramid` is 1-based; the params array is 0-based.
        if param_id <= 0 {
            return Err(raise_no_value_found(param_id));
        }
        let idx = (param_id - 1) as usize;

        // SAFETY: param_list_info is non-null and points at a valid
        // ParamListInfoData; numParams matches the trailing flexible array.
        let n_params = unsafe { (*param_list_info).numParams } as usize;
        if idx >= n_params {
            return Err(raise_no_value_found(param_id));
        }

        // Pointer to the slot; PG's flexible-array binding requires we go
        // through a raw pointer rather than indexing the Rust struct.
        let prm_slot: *mut pg_sys::ParamExternData =
            unsafe { (*param_list_info).params.as_mut_ptr().add(idx) };

        // paramFetch may return the slot or this workspace buffer.
        let mut workspace = pg_sys::ParamExternData::default();

        // SAFETY: param_list_info is non-null; we read the ParamFetchHook field.
        let prm_ptr: *mut pg_sys::ParamExternData =
            match unsafe { (*param_list_info).paramFetch } {
                Some(fetch) => unsafe {
                    fetch(param_list_info, param_id, false, &mut workspace)
                },
                None => prm_slot,
            };

        if prm_ptr.is_null() {
            return Err(raise_no_value_found(param_id));
        }

        // SAFETY: prm_ptr is either the slot pointer or &workspace, both valid
        // for the duration of this borrow.
        let prm = unsafe { &*prm_ptr };

        // OidIsValid(prm->ptype) — InvalidOid is the same as Oid::INVALID.
        if prm.ptype == pg_sys::Oid::INVALID {
            return Err(raise_no_value_found(param_id));
        }

        if spec.expected_type != pg_sys::Oid::INVALID
            && prm.ptype != spec.expected_type
        {
            return Err(raise_type_mismatch(param_id, prm.ptype, spec.expected_type));
        }

        Ok(PgParamValue {
            param_id,
            paramkind: pg_sys::ParamKind::PARAM_EXTERN,
            type_oid: prm.ptype,
            collid: spec.collid,
            datum: prm.value,
            is_null: prm.isnull,
        })
    }

    /// Materialize pending `PARAM_EXEC` InitPlans (`ExecSetParamPlanMulti` or per-id fallback).
    ///
    /// # Safety
    ///
    /// The resolver's `estate` and `econtext` must be valid for the current executor pass.
    unsafe fn materialize_exec_pending(self, exec_params: &[ExecParamRef]) {
        let exec_vals = unsafe { (*self.estate).es_param_exec_vals };
        if exec_vals.is_null() {
            // No PARAM_EXEC slots are allocated in this plan; if the caller
            // still references them we let resolve_exec turn it into a sane
            // null below. (PG itself would simply read past `exec_vals`, which
            // is unsound; we choose to short-circuit.)
            return;
        }

        let mut pending: *mut pg_sys::Bitmapset = core::ptr::null_mut();
        let mut any_pending = false;
        for spec in exec_params {
            let id = spec.param_id;
            if id < 0 {
                // Defensive: a negative id indexes before the array. Skip;
                // resolve_exec will raise below.
                continue;
            }
            let slot_ptr: *mut pg_sys::ParamExecData =
                unsafe { exec_vals.add(id as usize) };
            let exec_plan = unsafe { (*slot_ptr).execPlan };
            if !exec_plan.is_null() {
                any_pending = true;
                // SAFETY: bms_add_member returns a (possibly relocated) bitmap.
                pending = unsafe { pg_sys::bms_add_member(pending, id) };
            }
        }

        if !any_pending {
            // Nothing pending — drop the (still-empty) bitmap if any. Note
            // that bms_add_member with a NULL input returning a fresh bitmap
            // can't have happened here because `any_pending` was false.
            debug_assert!(pending.is_null());
            return;
        }

        // Prefer the multi-id API when the binding is exposed.
        if let Some(set_multi) = ffi::exec_set_param_plan_multi() {
            // SAFETY: pending is a valid Bitmapset *; econtext is non-null.
            unsafe { set_multi(pending, self.econtext) };
        } else {
            let set_one = ffi::exec_set_param_plan();
            for spec in exec_params {
                let id = spec.param_id;
                if id < 0 {
                    continue;
                }
                let slot_ptr: *mut pg_sys::ParamExecData =
                    unsafe { exec_vals.add(id as usize) };
                let exec_plan = unsafe { (*slot_ptr).execPlan };
                if exec_plan.is_null() {
                    continue;
                }
                // SAFETY: execPlan is a SubPlanState* per nodeSubplan.h.
                unsafe {
                    set_one(exec_plan as *mut pg_sys::SubPlanState, self.econtext);
                }
            }
        }

        // SAFETY: pending was allocated by bms_add_member; free it now.
        if !pending.is_null() {
            unsafe { pg_sys::bms_free(pending) };
        }
    }

    /// # Safety
    ///
    /// `exec_vals` must point at `EState.es_param_exec_vals` when non-null.
    unsafe fn resolve_exec(
        self,
        exec_vals: *mut pg_sys::ParamExecData,
        spec: ExecParamRef,
    ) -> Result<PgParamValue, PgReportError> {
        let param_id = spec.param_id;

        if exec_vals.is_null() || param_id < 0 {
            // No slots allocated, or an out-of-range id. The caller
            // (BeginCustomScan) gathered these ids from the plan's pushed
            // expressions, so reaching this branch indicates an invariant
            // failure in plan-stage walking. Surface it as
            // ERRCODE_INTERNAL_ERROR rather than silently returning NULL.
            return Err(raise_internal(format!(
                "PARAM_EXEC slot {param_id} not available in EState.es_param_exec_vals"
            )));
        }

        let slot_ptr: *mut pg_sys::ParamExecData =
            unsafe { exec_vals.add(param_id as usize) };
        // SAFETY: slot_ptr points at a valid ParamExecData entry.
        let slot = unsafe { &*slot_ptr };

        // After materialize_exec_pending, execPlan should be NULL. Defensive
        // check: if it's still set, our materialization path is broken.
        debug_assert!(
            slot.execPlan.is_null(),
            "PARAM_EXEC slot {} still has a pending execPlan after materialization",
            param_id
        );

        Ok(PgParamValue {
            param_id,
            paramkind: pg_sys::ParamKind::PARAM_EXEC,
            type_oid: spec.expected_type,
            collid: spec.collid,
            datum: slot.value,
            is_null: slot.isnull,
        })
    }
}

#[cfg(debug_assertions)]
fn debug_assert_param_key_invariant(values: &[PgParamValue]) {
    use std::collections::HashMap;

    // Key: the raw `(paramkind, param_id)` tuple. Value: the resolved
    // `(datum.value(), is_null)` pair we expect every entry sharing that key
    // to agree on.
    let mut seen: HashMap<(pg_sys::ParamKind::Type, c_int), (usize, bool)> =
        HashMap::with_capacity(values.len());

    for v in values {
        let key = (v.paramkind, v.param_id);
        let value = (v.datum.value(), v.is_null);
        if let Some(prev) = seen.insert(key, value) {
            debug_assert_eq!(
                prev, value,
                "RuntimeParamResolver: ParamKey (paramkind={}, param_id={}) resolved to \
                two differing values; the produced set must hold at most one \
                value per ParamKey",
                key.0, key.1
            );
        }
    }
}

#[cfg(not(debug_assertions))]
#[inline(always)]
fn debug_assert_param_key_invariant(_values: &[PgParamValue]) {}

fn raise_no_value_found(param_id: c_int) -> PgReportError {
    PgReportError::from_message(
        PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
        format!("no value found for parameter {param_id}"),
    )
}

fn raise_type_mismatch(
    param_id: c_int,
    runtime_type: pg_sys::Oid,
    expected_type: pg_sys::Oid,
) -> PgReportError {
    let runtime_name = format_type_owned(runtime_type);
    let expected_name = format_type_owned(expected_type);
    PgReportError::from_message(
        PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH,
        format!(
            "type of parameter {param_id} ({runtime_name}) does not match that when preparing the plan ({expected_name})"
        ),
    )
}

fn raise_internal(message: String) -> PgReportError {
    PgReportError::from_message(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, message)
}

fn format_type_owned(oid: pg_sys::Oid) -> String {
    // SAFETY: format_type_be is a SQL-safe wrapper around format_type that
    // accepts any Oid (it returns "???" for unknown types).
    let raw = unsafe { pg_sys::format_type_be(oid) };
    if raw.is_null() {
        return format!("oid={}", u32::from(oid));
    }
    // SAFETY: format_type_be returns a NUL-terminated palloc'd C string.
    let owned = unsafe { core::ffi::CStr::from_ptr(raw) }
        .to_string_lossy()
        .into_owned();
    // SAFETY: raw was palloc'd by PG.
    unsafe { pg_sys::pfree(raw as *mut core::ffi::c_void) };
    owned
}

/// `ExecSetParamPlan` / `ExecSetParamPlanMulti` — not exposed by `pgrx-pg-sys` on PG17.
mod ffi {
    use pgrx::pg_sys;

    /// Type of `ExecSetParamPlan(SubPlanState *node, ExprContext *econtext)`.
    pub type ExecSetParamPlanFn = unsafe extern "C-unwind" fn(
        node: *mut pg_sys::SubPlanState,
        econtext: *mut pg_sys::ExprContext,
    );

    /// Type of `ExecSetParamPlanMulti(const Bitmapset *params, ExprContext *econtext)`.
    pub type ExecSetParamPlanMultiFn = unsafe extern "C-unwind" fn(
        params: *const pg_sys::Bitmapset,
        econtext: *mut pg_sys::ExprContext,
    );

    #[cfg(feature = "pg17")]
    #[allow(non_snake_case)]
    unsafe extern "C-unwind" {
        pub fn ExecSetParamPlan(
            node: *mut pg_sys::SubPlanState,
            econtext: *mut pg_sys::ExprContext,
        );

        pub fn ExecSetParamPlanMulti(
            params: *const pg_sys::Bitmapset,
            econtext: *mut pg_sys::ExprContext,
        );
    }

    #[inline]
    pub fn exec_set_param_plan() -> ExecSetParamPlanFn {
        #[cfg(feature = "pg17")]
        {
            ExecSetParamPlan
        }
        #[cfg(not(feature = "pg17"))]
        {
            // Future PG versions add their own `cfg` arm.
            compile_error!(
                "pg-lakebase-core: ExecSetParamPlan FFI binding only declared for pg17"
            );
        }
    }

    #[inline]
    pub fn exec_set_param_plan_multi() -> Option<ExecSetParamPlanMultiFn> {
        #[cfg(feature = "pg17")]
        {
            Some(ExecSetParamPlanMulti)
        }
        #[cfg(not(feature = "pg17"))]
        {
            None
        }
    }
}

#[cfg(feature = "pg_test")]
#[doc(hidden)]
pub mod ffi_accessors {
    pub use super::ffi::{
        ExecSetParamPlanFn, ExecSetParamPlanMultiFn, exec_set_param_plan,
        exec_set_param_plan_multi,
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extern_param_ref_is_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<ExternParamRef>();
        assert_copy::<ExecParamRef>();
    }
}
