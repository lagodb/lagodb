use core::ffi::{CStr, c_int, c_void};
use std::panic::AssertUnwindSafe;

use pgrx::{PgTryBuilder, pg_sys};

use crate::diag::PgError;

use super::PgWrapper;

#[cfg(feature = "pg17")]
#[allow(non_snake_case)]
unsafe extern "C-unwind" {
    fn ExecSetParamPlanMulti(
        params: *const pg_sys::Bitmapset,
        econtext: *mut pg_sys::ExprContext,
    );
}

impl PgWrapper {
    /// Fetch one `PARAM_EXTERN` value using PostgreSQL's hook-first semantics.
    ///
    /// `ParamListInfoData.params[]` may have zero elements when `paramFetch`
    /// is installed, so the array address must not be formed before selecting
    /// the non-hook branch.
    ///
    /// # Safety
    ///
    /// `param_list_info` must be NULL or a live PostgreSQL `ParamListInfo`.
    /// Any installed `paramFetch` hook must satisfy PostgreSQL's contract and
    /// return a valid `ParamExternData` pointer for an in-range `param_id`.
    pub(crate) unsafe fn fetch_external_param(
        param_list_info: pg_sys::ParamListInfo,
        param_id: c_int,
    ) -> Result<Option<pg_sys::ParamExternData>, PgError> {
        if param_list_info.is_null()
            || param_id <= 0
            || param_id > unsafe { (*param_list_info).numParams }
        {
            return Ok(None);
        }

        let param_list_info = AssertUnwindSafe(param_list_info);
        unsafe {
            PgTryBuilder::new(move || {
                let param_list_info = *param_list_info;
                let mut workspace = pg_sys::ParamExternData::default();
                let param = match (*param_list_info).paramFetch {
                    Some(fetch) => {
                        fetch(param_list_info, param_id, false, &mut workspace)
                    }
                    None => (*param_list_info)
                        .params
                        .as_ptr()
                        .add((param_id - 1) as usize),
                };
                Ok(param.read())
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
            .map(Some)
        }
    }

    /// Execute all pending InitPlans referenced by `params`.
    ///
    /// # Safety
    ///
    /// `params` must point to a live PostgreSQL `Bitmapset`, and `econtext`
    /// must be the live expression context belonging to the same executor.
    pub(crate) unsafe fn exec_set_param_plan_multi(
        params: *const pg_sys::Bitmapset,
        econtext: *mut pg_sys::ExprContext,
    ) -> Result<(), PgError> {
        let params = AssertUnwindSafe(params);
        let econtext = AssertUnwindSafe(econtext);
        unsafe {
            PgTryBuilder::new(move || {
                #[cfg(feature = "pg17")]
                ExecSetParamPlanMulti(*params, *econtext);
                #[cfg(not(feature = "pg17"))]
                compile_error!(
                    "pg-lakebase-core: ExecSetParamPlanMulti FFI binding only declared for pg17"
                );
                Ok(())
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    /// Return PostgreSQL's owned display name for a valid type OID.
    ///
    /// # Safety
    ///
    /// This must run inside a live PostgreSQL backend with an active memory
    /// context, and `oid` must be a type OID supplied by PostgreSQL.
    pub(crate) unsafe fn format_type_owned(
        oid: pg_sys::Oid,
    ) -> Result<String, PgError> {
        let raw = unsafe {
            PgTryBuilder::new(move || Ok(pg_sys::format_type_be(oid)))
                .catch_others(|err| Err(PgError::from_caught(err)))
                .execute()
        }?;

        // SAFETY: format_type_be returns a NUL-terminated palloc'd string on
        // success for a valid type OID.
        let owned = unsafe { CStr::from_ptr(raw) }
            .to_string_lossy()
            .into_owned();
        unsafe { pg_sys::pfree(raw.cast::<c_void>()) };
        Ok(owned)
    }
}
