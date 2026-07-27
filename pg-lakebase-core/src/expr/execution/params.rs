//! Executor-side resolution of `PARAM_EXTERN` / `PARAM_EXEC` for `BeginCustomScan` /
//! `ReScanCustomScan` (mirrors `ExecEvalParamExtern` / `ExecEvalParamExec`).
//! Does not copy `Datum`s; values live in per-query executor storage.

use core::ffi::c_int;

use pgrx::pg_sys;

use crate::expr::contract::ParamKey;
use crate::wrapper::PgWrapper;

pub use super::error::RuntimeParamError;
use super::value::PgDatumRef;

/// Executor-resolved parameter storage. Raw Datum access is only exposed
/// through a lifetime-bound PgParamValue view.
#[derive(Debug)]
pub struct ResolvedParam {
    key: ParamKey,
    type_oid: pg_sys::Oid,
    collid: pg_sys::Oid,
    datum: pg_sys::Datum,
    is_null: bool,
}

impl ResolvedParam {
    pub(crate) fn new(
        key: ParamKey,
        type_oid: pg_sys::Oid,
        collid: pg_sys::Oid,
        datum: pg_sys::Datum,
        is_null: bool,
    ) -> Self {
        Self {
            key,
            type_oid,
            collid,
            datum,
            is_null,
        }
    }

    /// Construct a resolved parameter from executor-owned raw parts.
    ///
    /// # Safety
    ///
    /// When `is_null` is false, `datum` must use the PostgreSQL representation
    /// required by `type_oid`; a pass-by-reference value must remain valid for
    /// every [`PgParamValue`] borrowed from the returned value. `collid` must
    /// be the collation of the same executor parameter slot. When `is_null` is
    /// true, consumers must not inspect the raw datum payload.
    #[doc(hidden)]
    pub unsafe fn from_raw_parts(
        key: ParamKey,
        type_oid: pg_sys::Oid,
        collid: pg_sys::Oid,
        datum: pg_sys::Datum,
        is_null: bool,
    ) -> Self {
        Self::new(key, type_oid, collid, datum, is_null)
    }

    #[inline]
    pub fn key(&self) -> ParamKey {
        self.key
    }

    #[inline]
    pub fn value(&self) -> PgParamValue<'_> {
        PgParamValue { inner: self }
    }
}

/// Borrowed provider view over one executor-resolved parameter.
#[derive(Clone, Copy, Debug)]
pub struct PgParamValue<'a> {
    inner: &'a ResolvedParam,
}

impl<'a> PgParamValue<'a> {
    #[inline]
    pub fn key(self) -> ParamKey {
        self.inner.key
    }

    #[inline]
    pub fn type_oid(self) -> pg_sys::Oid {
        self.inner.type_oid
    }

    #[inline]
    pub fn collid(self) -> pg_sys::Oid {
        self.inner.collid
    }

    #[inline]
    pub fn is_null(self) -> bool {
        self.inner.is_null
    }

    #[inline]
    pub fn datum(self) -> PgDatumRef<'a> {
        // SAFETY: the returned view cannot outlive the borrowed
        // ResolvedParam, whose Datum is retained by the executor lifecycle.
        unsafe { PgDatumRef::from_raw(self.inner.datum) }
    }
}

/// `PARAM_EXTERN` slot to resolve (plan-time `Param.paramtype` / `paramcollid`).
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct ExternParamRef {
    pub param_id: c_int,
    pub expected_type: pg_sys::Oid,
    pub collid: pg_sys::Oid,
}

/// `PARAM_EXEC` slot to resolve (`Param.paramid` indexes `es_param_exec_vals`).
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct ExecParamRef {
    pub param_id: c_int,
    pub expected_type: pg_sys::Oid,
    pub collid: pg_sys::Oid,
}

/// Executor-side resolver for `PARAM_EXTERN` and `PARAM_EXEC` values.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct RuntimeParamResolver {
    estate: *mut pg_sys::EState,
}

impl RuntimeParamResolver {
    /// Build a resolver for the current executor pass.
    ///
    /// # Safety
    ///
    /// `estate` must be non-NULL and live for the current executor pass. Its
    /// parameter arrays and `es_param_list_info`, when present, must remain
    /// valid for every call made through the resolver.
    #[inline]
    pub unsafe fn new(estate: *mut pg_sys::EState) -> Self {
        debug_assert!(
            !estate.is_null(),
            "RuntimeParamResolver::new: estate must be non-null"
        );
        Self { estate }
    }

    /// Resolve extern then exec params; output order matches input chain.
    ///
    /// # Safety
    ///
    /// Each EXTERN id must be one-based. Each EXEC id must be a valid
    /// zero-based index into `estate.es_param_exec_vals`, and its pending
    /// `execPlan`, if any, must already have been materialized. All metadata
    /// must originate from the plan being executed by `estate`. Any returned
    /// pass-by-reference `Datum` must be consumed while the executor-owned
    /// parameter storage remains live.
    pub unsafe fn resolve(
        self,
        extern_params: &[ExternParamRef],
        exec_params: &[ExecParamRef],
    ) -> Result<Vec<ResolvedParam>, RuntimeParamError> {
        let mut out = Vec::with_capacity(extern_params.len() + exec_params.len());

        let param_list_info = unsafe { (*self.estate).es_param_list_info };
        for spec in extern_params {
            out.push(unsafe { Self::resolve_extern(param_list_info, *spec) }?);
        }

        if !exec_params.is_empty() {
            let exec_vals = unsafe { (*self.estate).es_param_exec_vals };
            for spec in exec_params {
                out.push(unsafe { Self::resolve_exec(exec_vals, *spec) });
            }
        }

        Ok(out)
    }

    /// # Safety
    ///
    /// `param_list_info` may be NULL; when non-NULL it and any installed
    /// `paramFetch` hook must satisfy PostgreSQL's `ParamListInfo` contract.
    unsafe fn resolve_extern(
        param_list_info: *mut pg_sys::ParamListInfoData,
        spec: ExternParamRef,
    ) -> Result<ResolvedParam, RuntimeParamError> {
        let param_id = spec.param_id;
        let prm =
            unsafe { PgWrapper::fetch_external_param(param_list_info, param_id) }
                .map_err(|source| RuntimeParamError::FetchExternal {
                    param_id,
                    source,
                })?
                .ok_or(RuntimeParamError::NoValueFound { param_id })?;

        // OidIsValid(prm->ptype) — InvalidOid is the same as Oid::INVALID.
        if prm.ptype == pg_sys::Oid::INVALID {
            return Err(RuntimeParamError::NoValueFound { param_id });
        }

        if spec.expected_type != pg_sys::Oid::INVALID
            && prm.ptype != spec.expected_type
        {
            let runtime_type_name =
                unsafe { PgWrapper::format_type_owned(prm.ptype) }.map_err(
                    |source| RuntimeParamError::FormatType {
                        type_oid: prm.ptype,
                        source,
                    },
                )?;
            let expected_type_name =
                unsafe { PgWrapper::format_type_owned(spec.expected_type) }.map_err(
                    |source| RuntimeParamError::FormatType {
                        type_oid: spec.expected_type,
                        source,
                    },
                )?;
            return Err(RuntimeParamError::TypeMismatch {
                param_id,
                runtime_type_name,
                expected_type_name,
            });
        }

        Ok(ResolvedParam::new(
            ParamKey {
                paramkind: pg_sys::ParamKind::PARAM_EXTERN,
                param_id,
            },
            prm.ptype,
            spec.collid,
            prm.value,
            prm.isnull,
        ))
    }

    /// # Safety
    ///
    /// `exec_vals` must point at `EState.es_param_exec_vals`; `spec.param_id`
    /// must be a valid index whose pending `execPlan` has been materialized.
    unsafe fn resolve_exec(
        exec_vals: *mut pg_sys::ParamExecData,
        spec: ExecParamRef,
    ) -> ResolvedParam {
        let param_id = spec.param_id;
        let slot_ptr: *mut pg_sys::ParamExecData =
            unsafe { exec_vals.add(param_id as usize) };
        // SAFETY: slot_ptr points at a valid ParamExecData entry.
        let slot = unsafe { &*slot_ptr };
        ResolvedParam::new(
            ParamKey {
                paramkind: pg_sys::ParamKind::PARAM_EXEC,
                param_id,
            },
            spec.expected_type,
            spec.collid,
            slot.value,
            slot.isnull,
        )
    }
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
