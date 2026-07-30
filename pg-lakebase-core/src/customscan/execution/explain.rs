//! Executor-side `ExplainCustomScan`: deparsed pushed predicates (`Pushed Filter:` etc.).
//! Residual quals stay on PG's `Filter:` line. Uses `pushed_count` /
//! `recheck_count` from `custom_private`, not a blind `custom_exprs` walk.

use core::ffi::CStr;
use core::ptr;
use std::ffi::CString;

use pgrx::pg_guard;
use pgrx::pg_sys;

use crate::customscan::plan_data::custom_exprs::CustomExprSections;
use crate::customscan::plan_data::custom_private::{
    EncodedPrivate, assert_provider_name_matches, decode_private,
};
use crate::customscan::provider::LakebaseCustomScanProvider;
use crate::diag::ReportableError;
use crate::expr::contract::PushdownContract;

const GROUP_LABEL: &CStr = c"Lakebase Pushdown";
const PROP_PROVIDER: &CStr = c"Provider";
const PROP_SCAN_PURPOSE: &CStr = c"Scan Purpose";
const PROP_PUSHED_FILTER: &CStr = c"Pushed Filter";
const PROP_PUSHED_FILTER_EXACT: &CStr = c"Pushed Filter Exact";
const PROP_PUSHED_FILTER_CONSERVATIVE_PRUNING: &CStr =
    c"Pushed Filter Conservative Pruning";
const PROP_RECHECK: &CStr = c"Recheck";

/// `ExplainCustomScan` trampoline (`#[doc(hidden)]` for core-tests).
///
/// # Safety
///
/// PostgreSQL calls this callback with the initialized CustomScan plan and
/// live ExplainState for the current EXPLAIN operation.
#[doc(hidden)]
#[pg_guard]
#[allow(
    clippy::extra_unused_type_parameters,
    reason = "The `P` parameter is required for monomorphic dispatch via \
              `method_tables_for::<P>()`: each provider type gets its own \
              cached callback-table bundle, and the function pointer \
              stored in `ExplainCustomScan` must be unique per `P` so PG's \
              name -> methods hash resolves correctly. Removing `P` would \
              collapse all providers onto one symbol."
)]
pub unsafe extern "C-unwind" fn explain_custom_scan_trampoline<
    P: LakebaseCustomScanProvider,
>(
    node: *mut pg_sys::CustomScanState,
    ancestors: *mut pg_sys::List,
    es: *mut pg_sys::ExplainState,
) {
    let plan = unsafe { (*node).ss.ps.plan };
    let cscan = plan as *mut pg_sys::CustomScan;
    let priv_payload: EncodedPrivate =
        unsafe { decode_private((*cscan).custom_private) }.report_unwrap();

    assert_provider_name_matches(
        priv_payload.provider_id_or_name.as_c_str(),
        P::NAME,
    )
    .report_unwrap();

    let expr_sections = unsafe {
        CustomExprSections::from_custom_exprs(
            (*cscan).custom_exprs,
            priv_payload.pushed_count,
            priv_payload.recheck_count,
        )
    }
    .report_unwrap();

    debug_assert_eq!(
        priv_payload.pushed_contracts.len(),
        priv_payload.pushed_count,
        "ExplainCustomScan: pushed_contracts length must equal pushed_count",
    );
    let mut exact_row_filter_exprs: Vec<*mut pg_sys::Expr> = Vec::new();
    let mut conservative_pruning_exprs: Vec<*mut pg_sys::Expr> = Vec::new();
    for (expr, contract) in expr_sections
        .pushed()
        .iter()
        .zip(priv_payload.pushed_contracts.iter())
    {
        match contract {
            PushdownContract::ExactRowFilter => exact_row_filter_exprs.push(*expr),
            PushdownContract::ConservativePruning => {
                conservative_pruning_exprs.push(*expr)
            }
        }
    }

    let is_text =
        unsafe { (*es).format == pg_sys::ExplainFormat::EXPLAIN_FORMAT_TEXT };
    let verbose = unsafe { (*es).verbose };

    if priv_payload.purpose.is_modify() || verbose {
        unsafe {
            pg_sys::ExplainPropertyText(
                PROP_SCAN_PURPOSE.as_ptr(),
                priv_payload.purpose.label().as_ptr(),
                es,
            );
        }
    }

    let need_deparse = !expr_sections.pushed().is_empty()
        || (verbose && !expr_sections.recheck().is_empty());
    let dpcontext: *mut pg_sys::List = if need_deparse {
        unsafe {
            pg_sys::set_deparse_context_plan((*es).deparse_cxt, plan, ancestors)
        }
    } else {
        ptr::null_mut()
    };

    if is_text {
        if verbose {
            unsafe {
                pg_sys::ExplainPropertyText(
                    PROP_PROVIDER.as_ptr(),
                    priv_payload.provider_id_or_name.as_ptr(),
                    es,
                );
            }
            unsafe {
                emit_section_exprs(
                    es,
                    is_text,
                    PROP_PUSHED_FILTER_EXACT,
                    dpcontext,
                    &exact_row_filter_exprs,
                );
                emit_section_exprs(
                    es,
                    is_text,
                    PROP_PUSHED_FILTER_CONSERVATIVE_PRUNING,
                    dpcontext,
                    &conservative_pruning_exprs,
                );
                emit_section_exprs(
                    es,
                    is_text,
                    PROP_RECHECK,
                    dpcontext,
                    expr_sections.recheck(),
                );
            }
        } else {
            unsafe {
                emit_section_exprs(
                    es,
                    is_text,
                    PROP_PUSHED_FILTER,
                    dpcontext,
                    expr_sections.pushed(),
                );
            }
        }
    } else {
        unsafe {
            pg_sys::ExplainOpenGroup(
                GROUP_LABEL.as_ptr(),
                GROUP_LABEL.as_ptr(),
                true,
                es,
            );
        }

        if verbose {
            unsafe {
                pg_sys::ExplainPropertyText(
                    PROP_PROVIDER.as_ptr(),
                    priv_payload.provider_id_or_name.as_ptr(),
                    es,
                );
            }
            if !exact_row_filter_exprs.is_empty() {
                unsafe {
                    emit_section_exprs(
                        es,
                        is_text,
                        PROP_PUSHED_FILTER_EXACT,
                        dpcontext,
                        &exact_row_filter_exprs,
                    );
                }
            }
            if !conservative_pruning_exprs.is_empty() {
                unsafe {
                    emit_section_exprs(
                        es,
                        is_text,
                        PROP_PUSHED_FILTER_CONSERVATIVE_PRUNING,
                        dpcontext,
                        &conservative_pruning_exprs,
                    );
                }
            }
            if !expr_sections.recheck().is_empty() {
                unsafe {
                    emit_section_exprs(
                        es,
                        is_text,
                        PROP_RECHECK,
                        dpcontext,
                        expr_sections.recheck(),
                    );
                }
            }
        } else if !expr_sections.pushed().is_empty() {
            unsafe {
                emit_section_exprs(
                    es,
                    is_text,
                    PROP_PUSHED_FILTER,
                    dpcontext,
                    expr_sections.pushed(),
                );
            }
        }

        unsafe {
            pg_sys::ExplainCloseGroup(
                GROUP_LABEL.as_ptr(),
                GROUP_LABEL.as_ptr(),
                true,
                es,
            );
        }
    }
}

/// Join deparsed exprs with ` AND `; returns `None` if nothing to print.
///
/// # Safety
///
/// `dpcontext` from `set_deparse_context_plan` when any expr is non-null.
unsafe fn deparse_and_join<I>(
    dpcontext: *mut pg_sys::List,
    exprs: I,
) -> Option<CString>
where
    I: IntoIterator<Item = *mut pg_sys::Expr>,
{
    let mut parts: Vec<String> = Vec::new();
    for expr in exprs {
        if expr.is_null() {
            continue;
        }
        let exprstr = unsafe {
            pg_sys::deparse_expression(
                expr.cast::<pg_sys::Node>(),
                dpcontext,
                false,
                false,
            )
        };
        if exprstr.is_null() {
            continue;
        }
        let part = unsafe { CStr::from_ptr(exprstr) }
            .to_string_lossy()
            .into_owned();
        parts.push(part);
    }

    if parts.is_empty() {
        None
    } else {
        Some(
            CString::new(parts.join(" AND "))
                .expect("deparsed predicate text contains no interior NUL"),
        )
    }
}

/// Emit one EXPLAIN section (TEXT line or structured list).
///
/// # Safety
///
/// Live `es`; non-null `dpcontext` when `exprs` is non-empty.
unsafe fn emit_section_exprs(
    es: *mut pg_sys::ExplainState,
    is_text: bool,
    label: &CStr,
    dpcontext: *mut pg_sys::List,
    exprs: &[*mut pg_sys::Expr],
) {
    if is_text {
        if let Some(joined) =
            unsafe { deparse_and_join(dpcontext, exprs.iter().copied()) }
        {
            unsafe {
                pg_sys::ExplainPropertyText(label.as_ptr(), joined.as_ptr(), es);
            }
        }
    } else {
        unsafe {
            let mut list: *mut pg_sys::List = ptr::null_mut();
            for expr in exprs {
                if expr.is_null() {
                    continue;
                }
                let exprstr = pg_sys::deparse_expression(
                    expr.cast::<pg_sys::Node>(),
                    dpcontext,
                    false,
                    false,
                );
                if exprstr.is_null() {
                    continue;
                }
                list = pg_sys::lappend(list, exprstr.cast());
            }
            pg_sys::ExplainPropertyList(label.as_ptr(), list, es);
        }
    }
}
