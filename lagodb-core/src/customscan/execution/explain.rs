//! Executor-side `ExplainCustomScan` for pushed predicates and EPQ recheck.

use core::ffi::CStr;
use core::ptr;
use std::ffi::CString;

use pgrx::pg_guard;
use pgrx::pg_sys;

use crate::customscan::error::CustomScanError;
use crate::customscan::execution::state::CustomScanStateWrapper;
use crate::customscan::filter::CustomScanFilters;
use crate::customscan::plan_data::custom_exprs::CustomExprSections;
use crate::customscan::plan_data::custom_private::{
    EncodedPrivate, assert_provider_name_matches, decode_private,
};
use crate::customscan::provider::LagodbCustomScanProvider;
use crate::diag::ReportableError;

const GROUP_LABEL: &CStr = c"LagoDB Pushdown";
const PROP_PROVIDER: &CStr = c"Provider";
const PROP_SCAN_PURPOSE: &CStr = c"Scan Purpose";
const PROP_PUSHED_FILTER: &CStr = c"Pushed Filter";
const PROP_PUSHED_FILTER_EXACT: &CStr = c"Pushed Filter Exact";
const PROP_PUSHED_FILTER_CONSERVATIVE: &CStr = c"Pushed Filter Conservative";
const PROP_RECHECK: &CStr = c"Recheck";

/// `ExplainCustomScan` trampoline (`#[doc(hidden)]` for core-tests).
///
/// # Safety
///
/// PostgreSQL calls this callback with the initialized CustomScan plan and
/// live ExplainState for the current EXPLAIN operation.
#[doc(hidden)]
#[pg_guard]
pub unsafe extern "C-unwind" fn explain_custom_scan_trampoline<
    P: LagodbCustomScanProvider,
>(
    node: *mut pg_sys::CustomScanState,
    ancestors: *mut pg_sys::List,
    es: *mut pg_sys::ExplainState,
) {
    unsafe { explain_custom_scan::<P>(node, ancestors, es) }.report_unwrap();
}

unsafe fn explain_custom_scan<P: LagodbCustomScanProvider>(
    node: *mut pg_sys::CustomScanState,
    ancestors: *mut pg_sys::List,
    es: *mut pg_sys::ExplainState,
) -> Result<(), CustomScanError> {
    let plan = unsafe { (*node).ss.ps.plan };
    let cscan = plan as *mut pg_sys::CustomScan;
    let priv_payload: EncodedPrivate =
        unsafe { decode_private((*cscan).custom_private) }?;

    assert_provider_name_matches(
        priv_payload.provider_id_or_name.as_c_str(),
        P::NAME,
    )?;

    let expr_sections = unsafe {
        CustomExprSections::from_custom_exprs(
            (*cscan).custom_exprs,
            priv_payload.binding_count,
            priv_payload.planned_filter_count,
        )
    }?;
    let wrapper = unsafe { CustomScanStateWrapper::<P>::from_node_ptr(node) };
    let contracts = match wrapper.filters.as_ref() {
        Some(filters) => filters.explain_contracts(),
        None => unsafe {
            CustomScanFilters::<P>::decode_explain_contracts(&priv_payload)
        }?,
    };
    let mut exact = Vec::new();
    let mut conservative = Vec::new();
    for (&expr, contract) in expr_sections.pushed().iter().zip(contracts.iter()) {
        if contract.requires_recheck() {
            exact.push(expr);
        } else {
            conservative.push(expr);
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

    let need_deparse = !expr_sections.pushed().is_empty();
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
                    &exact,
                );
                emit_section_exprs(
                    es,
                    is_text,
                    PROP_PUSHED_FILTER_CONSERVATIVE,
                    dpcontext,
                    &conservative,
                );
                emit_section_exprs(es, is_text, PROP_RECHECK, dpcontext, &exact);
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
            unsafe {
                emit_section_exprs(
                    es,
                    is_text,
                    PROP_PUSHED_FILTER_EXACT,
                    dpcontext,
                    &exact,
                );
                emit_section_exprs(
                    es,
                    is_text,
                    PROP_PUSHED_FILTER_CONSERVATIVE,
                    dpcontext,
                    &conservative,
                );
                emit_section_exprs(es, is_text, PROP_RECHECK, dpcontext, &exact);
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

        unsafe {
            pg_sys::ExplainCloseGroup(
                GROUP_LABEL.as_ptr(),
                GROUP_LABEL.as_ptr(),
                true,
                es,
            );
        }
    }
    Ok(())
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
    if exprs.is_empty() {
        return;
    }
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
