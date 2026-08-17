//! Planning-time predicate descriptions and `ExplainForeignScan` emission.

use core::ffi::{CStr, c_void};
use core::ptr;
use std::ffi::CString;

use pgrx::pg_guard;
use pgrx::pg_sys;

use crate::expr::pushdown::{
    FilterBindingExpr, FilterValueSourceKind, NegotiatedFilterSet,
};
use crate::fdw::{ForeignPrivateReader, ForeignPrivateWriter};

use super::contract::FdwScan;
use super::error::{ForeignScanError, ForeignScanPhase};
use super::plan_filter::ForeignFilterExplainValues;
use super::private::decode_scan_explain_private;

const GROUP_LABEL: &CStr = c"Lakebase Pushdown";
const PROP_PROVIDER: &CStr = c"Provider";
const PROP_PUSHED_FILTER: &CStr = c"Pushed Filter";
const PROP_PUSHED_FILTER_EXACT: &CStr = c"Pushed Filter Exact";
const PROP_PUSHED_FILTER_CONSERVATIVE: &CStr = c"Pushed Filter Conservative";
const PROP_RECHECK: &CStr = c"Recheck";

struct ForeignScanExplainEntry {
    requires_recheck: bool,
    text: String,
}

pub(crate) struct ForeignScanExplain {
    entries: Vec<ForeignScanExplainEntry>,
}

impl ForeignScanExplain {
    /// Build provider-owned predicate descriptions before PostgreSQL replaces
    /// outer Vars with executor parameters.
    ///
    /// # Safety
    ///
    /// Every binding expression in `filters` must remain live in the current
    /// planner context for this call.
    pub(crate) unsafe fn build<P: FdwScan>(
        filters: &NegotiatedFilterSet<P::PlannedPredicate>,
    ) -> Result<Self, ForeignScanError> {
        let mut entries = Vec::with_capacity(filters.planned.len());
        for filter in &filters.planned {
            let start = filter.binding_start;
            let end = start + filter.binding_count;
            let bindings = &filters.bindings[start..end];
            let mut values = Vec::with_capacity(bindings.len());
            for (local_index, binding) in bindings.iter().enumerate() {
                values
                    .push(unsafe { explain_binding(binding, start + local_index) }?);
            }
            let values = ForeignFilterExplainValues::new(&values);
            let Some(text) = P::explain_filter(&filter.planned, values)? else {
                continue;
            };
            if text.is_empty() {
                return Err(ForeignScanError::framework(
                    "FDW provider returned an empty pushed-filter description",
                ));
            }
            entries.push(ForeignScanExplainEntry {
                requires_recheck: filter.effective.contract.requires_recheck(),
                text,
            });
        }
        Ok(Self { entries })
    }

    pub(crate) fn encode(&self) -> Result<*mut pg_sys::List, ForeignScanError> {
        ForeignPrivateWriter::encode_list(|writer| {
            for entry in &self.entries {
                writer.append_nested(|record| {
                    record
                        .append_bool(entry.requires_recheck)
                        .append_str(&entry.text);
                });
            }
            Ok(())
        })
    }

    /// # Safety
    ///
    /// `raw` must be the dedicated plan-owned EXPLAIN list encoded by
    /// [`Self::encode`].
    unsafe fn decode(raw: *mut pg_sys::List) -> Result<Self, ForeignScanError> {
        unsafe {
            ForeignPrivateReader::decode_list(raw, |reader| {
                let mut entries = Vec::with_capacity(reader.remaining());
                while reader.remaining() > 0 {
                    entries.push(reader.read_nested(|record| {
                        let requires_recheck = record.read_bool()?;
                        let text = record.read_str()?;
                        if text.is_empty() {
                            return Err(ForeignScanError::framework(
                                "FDW EXPLAIN data contains an empty predicate",
                            ));
                        }
                        Ok(ForeignScanExplainEntry {
                            requires_recheck,
                            text,
                        })
                    })?);
                }
                Ok(Self { entries })
            })
        }
    }

    /// # Safety
    ///
    /// `es` must be the live state for the current EXPLAIN operation.
    unsafe fn emit<P: FdwScan>(&self, es: *mut pg_sys::ExplainState) {
        if self.entries.is_empty() {
            return;
        }
        let is_text =
            unsafe { (*es).format == pg_sys::ExplainFormat::EXPLAIN_FORMAT_TEXT };
        let verbose = unsafe { (*es).verbose };

        if !is_text {
            unsafe {
                pg_sys::ExplainOpenGroup(
                    GROUP_LABEL.as_ptr(),
                    GROUP_LABEL.as_ptr(),
                    true,
                    es,
                );
            }
        }

        if verbose {
            unsafe {
                pg_sys::ExplainPropertyText(
                    PROP_PROVIDER.as_ptr(),
                    P::NAME.as_ptr(),
                    es,
                );
            }
            let exact = self
                .entries
                .iter()
                .filter(|entry| entry.requires_recheck)
                .map(|entry| entry.text.as_str())
                .collect::<Vec<_>>();
            let conservative = self
                .entries
                .iter()
                .filter(|entry| !entry.requires_recheck)
                .map(|entry| entry.text.as_str())
                .collect::<Vec<_>>();
            unsafe {
                emit_section(es, is_text, PROP_PUSHED_FILTER_EXACT, &exact);
                emit_section(
                    es,
                    is_text,
                    PROP_PUSHED_FILTER_CONSERVATIVE,
                    &conservative,
                );
                emit_section(es, is_text, PROP_RECHECK, &exact);
            }
        } else {
            let pushed = self
                .entries
                .iter()
                .map(|entry| entry.text.as_str())
                .collect::<Vec<_>>();
            unsafe { emit_section(es, is_text, PROP_PUSHED_FILTER, &pushed) };
        }

        if !is_text {
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
}

/// # Safety
///
/// `binding.expr` must be a live planner-owned expression. Constants and
/// external parameters are deparsed without a namespace; executor parameters
/// and outer values use stable slot placeholders and are never deparsed.
unsafe fn explain_binding(
    binding: &FilterBindingExpr,
    global_index: usize,
) -> Result<String, ForeignScanError> {
    match binding.metadata.source_kind {
        FilterValueSourceKind::Constant | FilterValueSourceKind::ExternalParam => {
            let text = unsafe {
                pg_sys::deparse_expression(
                    binding.expr.cast::<pg_sys::Node>(),
                    ptr::null_mut(),
                    false,
                    false,
                )
            };
            if text.is_null() {
                return Err(ForeignScanError::framework(
                    "PostgreSQL could not deparse an FDW filter value",
                ));
            }
            unsafe { CStr::from_ptr(text) }
                .to_str()
                .map(str::to_owned)
                .map_err(|_| {
                    ForeignScanError::framework("FDW filter value is not valid UTF-8")
                })
        }
        FilterValueSourceKind::ExecParam | FilterValueSourceKind::OuterValue => {
            Ok(format!("${}", global_index + 1))
        }
    }
}

/// # Safety
///
/// `es` must be live. Every string is owned for the duration of the PostgreSQL
/// property call.
unsafe fn emit_section(
    es: *mut pg_sys::ExplainState,
    is_text: bool,
    label: &CStr,
    predicates: &[&str],
) {
    if predicates.is_empty() {
        return;
    }
    if is_text {
        let joined = CString::new(predicates.join(" AND "))
            .expect("validated FDW EXPLAIN text contains no NUL bytes");
        unsafe {
            pg_sys::ExplainPropertyText(label.as_ptr(), joined.as_ptr(), es);
        }
        return;
    }

    let values = predicates
        .iter()
        .map(|value| {
            CString::new(*value)
                .expect("validated FDW EXPLAIN text contains no NUL bytes")
        })
        .collect::<Vec<_>>();
    let mut list = ptr::null_mut();
    for value in &values {
        list = unsafe {
            pg_sys::lappend(list, value.as_ptr().cast_mut().cast::<c_void>())
        };
    }
    unsafe { pg_sys::ExplainPropertyList(label.as_ptr(), list, es) };
}

/// # Safety
///
/// PostgreSQL invokes this callback with an initialized ForeignScan plan and a
/// live ExplainState for the current EXPLAIN operation.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn explain_foreign_scan<P: FdwScan>(
    node: *mut pg_sys::ForeignScanState,
    es: *mut pg_sys::ExplainState,
) {
    let prior_ctx = unsafe { pg_sys::CurrentMemoryContext };
    let result = (|| {
        if node.is_null() || es.is_null() {
            return Err(ForeignScanError::framework(
                "ExplainForeignScan received a NULL PostgreSQL pointer",
            ));
        }
        let plan = unsafe { (*node).ss.ps.plan } as *mut pg_sys::ForeignScan;
        if plan.is_null()
            || unsafe { (*plan).scan.plan.type_ } != pg_sys::NodeTag::T_ForeignScan
        {
            return Err(ForeignScanError::framework(
                "ExplainForeignScan plan is not a ForeignScan node",
            ));
        }

        let raw = unsafe { decode_scan_explain_private::<P>((*plan).fdw_private) }?;
        let explain = unsafe { ForeignScanExplain::decode(raw) }?;
        unsafe { explain.emit::<P>(es) };
        Ok::<(), ForeignScanError>(())
    })();

    if let Err(error) = result {
        error
            .with_callback_phase::<P>(ForeignScanPhase::Explain)
            .report_after_switch(prior_ctx);
    }
}
