//! Shared standard COPY FROM routing and unclaimed URI rejection.

use std::ffi::CStr;

use lagodb_core::copy::{
    CopyCompletion, CopyContext, CopyEndpoint, CopyError, CopyTargetProbe,
    RoutedCopyFromDriver,
};

use lagodb_core::diag::PgReportError;
use lagodb_core::hooks::{HookError, UtilityHookPhase};
use pgrx::{PgSqlErrorCode, pg_sys};

use super::ProcessUtilityArgs;

/// Build the fail-closed error for a URI-form COPY filename after every
/// registered provider declined it.
///
/// # Safety
///
/// `node` must be the live `T_CopyStmt` node for the current ProcessUtility
/// invocation.
pub(super) unsafe fn unclaimed_uri_error(
    node: *mut pg_sys::Node,
) -> Option<PgReportError> {
    // SAFETY: the caller guarantees the node tag and lifetime.
    let statement = unsafe { &*node.cast::<pg_sys::CopyStmt>() };
    if statement.is_program || statement.filename.is_null() {
        return None;
    }
    // SAFETY: PostgreSQL COPY parse nodes store `filename` as a live
    // NUL-terminated string for the utility statement lifetime.
    let filename = unsafe { CStr::from_ptr(statement.filename) };
    if CopyEndpoint::from_filename(Some(filename), statement.is_program)
        != CopyEndpoint::ExternalUri
    {
        return None;
    }
    Some(PgReportError::from_parts(
        PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
        "no LagoDB provider claimed the COPY URI",
        None,
        Some(
            "configure the required provider in lagodb.provider_libraries and restart PostgreSQL"
                .to_owned(),
        ),
    ))
}

/// Shared PostgreSQL-format FROM fallback, after format consumers declined.
/// Return true only when this router consumed the COPY statement.
///
/// # Safety
/// Arguments must describe the live COPY ProcessUtility invocation.
pub(super) unsafe fn try_route(args: ProcessUtilityArgs) -> Result<bool, HookError> {
    // SAFETY: the caller supplies the live COPY invocation.
    unsafe { route_from(args) }.map_err(|error| {
        HookError::from_copy_error(error).with_utility_context(
            "CopyFromRouter",
            UtilityHookPhase::Consume,
            pg_sys::NodeTag::T_CopyStmt,
        )
    })
}

/// Admission deliberately retains the existing unlocked-probe DDL window.
///
/// # Safety
/// Arguments must describe the live COPY ProcessUtility invocation.
unsafe fn route_from(args: ProcessUtilityArgs) -> Result<bool, CopyError> {
    let context = unsafe { args.copy_context() };
    if !context.statement().is_from()
        || !CopyTargetProbe::new(context.statement()).requires_write_bridge()?
    {
        return Ok(false);
    }
    // Preparation transforms expressions, so honor ProcessUtility's writable
    // tree contract only after the read-only admission accepted the command.
    let mut context = unsafe { args.writable_copy().copy_context() };
    let parse = context.parse_state();
    let preparation = context.prepare_from(&parse)?;
    preparation.require_write_bridge()?;
    let processed = unsafe {
        RoutedCopyFromDriver::begin(context.statement(), &parse, preparation)?
    }
    .execute()?;
    parse.dispose()?;
    context.complete(CopyCompletion::new(processed));
    Ok(true)
}

impl ProcessUtilityArgs {
    /// # Safety
    /// These arguments must describe a live COPY ProcessUtility invocation,
    /// and the returned context must not outlive that invocation.
    unsafe fn copy_context<'a>(self) -> CopyContext<'a> {
        // SAFETY: the caller guarantees a live CopyStmt invocation.
        unsafe {
            CopyContext::from_raw(
                self.pstmt,
                self.query_string,
                self.read_only_tree,
                self.context,
                self.params,
                self.query_env,
                self.dest,
                self.completion_tag,
            )
        }
    }
}
