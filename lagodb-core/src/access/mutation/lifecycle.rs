//! Provider-local command lifecycle for FDWs that request the original COPY CID.
//! AM writers are owned by COPY drivers and do not use these utility hooks.

use std::sync::Once;

use pgrx::pg_sys;

use crate::hooks::{
    CopyStmtNode, PostUtilityContext, UtilityHook, UtilityHookError, UtilityNode,
    register_utility_hook,
};

use super::command;

static MUTATION_LIFECYCLE_INIT: Once = Once::new();

struct CopyFromLifecycle;

impl UtilityHook for CopyFromLifecycle {
    fn name(&self) -> &'static str {
        "mutation_copy_from_lifecycle"
    }

    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let Some(copy) = context.cast::<CopyStmtNode>() else {
            return Ok(());
        };
        if copy.is_from {
            command::begin_copy_from_frame();
        }
        Ok(())
    }

    fn on_post(&self, context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        let Some(copy) = context.original_stmt().cast::<CopyStmtNode>() else {
            return Ok(());
        };
        if copy.is_from {
            command::finish_current_copy_frame().map_err(UtilityHookError::from)?;
        }
        Ok(())
    }
}

/// Register this extension's COPY command lifecycle for FDW providers
/// that request a COPY command ID. Call during extension initialization,
/// before publishing hooks. The lifecycle and its consumers must belong to the
/// same extension; providers that do not request a CID need not register it.
pub fn init_lifecycle_hooks() {
    MUTATION_LIFECYCLE_INIT.call_once(|| {
        register_utility_hook(
            pg_sys::NodeTag::T_CopyStmt,
            Box::new(CopyFromLifecycle),
        );
    });
}
