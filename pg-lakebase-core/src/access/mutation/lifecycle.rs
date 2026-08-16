//! COPY FROM utility lifecycle.
//!
//! ModifyTable execution owns its mutation state inside the outer CustomScan. COPY
//! FROM bypasses ModifyTable, so it alone needs utility-hook boundaries.

use std::sync::Once;

use pgrx::pg_sys;

use crate::hooks::{
    CopyStmtNode, PostUtilityContext, UtilityHook, UtilityHookError, UtilityNode,
    register_utility_hook,
};

use super::session;

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
            session::begin_copy_from_frame();
        }
        Ok(())
    }

    fn on_post(&self, context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        let Some(copy) = context.original_stmt().cast::<CopyStmtNode>() else {
            return Ok(());
        };
        if copy.is_from {
            session::finish_current_copy_frame().map_err(UtilityHookError::from)?;
        }
        Ok(())
    }
}

pub fn init_lifecycle_hooks() {
    MUTATION_LIFECYCLE_INIT.call_once(|| {
        register_utility_hook(
            pg_sys::NodeTag::T_CopyStmt,
            Box::new(CopyFromLifecycle),
        );
    });
}
