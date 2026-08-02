//! Optional FDW INSERT/UPDATE/DELETE capability.

mod begin;
mod callbacks;
mod contract;
mod delete;
mod error;
mod execution_context;
mod executor;
mod planning;
mod planning_context;
mod private;
mod return_layout;
mod return_requirements;
mod row_layout;
mod slot;
mod slot_buffer;
mod state;

pub use super::row_identity::{
    ForeignRowIdentity, ForeignRowIdentityKind, ModifyPlanSlot,
};
pub use contract::{
    FdwModify, ForeignModifyCapabilities, ForeignModifyOperation,
    ForeignModifyOutcome, ForeignModifyPlanSpec, ForeignModifyPrivate,
    ForeignModifyState, ForeignReturnedIdentity,
};
pub use error::{ForeignModifyError, ForeignModifyPhase};
pub use execution_context::{ForeignInsertBeginContext, ForeignModifyBeginContext};
pub use planning_context::{
    ForeignModifyPlanContext, ForeignModifyRelationContext,
    ForeignUpdateTargetContext,
};
pub use slot::ModifySlot;

pub(crate) use callbacks::{
    begin_foreign_insert, begin_foreign_modify, end_foreign_insert,
    end_foreign_modify, exec_foreign_insert, exec_foreign_update,
};
pub(crate) use delete::exec_foreign_delete;
pub(crate) use planning::{
    add_foreign_update_targets, is_foreign_rel_updatable, plan_foreign_modify,
};
