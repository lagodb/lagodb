//! Transaction-local Iceberg mutation state shared by catalog adapters.
//!
//! This layer owns action ordering, savepoint rollback/promotion, overlays,
//! and provider-independent transaction materialization. It does not load an
//! authoritative metadata location and does not publish catalog changes.

mod action_log;
mod properties;
mod table_state;

#[cfg(test)]
pub(crate) use action_log::EffectiveCommitAction;
pub(crate) use action_log::{
    ExclusiveTransactionAction, TxTableActionLog, TxTableCommitPlan,
};
pub(crate) use properties::PreparedTablePropertyUpdate;
pub(crate) use table_state::TableTransactionState;
