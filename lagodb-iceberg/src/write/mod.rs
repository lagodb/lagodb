//! Shared PostgreSQL/Iceberg write data plane.
//!
//! This module owns format-level row identity, relation-local row ownership,
//! mutation scan tasks, and Arrow write plans. Managed tables and writable FDW
//! adapters decide where these objects live and how their catalog commits run.

mod data_file_sink;
mod isolation;
mod mutation_sinks;
mod registry;
mod row_delete;
mod row_identity;
mod scan_tasks;
mod transaction;

pub(crate) use data_file_sink::DataFileSink;
pub(crate) use isolation::PgTransactionIsolation;
pub(crate) use mutation_sinks::MutationSinks;
pub(crate) use registry::{
    IcebergFileId, ModifyStateId, OwnedRowPositions, RelationRowRegistry,
    RowMutationClaim,
};
pub(crate) use row_delete::{RowDeleteClaim, RowDeleteOutput, RowDeleteState};
pub(crate) use row_identity::IcebergRowIdentity;
pub(crate) use scan_tasks::PlannedMutationTasks;
#[cfg(test)]
pub(crate) use transaction::EffectiveCommitAction;
pub(crate) use transaction::{
    ExclusiveTransactionAction, PreparedTablePropertyUpdate, TableTransactionState,
    TxTableActionLog, TxTableCommitPlan,
};
