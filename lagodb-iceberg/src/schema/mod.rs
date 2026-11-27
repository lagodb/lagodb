//! Shared PostgreSQL/Iceberg schema binding and conversion plans.

mod iceberg;
mod projection;
mod read_plan;
mod relation;
mod write_plan;

pub(crate) use iceberg::IcebergTypeExt;
pub(crate) use projection::{ProjectedAttribute, SlotProjection, StorageProjection};
pub(crate) use read_plan::PgReadPlan;
pub(crate) use relation::{LiveColumn, RelationLayout};
pub(crate) use write_plan::PgWritePlan;
