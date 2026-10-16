//! Planner-owned, `copyObject`-safe Iceberg table scan descriptors.

use lagodb_core::handles::{ExecutorHandle, RelationGuard};
use lagodb_core::plan_data::{PlanDataError, PlanDataReader, PlanDataWriter};
use pgrx::pg_sys;

use crate::foreign_table::{
    ForeignSchemaBinding, ForeignTableIdentity, ForeignTransaction, IcebergFdwError,
    IcebergForeignTable, PlanSourceIdentity,
};
use crate::managed_table::ManagedTableReadView;
use crate::scan::IcebergReadSnapshot;
use crate::scan::columnar::BoundScan as BoundQueryScan;
use crate::scan::parallel::TaskGroupingConfig;
use crate::schema::relation::RelationLayout;

use super::error::Error;
use super::projection::PlanProjection;
use super::worker::ReopenPlan;

const PLAN_MANAGED: i32 = 1;
const PLAN_FOREIGN: i32 = 2;
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ManagedScanPlan {
    relation_oid: pg_sys::Oid,
    projection: PlanProjection,
}

impl ManagedScanPlan {
    fn bind(&self, executor: &ExecutorHandle<'_>) -> Result<BoundScan, Error> {
        let view = ManagedTableReadView::for_snapshot(
            self.relation_oid,
            &executor.snapshot(),
        )?;
        let task_grouping = TaskGroupingConfig::from_properties(view.properties());
        let snapshot = view.into_read_snapshot()?;
        let scan = match &self.projection {
            PlanProjection::CountRows => PlanProjection::bind_count(snapshot)?,
            PlanProjection::Columns(attnos) => {
                let relation = RelationGuard::open_table(
                    self.relation_oid,
                    pg_sys::NoLock as pg_sys::LOCKMODE,
                )?;
                let layout = RelationLayout::from_relation(&relation.as_handle())?;
                PlanProjection::bind_columns(attnos, snapshot, &layout)?
            }
        };
        Ok(BoundScan {
            scan: BoundQueryScan::new(scan, task_grouping),
            worker: ReopenPlan::Managed {
                relation_oid: self.relation_oid,
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct ForeignScanPlan {
    relation_oid: pg_sys::Oid,
    check_as_user_id: pg_sys::Oid,
    identity: ForeignTableIdentity,
    source: Option<PlanSourceIdentity>,
    projection: PlanProjection,
}

impl ForeignScanPlan {
    fn effective_user_oid(&self) -> pg_sys::Oid {
        if self.check_as_user_id == pg_sys::InvalidOid {
            // SAFETY: scan binding runs in a PostgreSQL backend with an
            // established effective user.
            unsafe { pg_sys::GetUserId() }
        } else {
            self.check_as_user_id
        }
    }

    fn bind(&self, executor: &ExecutorHandle<'_>) -> Result<BoundScan, Error> {
        let effective_user_oid = self.effective_user_oid();
        let resolved = IcebergForeignTable::resolve_for_execution(
            self.relation_oid,
            effective_user_oid,
            executor,
        )?;
        if resolved.identity() != &self.identity {
            return Err(IcebergFdwError::PlanIdentityChanged.into());
        }
        let view = ForeignTransaction::scan_view(
            resolved,
            executor.snapshot().command_id(),
        )?;
        let generation = PlanSourceIdentity::from_table(&view.table);
        if self
            .source
            .as_ref()
            .is_some_and(|planned| planned != &generation)
        {
            return Err(IcebergFdwError::PlanSourceChanged.into());
        }
        let task_grouping =
            TaskGroupingConfig::from_properties(view.table.metadata().properties());
        let relation = RelationGuard::open_table(
            self.relation_oid,
            pg_sys::NoLock as pg_sys::LOCKMODE,
        )?;
        let binding = ForeignSchemaBinding::new(&relation.as_handle())?;
        let columns = match &self.projection {
            PlanProjection::CountRows => &[][..],
            PlanProjection::Columns(attnos) => attnos.as_ref(),
        };
        binding.validate_read(
            view.table.metadata(),
            &self.identity,
            columns.iter().copied(),
        )?;
        let layout = binding.into_relation_layout();
        let snapshot = IcebergReadSnapshot::new(view.table, view.delta);
        let scan = self.projection.bind_with_layout(snapshot, &layout)?;
        Ok(BoundScan {
            scan: BoundQueryScan::new(scan, task_grouping),
            worker: ReopenPlan::Foreign {
                relation_oid: self.relation_oid,
                effective_user_oid,
                identity: self.identity.clone(),
                generation,
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum Plan {
    Managed(ManagedScanPlan),
    Foreign(ForeignScanPlan),
}

impl Plan {
    pub(super) fn managed(
        relation_oid: pg_sys::Oid,
        projection: PlanProjection,
    ) -> Self {
        Self::Managed(ManagedScanPlan {
            relation_oid,
            projection,
        })
    }

    pub(super) fn foreign(
        relation_oid: pg_sys::Oid,
        check_as_user_id: pg_sys::Oid,
        identity: ForeignTableIdentity,
        source: Option<PlanSourceIdentity>,
        projection: PlanProjection,
    ) -> Self {
        Self::Foreign(ForeignScanPlan {
            relation_oid,
            check_as_user_id,
            identity,
            source,
            projection,
        })
    }

    pub(super) fn encode(&self, writer: &mut PlanDataWriter) {
        match self {
            Self::Managed(plan) => {
                writer
                    .append_i32(PLAN_MANAGED)
                    .append_oid(plan.relation_oid);
                plan.projection.encode(writer);
            }
            Self::Foreign(plan) => {
                writer
                    .append_i32(PLAN_FOREIGN)
                    .append_oid(plan.relation_oid)
                    .append_oid(plan.check_as_user_id);
                plan.identity.encode(writer);
                writer.append_bool(plan.source.is_some());
                if let Some(source) = &plan.source {
                    source.encode(writer);
                }
                plan.projection.encode(writer);
            }
        }
    }

    pub(super) fn decode(reader: &mut PlanDataReader<'_>) -> Result<Self, Error> {
        match reader.read_i32()? {
            PLAN_MANAGED => Ok(Self::managed(
                reader.read_oid()?,
                PlanProjection::decode(reader)?,
            )),
            PLAN_FOREIGN => Ok(Self::foreign(
                reader.read_oid()?,
                reader.read_oid()?,
                ForeignTableIdentity::decode(reader)?,
                reader
                    .read_bool()?
                    .then(|| PlanSourceIdentity::decode(reader))
                    .transpose()?,
                PlanProjection::decode(reader)?,
            )),
            found => Err(PlanError::UnknownPlan { found }.into()),
        }
    }

    pub(super) fn bind(
        &self,
        executor: &ExecutorHandle<'_>,
    ) -> Result<BoundScan, Error> {
        match self {
            Self::Managed(plan) => plan.bind(executor),
            Self::Foreign(plan) => plan.bind(executor),
        }
    }
}

pub(super) struct BoundScan {
    pub(super) scan: BoundQueryScan,
    pub(super) worker: ReopenPlan,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum PlanError {
    #[error("Iceberg scan plan-data primitive failed: {0}")]
    PlanData(#[from] PlanDataError),
    #[error("Iceberg scan plan kind {found} is unsupported")]
    UnknownPlan { found: i32 },
    #[error("Iceberg scan projection kind {found} is unsupported")]
    UnknownProjection { found: i32 },
    #[error("Iceberg scan column projection is empty")]
    EmptyProjection,
    #[error("Iceberg scan column projection contains an invalid attribute")]
    InvalidAttribute,
}
