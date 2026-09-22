//! Planner-owned, `copyObject`-safe Iceberg table scan descriptors.

use lagodb_core::handles::RelationGuard;
use lagodb_core::plan_data::{PlanDataError, PlanDataReader, PlanDataWriter};
use pgrx::pg_sys;

use crate::foreign_table::{
    ForeignSchemaBinding, ForeignTableIdentity, ForeignTransaction, IcebergFdwError,
    PlanSourceIdentity, RestForeignTable,
};
use crate::managed_table::ManagedTableSnapshot;
use crate::scan::parallel::TaskGroupingConfig;
use crate::scan::query::BoundScan as BoundQueryScan;
use crate::scan::{
    CountRowsRead, IcebergReadSnapshot, PreparedIcebergRead, QuerySourceBinding,
    ScanPredicates,
};
use crate::schema::relation::RelationShape;

use super::error::Error;
use super::worker::ReopenPlan;

const PLAN_MANAGED: i32 = 1;
const PLAN_FOREIGN: i32 = 2;
const PROJECTION_COUNT_ROWS: i32 = 1;
const PROJECTION_COLUMNS: i32 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PlanProjection {
    CountRows,
    Columns(Box<[pg_sys::AttrNumber]>),
}

impl PlanProjection {
    const fn plan_kind(&self) -> i32 {
        match self {
            Self::CountRows => PROJECTION_COUNT_ROWS,
            Self::Columns(_) => PROJECTION_COLUMNS,
        }
    }

    fn encode(&self, writer: &mut PlanDataWriter) {
        writer.append_i32(self.plan_kind());
        if let Self::Columns(attnos) = self {
            writer.append_count(attnos.len());
            for attno in attnos {
                writer.append_i32(i32::from(*attno));
            }
        }
    }

    fn decode(reader: &mut PlanDataReader<'_>) -> Result<Self, PlanError> {
        match reader.read_i32()? {
            PROJECTION_COUNT_ROWS => Ok(Self::CountRows),
            PROJECTION_COLUMNS => {
                let count = reader.read_count()?;
                if count == 0 {
                    return Err(PlanError::EmptyProjection);
                }
                let mut attnos = Vec::with_capacity(count);
                for _ in 0..count {
                    let attno = pg_sys::AttrNumber::try_from(reader.read_i32()?)
                        .map_err(|_| PlanError::InvalidAttribute)?;
                    if attno <= 0 {
                        return Err(PlanError::InvalidAttribute);
                    }
                    attnos.push(attno);
                }
                Ok(Self::Columns(attnos.into_boxed_slice()))
            }
            found => Err(PlanError::UnknownProjection { found }),
        }
    }

    fn bind_count(
        snapshot: IcebergReadSnapshot,
    ) -> Result<QuerySourceBinding, Error> {
        let read: CountRowsRead = PreparedIcebergRead::count_rows(snapshot);
        Ok(read.bind_query_source()?)
    }

    fn bind_columns(
        attnos: &[pg_sys::AttrNumber],
        snapshot: IcebergReadSnapshot,
        shape: &RelationShape,
    ) -> Result<QuerySourceBinding, Error> {
        let mut attnos = attnos.to_vec();
        attnos.sort_unstable();
        let project_field_ids =
            shape.project_field_ids(snapshot.schema(), attnos.into_iter())?;
        Ok(PreparedIcebergRead::projected_fields(
            snapshot,
            project_field_ids,
            ScanPredicates::unfiltered(),
        )
        .bind_query_source()?)
    }

    fn bind_with_shape(
        &self,
        snapshot: IcebergReadSnapshot,
        shape: &RelationShape,
    ) -> Result<QuerySourceBinding, Error> {
        match self {
            Self::CountRows => Self::bind_count(snapshot),
            Self::Columns(attnos) => Self::bind_columns(attnos, snapshot, shape),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct ManagedScanPlan {
    relation_oid: pg_sys::Oid,
    tablespace_oid: pg_sys::Oid,
    projection: PlanProjection,
}

impl ManagedScanPlan {
    fn bind(&self) -> Result<BoundScan, Error> {
        let loaded =
            ManagedTableSnapshot::load_query(self.relation_oid, self.tablespace_oid)?;
        let task_grouping = TaskGroupingConfig::from_properties(loaded.properties());
        let snapshot = loaded.into_read_snapshot();
        let scan = match &self.projection {
            PlanProjection::CountRows => PlanProjection::bind_count(snapshot)?,
            PlanProjection::Columns(attnos) => {
                let relation = RelationGuard::open(
                    self.relation_oid,
                    pg_sys::NoLock as pg_sys::LOCKMODE,
                )?;
                let shape = RelationShape::from_relation(&relation.as_handle())?;
                PlanProjection::bind_columns(attnos, snapshot, &shape)?
            }
        };
        Ok(BoundScan {
            scan: BoundQueryScan::new(scan, task_grouping),
            worker: ReopenPlan::Managed {
                tablespace_oid: self.tablespace_oid,
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct ForeignScanPlan {
    relation_oid: pg_sys::Oid,
    check_as_user_id: pg_sys::Oid,
    identity: ForeignTableIdentity,
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

    fn bind(&self) -> Result<BoundScan, Error> {
        let effective_user_oid = self.effective_user_oid();
        let resolved =
            RestForeignTable::resolve(self.relation_oid, effective_user_oid)?;
        if resolved.identity() != &self.identity {
            return Err(IcebergFdwError::PlanIdentityChanged.into());
        }
        let view = ForeignTransaction::scan_view(resolved)?;
        let generation = PlanSourceIdentity::from_table(&view.table);
        let task_grouping =
            TaskGroupingConfig::from_properties(view.table.metadata().properties());
        let relation = RelationGuard::open(
            self.relation_oid,
            pg_sys::NoLock as pg_sys::LOCKMODE,
        )?;
        let shape = ForeignSchemaBinding::bind(
            &relation.as_handle(),
            view.table.metadata().current_schema(),
        )?
        .into_relation_shape();
        let snapshot = IcebergReadSnapshot::new(view.table, view.delta);
        let scan = self.projection.bind_with_shape(snapshot, &shape)?;
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
        tablespace_oid: pg_sys::Oid,
        projection: PlanProjection,
    ) -> Self {
        Self::Managed(ManagedScanPlan {
            relation_oid,
            tablespace_oid,
            projection,
        })
    }

    pub(super) fn foreign(
        relation_oid: pg_sys::Oid,
        check_as_user_id: pg_sys::Oid,
        identity: ForeignTableIdentity,
        projection: PlanProjection,
    ) -> Self {
        Self::Foreign(ForeignScanPlan {
            relation_oid,
            check_as_user_id,
            identity,
            projection,
        })
    }

    pub(super) fn encode(&self, writer: &mut PlanDataWriter) {
        match self {
            Self::Managed(plan) => {
                writer
                    .append_i32(PLAN_MANAGED)
                    .append_oid(plan.relation_oid)
                    .append_oid(plan.tablespace_oid);
                plan.projection.encode(writer);
            }
            Self::Foreign(plan) => {
                writer
                    .append_i32(PLAN_FOREIGN)
                    .append_oid(plan.relation_oid)
                    .append_oid(plan.check_as_user_id);
                plan.identity.encode(writer);
                plan.projection.encode(writer);
            }
        }
    }

    pub(super) fn decode(reader: &mut PlanDataReader<'_>) -> Result<Self, Error> {
        match reader.read_i32()? {
            PLAN_MANAGED => Ok(Self::managed(
                reader.read_oid()?,
                reader.read_oid()?,
                PlanProjection::decode(reader)?,
            )),
            PLAN_FOREIGN => Ok(Self::foreign(
                reader.read_oid()?,
                reader.read_oid()?,
                ForeignTableIdentity::decode(reader)?,
                PlanProjection::decode(reader)?,
            )),
            found => Err(PlanError::UnknownPlan { found }.into()),
        }
    }

    pub(super) fn bind(&self) -> Result<BoundScan, Error> {
        match self {
            Self::Managed(plan) => plan.bind(),
            Self::Foreign(plan) => plan.bind(),
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
