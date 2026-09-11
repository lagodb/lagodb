//! Planner-owned, `copyObject`-safe managed-Iceberg scan descriptor.

use lagodb_core::handles::RelationGuard;
use lagodb_core::plan_data::{PlanDataError, PlanDataReader, PlanDataWriter};
use pgrx::pg_sys;

use crate::engine::scan::ScanSpec;
use crate::engine::scan::projection::{ProjectedField, Projection};
use crate::engine::schema::relation::RelationShape;
use crate::error::IcebergError;
use crate::managed_table::access::scan::LoadedScanMetadata;

use super::{BoundIcebergTableScan, IcebergTableScanError};

const PROJECTION_COUNT_ROWS: i32 = 1;
const PROJECTION_COLUMNS: i32 = 2;

/// Iceberg scan projection currently consumed by the query engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IcebergScanProjection {
    CountRows,
    Columns(Box<[pg_sys::AttrNumber]>),
}

impl IcebergScanProjection {
    const fn plan_kind(&self) -> i32 {
        match self {
            Self::CountRows => PROJECTION_COUNT_ROWS,
            Self::Columns(_) => PROJECTION_COLUMNS,
        }
    }

    fn from_plan_kind(kind: i32) -> Result<Self, IcebergScanPlanError> {
        match kind {
            PROJECTION_COUNT_ROWS => Ok(Self::CountRows),
            PROJECTION_COLUMNS => Ok(Self::Columns(Box::new([]))),
            found => Err(IcebergScanPlanError::UnknownProjection { found }),
        }
    }
}

/// Planner-owned descriptor for one managed Iceberg table scan.
///
/// It contains only copyable relation identity and projection semantics.
/// The provider's validated scan estimate is carried beside this opaque
/// payload in the selected plan. Active snapshots, tasks, readers, and
/// backend-local resources are acquired only by [`Self::bind`] during
/// executor Begin.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IcebergScanPlan {
    relation_oid: pg_sys::Oid,
    tablespace_oid: pg_sys::Oid,
    projection: IcebergScanProjection,
}

impl IcebergScanPlan {
    pub(crate) fn scalar_count(
        relation_oid: pg_sys::Oid,
        tablespace_oid: pg_sys::Oid,
    ) -> Self {
        Self {
            relation_oid,
            tablespace_oid,
            projection: IcebergScanProjection::CountRows,
        }
    }

    pub(crate) fn columns(
        relation_oid: pg_sys::Oid,
        tablespace_oid: pg_sys::Oid,
        attnos: &[pg_sys::AttrNumber],
    ) -> Self {
        Self {
            relation_oid,
            tablespace_oid,
            projection: IcebergScanProjection::Columns(attnos.into()),
        }
    }

    /// Append this provider-owned frame to a containing query plan.
    pub(crate) fn encode(&self, writer: &mut PlanDataWriter) {
        writer
            .append_oid(self.relation_oid)
            .append_oid(self.tablespace_oid)
            .append_i32(self.projection.plan_kind());
        if let IcebergScanProjection::Columns(attnos) = &self.projection {
            writer.append_count(attnos.len());
            for attno in attnos {
                writer.append_i32(i32::from(*attno));
            }
        }
    }

    /// Decode this provider-owned frame from its containing query plan.
    pub(crate) fn decode(
        reader: &mut PlanDataReader<'_>,
    ) -> Result<Self, IcebergScanPlanError> {
        let relation_oid = reader.read_oid()?;
        let tablespace_oid = reader.read_oid()?;
        let mut projection =
            IcebergScanProjection::from_plan_kind(reader.read_i32()?)?;
        if matches!(projection, IcebergScanProjection::Columns(_)) {
            let count = reader.read_count()?;
            if count == 0 {
                return Err(IcebergScanPlanError::EmptyProjection);
            }
            let mut attnos = Vec::with_capacity(count);
            for _ in 0..count {
                let attno = pg_sys::AttrNumber::try_from(reader.read_i32()?)
                    .map_err(|_| IcebergScanPlanError::InvalidAttribute)?;
                if attno <= 0 {
                    return Err(IcebergScanPlanError::InvalidAttribute);
                }
                attnos.push(attno);
            }
            projection = IcebergScanProjection::Columns(attnos.into_boxed_slice());
        }
        Ok(Self {
            relation_oid,
            tablespace_oid,
            projection,
        })
    }

    /// Capture the current statement view and projected Arrow schema without
    /// traversing manifests or planning physical file tasks.
    pub(super) fn bind(
        &self,
    ) -> Result<BoundIcebergTableScan, IcebergTableScanError> {
        let source =
            LoadedScanMetadata::load_query(self.relation_oid, self.tablespace_oid)?
                .into_source();
        let scan = match &self.projection {
            IcebergScanProjection::CountRows => {
                return Ok(BoundIcebergTableScan::new(
                    ScanSpec::count_rows(source).bind()?,
                ));
            }
            IcebergScanProjection::Columns(attnos) => {
                let relation = RelationGuard::open(
                    self.relation_oid,
                    pg_sys::NoLock as pg_sys::LOCKMODE,
                )?;
                let shape = RelationShape::from_relation(&relation.as_handle())?;
                let projection = Projection::new(
                    attnos
                        .iter()
                        .enumerate()
                        .map(|(destination, &attno)| {
                            ProjectedField::new(attno, destination)
                        })
                        .collect(),
                );
                let attr_types = attnos
                    .iter()
                    .map(|attno| {
                        shape
                            .attr_types()
                            .get(*attno as usize - 1)
                            .copied()
                            .ok_or(IcebergError::InvariantViolated(
                                "Iceberg query projection attno exceeds relation width",
                            ))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                ScanSpec::projected(
                    source,
                    projection,
                    None,
                    None,
                    &shape,
                    &attr_types,
                )?
                .bind_query_source()?
            }
        };
        Ok(BoundIcebergTableScan::new(scan))
    }
}

/// Invalid or incompatible provider plan data.
#[derive(Debug, thiserror::Error)]
pub(crate) enum IcebergScanPlanError {
    #[error("Iceberg scan plan-data primitive failed: {0}")]
    PlanData(#[from] PlanDataError),
    #[error("Iceberg scan projection kind {found} is unsupported")]
    UnknownProjection { found: i32 },
    #[error("Iceberg scan column projection is empty")]
    EmptyProjection,
    #[error("Iceberg scan column projection contains an invalid attribute")]
    InvalidAttribute,
}
