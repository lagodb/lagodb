//! Planner-owned, `copyObject`-safe managed-Iceberg scan descriptor.

use lagodb_core::handles::RelationGuard;
use lagodb_core::plan_data::{PlanDataError, PlanDataReader, PlanDataWriter};
use lagodb_core::query_contract::ScanId;
use pgrx::pg_sys;

use crate::engine::predicate::BoundIcebergPredicate;
use crate::engine::scan::ScanSpec;
use crate::engine::scan::projection::{ProjectedField, Projection};
use crate::engine::schema::relation::RelationShape;
use crate::error::IcebergError;
use crate::managed_table::access::scan::LoadedScanMetadata;

use super::{IcebergTableScanError, PreparedIcebergTableScan};

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
/// It contains only copyable provider identities and projection semantics.
/// The provider's validated scan estimate is carried beside this opaque
/// payload in the selected plan. Active snapshots, tasks, readers, and
/// backend-local resources are acquired only by [`Self::prepare`] during
/// executor Begin.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IcebergScanPlan {
    scan: ScanId,
    relation_oid: pg_sys::Oid,
    tablespace_oid: pg_sys::Oid,
    projection: IcebergScanProjection,
}

impl IcebergScanPlan {
    pub(crate) fn scalar_count(
        scan: ScanId,
        relation_oid: pg_sys::Oid,
        tablespace_oid: pg_sys::Oid,
    ) -> Self {
        Self {
            scan,
            relation_oid,
            tablespace_oid,
            projection: IcebergScanProjection::CountRows,
        }
    }

    pub(crate) fn columns(
        scan: ScanId,
        relation_oid: pg_sys::Oid,
        tablespace_oid: pg_sys::Oid,
        attnos: &[pg_sys::AttrNumber],
    ) -> Self {
        Self {
            scan,
            relation_oid,
            tablespace_oid,
            projection: IcebergScanProjection::Columns(attnos.into()),
        }
    }

    /// Append this provider-owned frame to a containing query plan.
    pub(crate) fn encode(&self, writer: &mut PlanDataWriter) {
        writer
            .append_count(self.scan.index())
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

    /// Decode a provider frame after the containing query plan established its
    /// scan-table length.
    pub(crate) fn decode(
        reader: &mut PlanDataReader<'_>,
        expected_scan: ScanId,
    ) -> Result<Self, IcebergScanPlanError> {
        let scan_index = reader.read_count()?;
        let scan = ScanId::from_index(scan_index);
        if scan != expected_scan {
            return Err(IcebergScanPlanError::UnexpectedScan {
                expected: expected_scan.index(),
                found: scan_index,
            });
        }
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
            scan,
            relation_oid,
            tablespace_oid,
            projection,
        })
    }

    /// Capture the current statement view and plan its complete file-task
    /// inventory. This is the sole method in this type that performs catalog or
    /// storage I/O and must therefore be called only from non-EXPLAIN executor
    /// Begin.
    pub(super) fn prepare(
        &self,
        predicate: Option<&BoundIcebergPredicate>,
    ) -> Result<PreparedIcebergTableScan, IcebergTableScanError> {
        let source =
            LoadedScanMetadata::load_query(self.relation_oid, self.tablespace_oid)?
                .into_source();
        BoundIcebergPredicate::validate_schema(
            predicate,
            source.schema().schema_id(),
        )?;
        let predicate = BoundIcebergPredicate::conjoin(predicate);
        let scan = match &self.projection {
            IcebergScanProjection::CountRows => {
                let mut scan = ScanSpec::count_rows(source);
                // The query table-scan ABI supplies this expression only for
                // conservative provider pruning. DataFusion owns the exact
                // residual, so the Iceberg reader must not evaluate it again.
                scan.set_predicates(predicate, None);
                return Ok(PreparedIcebergTableScan::new(scan.prepare()?));
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
                    predicate,
                    None,
                    &shape,
                    &attr_types,
                )?
                .prepare_query_source()?
            }
        };
        Ok(PreparedIcebergTableScan::new(scan))
    }
}

/// Invalid or incompatible provider plan data.
#[derive(Debug, thiserror::Error)]
pub(crate) enum IcebergScanPlanError {
    #[error("Iceberg scan plan-data primitive failed: {0}")]
    PlanData(#[from] PlanDataError),
    #[error("Iceberg scan projection kind {found} is unsupported")]
    UnknownProjection { found: i32 },
    #[error(
        "Iceberg scan identity {found} does not match expected identity {expected}"
    )]
    UnexpectedScan { expected: usize, found: usize },
    #[error("Iceberg scan column projection is empty")]
    EmptyProjection,
    #[error("Iceberg scan column projection contains an invalid attribute")]
    InvalidAttribute,
}
