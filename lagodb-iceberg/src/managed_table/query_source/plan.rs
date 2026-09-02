//! Planner-owned, `copyObject`-safe managed-Iceberg source descriptor.

use lagodb_core::plan_data::{PlanDataError, PlanDataReader, PlanDataWriter};
use lagodb_core::query_contract::SourceId;
use pgrx::pg_sys;

use crate::engine::scan::ScanSpec;
use crate::error::IcebergResult;
use crate::managed_table::access::scan::LoadedScanMetadata;

use super::PreparedIcebergSource;

const ICEBERG_SOURCE_PLAN_KIND: i32 = 0x4c49_0001;
const ICEBERG_SOURCE_PLAN_VERSION: i32 = 1;
const PROJECTION_COUNT_ROWS: i32 = 1;

/// S1M's only executable Iceberg source projection.
///
/// The variant is explicit in plan data so a future field projection cannot be
/// decoded accidentally by an older zero-column reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IcebergSourceProjection {
    CountRows,
}

impl IcebergSourceProjection {
    const fn plan_kind(self) -> i32 {
        match self {
            Self::CountRows => PROJECTION_COUNT_ROWS,
        }
    }

    fn from_plan_kind(kind: i32) -> Result<Self, IcebergSourcePlanError> {
        match kind {
            PROJECTION_COUNT_ROWS => Ok(Self::CountRows),
            found => Err(IcebergSourcePlanError::UnknownProjection { found }),
        }
    }
}

/// Planner-owned descriptor for one managed Iceberg source instance.
///
/// It contains only copyable provider identities and projection semantics.
/// The provider's validated source estimate is carried beside this opaque
/// payload in the engine envelope. Active snapshots, tasks, readers, and
/// backend-local resources are acquired only by [`Self::prepare`] during
/// executor Begin.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IcebergSourcePlan {
    source: SourceId,
    relation_oid: pg_sys::Oid,
    tablespace_oid: pg_sys::Oid,
    projection: IcebergSourceProjection,
}

impl IcebergSourcePlan {
    pub(crate) fn scalar_count(
        source: SourceId,
        relation_oid: pg_sys::Oid,
        tablespace_oid: pg_sys::Oid,
    ) -> Result<Self, IcebergSourcePlanError> {
        if relation_oid == pg_sys::InvalidOid {
            return Err(IcebergSourcePlanError::InvalidRelationOid);
        }
        Ok(Self {
            source,
            relation_oid,
            tablespace_oid,
            projection: IcebergSourceProjection::CountRows,
        })
    }

    /// Append this provider-owned frame to a containing query plan.
    pub(crate) fn encode(&self, writer: &mut PlanDataWriter) {
        writer
            .append_i32(ICEBERG_SOURCE_PLAN_KIND)
            .append_i32(ICEBERG_SOURCE_PLAN_VERSION)
            .append_count(self.source.index())
            .append_oid(self.relation_oid)
            .append_oid(self.tablespace_oid)
            .append_i32(self.projection.plan_kind());
    }

    /// Decode a provider frame after the containing query plan established its
    /// source-table length.
    pub(crate) fn decode(
        reader: &mut PlanDataReader<'_>,
        expected_source: SourceId,
    ) -> Result<Self, IcebergSourcePlanError> {
        let kind = reader.read_i32()?;
        if kind != ICEBERG_SOURCE_PLAN_KIND {
            return Err(IcebergSourcePlanError::WrongKind {
                found: kind,
                expected: ICEBERG_SOURCE_PLAN_KIND,
            });
        }
        let version = reader.read_i32()?;
        if version != ICEBERG_SOURCE_PLAN_VERSION {
            return Err(IcebergSourcePlanError::WrongVersion {
                found: version,
                expected: ICEBERG_SOURCE_PLAN_VERSION,
            });
        }
        let source_index = reader.read_count()?;
        let source = SourceId::from_index(source_index);
        if source != expected_source {
            return Err(IcebergSourcePlanError::UnexpectedSource {
                expected: expected_source.index(),
                found: source_index,
            });
        }
        let relation_oid = reader.read_oid()?;
        let tablespace_oid = reader.read_oid()?;
        let projection = IcebergSourceProjection::from_plan_kind(reader.read_i32()?)?;
        let plan = Self {
            source,
            relation_oid,
            tablespace_oid,
            projection,
        };
        if plan.relation_oid == pg_sys::InvalidOid {
            return Err(IcebergSourcePlanError::InvalidRelationOid);
        }
        Ok(plan)
    }

    /// Capture the current statement view and plan its complete file-task
    /// inventory. This is the sole method in this type that performs catalog or
    /// storage I/O and must therefore be called only from non-EXPLAIN executor
    /// Begin.
    pub(crate) fn prepare(&self) -> IcebergResult<PreparedIcebergSource> {
        let source =
            LoadedScanMetadata::load_query(self.relation_oid, self.tablespace_oid)?
                .into_source();
        let scan = match self.projection {
            IcebergSourceProjection::CountRows => ScanSpec::count_rows(source),
        };
        Ok(PreparedIcebergSource::new(scan.prepare()?))
    }
}

/// Invalid or incompatible provider plan data.
#[derive(Debug, thiserror::Error)]
pub(crate) enum IcebergSourcePlanError {
    #[error("Iceberg source plan-data primitive failed: {0}")]
    PlanData(#[from] PlanDataError),
    #[error("Iceberg source plan has kind {found}, expected {expected}")]
    WrongKind { found: i32, expected: i32 },
    #[error(
        "Iceberg source plan version {found} is unsupported; expected {expected}"
    )]
    WrongVersion { found: i32, expected: i32 },
    #[error("Iceberg source projection kind {found} is unsupported")]
    UnknownProjection { found: i32 },
    #[error(
        "Iceberg source identity {found} does not match expected identity {expected}"
    )]
    UnexpectedSource { expected: usize, found: usize },
    #[error("Iceberg source relation OID is InvalidOid")]
    InvalidRelationOid,
}
