//! Planner-owned, `copyObject`-safe Parquet table scan descriptors.

use lagodb_core::handles::RelationGuard;
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};
use pgrx::pg_sys;

use crate::fdw::{PlannedFormat, ResolvedForeignRelation};
use crate::format::FormatKind;

use super::columns::ScanColumns;
use super::error::Error;
use super::scan::BoundScan;

const PROJECTION_COUNT_ROWS: i32 = 1;
const PROJECTION_COLUMNS: i32 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PlanProjection {
    CountRows,
    Columns(Box<[pg_sys::AttrNumber]>),
}

impl PlanProjection {
    pub(super) fn attnos(&self) -> &[pg_sys::AttrNumber] {
        match self {
            Self::CountRows => &[],
            Self::Columns(attnos) => attnos,
        }
    }

    fn encode(&self, writer: &mut PlanDataWriter) {
        match self {
            Self::CountRows => {
                writer.append_i32(PROJECTION_COUNT_ROWS);
            }
            Self::Columns(attnos) => {
                writer
                    .append_i32(PROJECTION_COLUMNS)
                    .append_count(attnos.len());
                for attno in attnos {
                    writer.append_i32(i32::from(*attno));
                }
            }
        }
    }

    fn decode(reader: &mut PlanDataReader<'_>) -> Result<Self, Error> {
        match reader.read_i32()? {
            PROJECTION_COUNT_ROWS => Ok(Self::CountRows),
            PROJECTION_COLUMNS => {
                // `encode` widened user attribute numbers.
                let count = reader.read_count()?;
                let attnos = (0..count)
                    .map(|_| Ok(reader.read_i32()? as pg_sys::AttrNumber))
                    .collect::<Result<Box<[_]>, Error>>()?;
                Ok(Self::Columns(attnos))
            }
            found => Err(Error::UnknownProjection { found }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Plan {
    relation_oid: pg_sys::Oid,
    check_as_user_id: pg_sys::Oid,
    projection: PlanProjection,
}

impl Plan {
    pub(super) const fn new(
        relation_oid: pg_sys::Oid,
        check_as_user_id: pg_sys::Oid,
        projection: PlanProjection,
    ) -> Self {
        Self {
            relation_oid,
            check_as_user_id,
            projection,
        }
    }

    pub(super) fn encode(&self, writer: &mut PlanDataWriter) {
        writer
            .append_oid(self.relation_oid)
            .append_oid(self.check_as_user_id);
        self.projection.encode(writer);
    }

    pub(super) fn decode(reader: &mut PlanDataReader<'_>) -> Result<Self, Error> {
        Ok(Self::new(
            reader.read_oid()?,
            reader.read_oid()?,
            PlanProjection::decode(reader)?,
        ))
    }

    fn effective_user_oid(&self) -> pg_sys::Oid {
        if self.check_as_user_id == pg_sys::InvalidOid {
            // SAFETY: scan binding runs in a PostgreSQL backend with an
            // established effective user.
            unsafe { pg_sys::GetUserId() }
        } else {
            self.check_as_user_id
        }
    }

    /// Resolve the location and declared columns for this execution; the
    /// object listing waits for task planning.
    pub(super) fn bind(&self) -> Result<BoundScan, Error> {
        // The executor already holds the range-table lock.
        let relation = RelationGuard::open_table(
            self.relation_oid,
            pg_sys::NoLock as pg_sys::LOCKMODE,
        )?;
        let resolved = ResolvedForeignRelation::resolve(self.relation_oid)?;
        resolved.ensure_planned(PlannedFormat::new(FormatKind::Parquet))?;
        let effective_user_oid = self.effective_user_oid();
        let location = resolved.into_read_location(effective_user_oid)?;
        let columns =
            ScanColumns::load(&relation.as_handle(), self.projection.attnos())?
                .ok_or(Error::Invariant(
                    "a planned column lost its columnar representation",
                ))?;
        Ok(BoundScan::new(
            self.relation_oid,
            effective_user_oid,
            location,
            columns,
        ))
    }
}
