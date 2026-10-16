//! Shared planning and execution projection for query-offload scans.

use std::sync::Arc;

use arrow_schema::{Schema as ArrowSchema, SchemaRef};
use iceberg_lite::spec::Schema as IcebergSchema;
use lagodb_core::handles::RelationGuard;
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};
use pgrx::pg_sys;

use crate::scan::{
    CountRowsRead, IcebergReadSnapshot, PreparedIcebergRead, ScanPredicates,
    ScanSourceBinding,
};
use crate::schema::projection::StorageProjection;
use crate::schema::relation::RelationLayout;

use super::error::Error;
use super::plan::PlanError;

const PROJECTION_COUNT_ROWS: i32 = 1;
const PROJECTION_COLUMNS: i32 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PlanProjection {
    CountRows,
    Columns(Box<[pg_sys::AttrNumber]>),
}

impl PlanProjection {
    pub(super) fn planned_schema(
        &self,
        relation_oid: pg_sys::Oid,
        source_schema: impl FnOnce() -> Result<Arc<IcebergSchema>, Error>,
    ) -> Result<SchemaRef, Error> {
        let Self::Columns(attnos) = self else {
            return Ok(Arc::new(ArrowSchema::empty()));
        };
        let schema = source_schema()?;
        let relation = RelationGuard::open_table(
            relation_oid,
            pg_sys::NoLock as pg_sys::LOCKMODE,
        )?;
        let layout = RelationLayout::from_relation(&relation.as_handle())?;
        let field_ids = layout.project_field_ids(&schema, attnos.iter().copied())?;
        Ok(Arc::new(
            StorageProjection::from_field_ids(schema, field_ids).to_arrow_schema()?,
        ))
    }

    const fn plan_kind(&self) -> i32 {
        match self {
            Self::CountRows => PROJECTION_COUNT_ROWS,
            Self::Columns(_) => PROJECTION_COLUMNS,
        }
    }

    pub(super) fn encode(&self, writer: &mut PlanDataWriter) {
        writer.append_i32(self.plan_kind());
        if let Self::Columns(attnos) = self {
            writer.append_count(attnos.len());
            for attno in attnos {
                writer.append_i32(i32::from(*attno));
            }
        }
    }

    pub(super) fn decode(reader: &mut PlanDataReader<'_>) -> Result<Self, PlanError> {
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

    pub(super) fn bind_count(
        snapshot: IcebergReadSnapshot,
    ) -> Result<ScanSourceBinding, Error> {
        let read: CountRowsRead = PreparedIcebergRead::count_rows(snapshot);
        Ok(read.bind_scan_source()?)
    }

    pub(super) fn bind_columns(
        attnos: &[pg_sys::AttrNumber],
        snapshot: IcebergReadSnapshot,
        layout: &RelationLayout,
    ) -> Result<ScanSourceBinding, Error> {
        let project_field_ids =
            layout.project_field_ids(snapshot.schema(), attnos.iter().copied())?;
        Ok(PreparedIcebergRead::projected_fields(
            snapshot,
            project_field_ids,
            ScanPredicates::unfiltered(),
        )
        .bind_scan_source()?)
    }

    pub(super) fn bind_with_layout(
        &self,
        snapshot: IcebergReadSnapshot,
        layout: &RelationLayout,
    ) -> Result<ScanSourceBinding, Error> {
        match self {
            Self::CountRows => Self::bind_count(snapshot),
            Self::Columns(attnos) => Self::bind_columns(attnos, snapshot, layout),
        }
    }
}
