use std::sync::Arc;

use crate::IcebergTableAm;
use crate::access::column_mapping::{RelationShape, ScanColumns};
use crate::access::row_location::PhysicalRowTarget;
use crate::catalog::metadata_tracker::TxMetadata;
use crate::error::{IcebergError, IcebergResult};
use crate::storage::StorageContext;
use arrow_array::RecordBatch;
use iceberg_lite::arrow::ArrowReaderBuilder;
use iceberg_lite::spec::SchemaRef;
use pg_arrow_conv::ArrowColumnDecoder;
use pg_lakebase_core::diag::report_warning;
use pg_lakebase_core::prelude::*;
use pgrx::pg_sys;

/// One physical Iceberg row in Arrow's native one-row batch representation.
struct ArrowPhysicalRow {
    schema: SchemaRef,
    batch: RecordBatch,
}

impl ArrowPhysicalRow {
    fn decode_into(self, rel: &RelationHandle, row: &mut Row) -> AmResult<()> {
        let shape = RelationShape::from_relation(rel);
        let plan = ScanColumns::new(self.schema, &shape)?;
        let decoder = ArrowColumnDecoder::new(plan.decoded_columns());
        row.ensure_len(rel.natts());
        decoder.read_owned_row_into(self.batch, 0, row)
    }
}

impl IcebergTableAm {
    fn fetch_physical_row(
        rel: &RelationHandle,
        target: PhysicalRowTarget,
    ) -> IcebergResult<Option<ArrowPhysicalRow>> {
        let ctx = StorageContext::for_tablespace(rel.tablespace_oid())?;
        let schema = Arc::clone(target.schema());

        let projected_field_ids: Vec<i32> = schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.id)
            .collect();
        let request = target.into_read_request(projected_field_ids)?;
        let Some(batch) = ArrowReaderBuilder::new(ctx.file_io().clone())
            .build()
            .read_physical_row(request)?
        else {
            return Ok(None);
        };

        Ok(Some(ArrowPhysicalRow { schema, batch }))
    }
}

impl AmRelation for IcebergTableAm {
    fn relation_estimate_size(
        rel: &RelationHandle,
        _attr_widths: Option<&mut AttrWidthsHandle>,
    ) -> AmResult<(pg_sys::BlockNumber, f64, f64)> {
        let stats = RelationStats::load_or_default(rel);

        Ok((stats.pages(), stats.rows as f64, 0.0))
    }

    fn relation_size(
        rel: &RelationHandle,
        fork_number: pg_sys::ForkNumber::Type,
    ) -> AmResult<u64> {
        if fork_number != pg_sys::ForkNumber::MAIN_FORKNUM {
            return Ok(0);
        }

        Ok(RelationStats::load_or_default(rel).bytes)
    }

    fn tuple_fetch_row_version(
        rel: &RelationHandle,
        tid: &ItemPointer,
        snapshot: &SnapshotHandle,
        row: &mut Row,
    ) -> AmResult<bool> {
        let Some(target) = PhysicalRowTarget::lookup_current(rel.oid(), tid)? else {
            return Ok(false);
        };
        if !snapshot.is_any() {
            return Err(IcebergError::NotImplemented(
                "Iceberg synthetic ctid fetch currently requires SnapshotAny",
            )
            .into());
        }

        let Some(fetched) = Self::fetch_physical_row(rel, target)? else {
            return Ok(false);
        };
        fetched.decode_into(rel, row)?;
        Ok(true)
    }
}

#[derive(Default)]
struct RelationStats {
    rows: u64,
    bytes: u64,
}

impl RelationStats {
    /// Load Iceberg snapshot statistics for the planner.
    ///
    /// The planner calls this on the read path and must never fail because
    /// statistics are unavailable: missing or unreadable metadata should
    /// degrade gracefully into "no information" rather than abort the query.
    /// Failures are surfaced as a warning so operators can still notice them.
    fn load_or_default(rel: &RelationHandle) -> Self {
        match Self::try_load(rel) {
            Ok(stats) => stats,
            Err(err) => {
                report_warning(&format!(
                    "pg_iceberg_am: failed to load Iceberg statistics for relation {}: {err}; planner will use default estimates",
                    rel.oid(),
                ));
                Self::default()
            }
        }
    }

    fn try_load(rel: &RelationHandle) -> IcebergResult<Self> {
        let ctx = StorageContext::for_tablespace(rel.tablespace_oid())?;
        let loaded =
            TxMetadata::current().current_table_metadata(rel.oid(), ctx.file_io())?;
        let (rows, bytes) = loaded.relation_stats(ctx.file_io())?;

        Ok(Self { rows, bytes })
    }

    fn pages(&self) -> pg_sys::BlockNumber {
        let page_size = pg_sys::BLCKSZ as u64;
        let pages = self.bytes.div_ceil(page_size);
        pages.min(u32::MAX as u64) as pg_sys::BlockNumber
    }
}
