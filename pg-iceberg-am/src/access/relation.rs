use crate::IcebergTableAm;
use crate::access::column_mapping::ScanColumns;
use crate::access::row_location::lookup_current;
use crate::access::scan::RelationShape;
use crate::catalog::bridge::IcebergTableId;
use crate::catalog::metadata_tracker::TxMetadata;
use crate::error::{IcebergError, IcebergResult};
use crate::storage::StorageContext;
use iceberg_lite::table::Table;
use pg_arrow_conv::ArrowColumnDecoder;
use pg_lakebase_core::diag::report_warning;
use pg_lakebase_core::prelude::*;
use pgrx::pg_sys;

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
        _snapshot: &SnapshotHandle,
        row: &mut Row,
    ) -> AmResult<bool> {
        let Some(location) = lookup_current(rel.oid(), tid)? else {
            return Ok(false);
        };
        let Some(snapshot_id) = location.starting_snapshot_id else {
            return Ok(false);
        };

        let ctx = StorageContext::for_tablespace(rel.tablespace_oid())?;
        let loaded =
            TxMetadata::current().current_table_metadata(rel.oid(), ctx.file_io())?;
        let schema = loaded.metadata.current_schema().clone();
        let delta = loaded.delta.clone();
        let table = Table::builder()
            .file_io(ctx.file_io().clone())
            .metadata_location(loaded.location)
            .metadata(loaded.metadata)
            .identifier(IcebergTableId::for_relation(rel.oid()).into_table_ident())
            .build()
            .map_err(IcebergError::from)?;

        let projected_field_ids: Vec<i32> = schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.id)
            .collect();
        let mut scan_builder = table.scan().snapshot_id(snapshot_id).select_empty();
        if let Some(delta) = delta {
            scan_builder = scan_builder.with_delta(delta);
        }
        let Some(batch) = scan_builder
            .build()
            .map_err(IcebergError::from)?
            .fetch_row_by_position(
                &location.data_file_path,
                location.position,
                &projected_field_ids,
            )
            .map_err(IcebergError::from)?
        else {
            return Ok(false);
        };

        let shape = RelationShape::from_relation(rel);
        let plan = ScanColumns::new(
            schema,
            shape.live_columns(),
            shape.slot_width(),
            shape.attr_types(),
        )?;
        let decoder =
            ArrowColumnDecoder::new(plan.decoded_columns(shape.attr_types()));
        let mut fetched = decoder.read_owned_row(batch, 0)?;
        fetched.ensure_len(rel.natts());
        row.replace_with(fetched);
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
