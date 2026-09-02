//! AM catalog and executor adaptation for the shared scan engine.

use std::sync::Arc;

use iceberg_lite::expr::Predicate;
use iceberg_lite::overlay::SnapshotDelta;
use iceberg_lite::spec::Schema as IcebergSchema;
use iceberg_lite::table::Table;
use lagodb_core::access::mutation::ModifyScanBinding;
use lagodb_core::prelude::OwnedScanKeys;
use pgrx::pg_sys;

use super::cursor::IcebergBatchCursor;
use crate::engine::scan::projection::Projection;
use crate::engine::scan::{
    AnalyzeScanInput, MutationScanInput, ScanSource, ScanSpec,
};
use crate::engine::schema::relation::RelationShape;
use crate::engine::write::PgTransactionIsolation;
use crate::error::IcebergResult;
use crate::managed_table::access::analyze::AnalyzePreparation;
use crate::managed_table::access::mutation::IcebergModifyQueryState;
use crate::managed_table::catalog::bridge::IcebergTableId;
use crate::managed_table::catalog::metadata_tracker::TxMetadata;
use crate::managed_table::storage::StorageContext;

#[derive(Clone, Copy)]
enum ScanMetadataPurpose {
    Query,
    Analyze,
}

pub(crate) struct LoadedScanMetadata {
    table: Table,
    schema: Arc<IcebergSchema>,
    delta: Option<Arc<SnapshotDelta>>,
    storage_bytes: Option<u64>,
}

impl LoadedScanMetadata {
    pub(crate) fn load_query(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
    ) -> IcebergResult<Self> {
        Self::load(rel_oid, spc_oid, ScanMetadataPurpose::Query)
    }

    pub(crate) fn schema(&self) -> &Arc<IcebergSchema> {
        &self.schema
    }

    fn load(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
        purpose: ScanMetadataPurpose,
    ) -> IcebergResult<Self> {
        PgTransactionIsolation::current()?;
        let ctx = StorageContext::for_tablespace(spc_oid)?;
        let loaded =
            TxMetadata::current().current_table_metadata(rel_oid, ctx.file_io())?;
        let schema = loaded.metadata.current_schema().clone();
        let storage_bytes = match purpose {
            ScanMetadataPurpose::Query => None,
            ScanMetadataPurpose::Analyze => {
                Some(loaded.relation_stats(ctx.file_io())?.1)
            }
        };
        let table = Table::builder()
            .file_io(ctx.file_io().clone())
            .metadata_location(loaded.location)
            .metadata(loaded.metadata)
            .identifier(IcebergTableId::for_relation(rel_oid).into_table_ident())
            .build()?;
        Ok(Self {
            table,
            schema,
            delta: loaded.delta,
            storage_bytes,
        })
    }

    pub(crate) fn into_source(self) -> ScanSource {
        ScanSource::transaction_view(self.table, self.delta, self.storage_bytes)
    }
}

impl ScanSpec {
    pub(crate) fn build(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
        keys: &OwnedScanKeys,
        shape: &RelationShape,
    ) -> IcebergResult<Self> {
        let source = LoadedScanMetadata::load_query(rel_oid, spc_oid)?.into_source();
        let mut spec = Self::full(source, None, None, shape)?;
        spec.refresh_filter(keys)?;
        Ok(spec)
    }

    pub(super) fn build_for_analyze(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
        shape: &RelationShape,
    ) -> IcebergResult<Self> {
        let source =
            LoadedScanMetadata::load(rel_oid, spc_oid, ScanMetadataPurpose::Analyze)?
                .into_source();
        Self::full(source, None, None, shape)
    }

    pub(crate) fn build_for_custom_scan(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
        planning_filter: Option<Predicate>,
        row_filter: Option<Predicate>,
        shape: &RelationShape,
    ) -> IcebergResult<Self> {
        let source = LoadedScanMetadata::load_query(rel_oid, spc_oid)?.into_source();
        Self::full(source, planning_filter, row_filter, shape)
    }

    pub(crate) fn build_with_projection(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
        projection: Projection,
        planning_filter: Option<Predicate>,
        row_filter: Option<Predicate>,
        shape: &RelationShape,
        scan_attr_types: &[(pg_sys::Oid, i32)],
    ) -> IcebergResult<Self> {
        let source = LoadedScanMetadata::load_query(rel_oid, spc_oid)?.into_source();
        Self::projected(
            source,
            projection,
            planning_filter,
            row_filter,
            shape,
            scan_attr_types,
        )
    }

    pub(crate) fn open_batch_cursor(&mut self) -> IcebergResult<IcebergBatchCursor> {
        Ok(IcebergBatchCursor::query(self.open_query_cursor()?))
    }

    pub(crate) fn prepare_analyze(&self) -> IcebergResult<AnalyzePreparation> {
        let AnalyzeScanInput {
            scan,
            tasks,
            decoder,
            storage_bytes,
        } = self.analyze_input()?;
        AnalyzePreparation::try_new(scan, tasks, decoder, storage_bytes)
    }

    pub(crate) fn open_mutation_batch_cursor(
        &mut self,
        binding: ModifyScanBinding<IcebergModifyQueryState>,
        table_oid: pg_sys::Oid,
    ) -> IcebergResult<IcebergBatchCursor> {
        let MutationScanInput { source, decoder } = self.mutation_input()?;
        Ok(IcebergBatchCursor::mutation(
            source, decoder, binding, table_oid,
        ))
    }

    pub(super) fn refresh_filter(
        &mut self,
        keys: &OwnedScanKeys,
    ) -> IcebergResult<()> {
        let filter = scan_keys_to_predicate(keys, self.schema())?;
        self.set_filter(filter);
        Ok(())
    }
}

/// Current TableAM scan keys never arrive through an advertised Iceberg index
/// path, so the executor remains the authority for filtering plain SeqScans.
fn scan_keys_to_predicate(
    _keys: &OwnedScanKeys,
    _schema: &IcebergSchema,
) -> IcebergResult<Option<Predicate>> {
    Ok(None)
}
