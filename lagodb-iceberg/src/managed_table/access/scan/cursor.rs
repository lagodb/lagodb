//! Slot-first query and mutation cursors.

use arrow_array::{Int64Array, RecordBatch};
use pg_arrow_conv::{ArrowColumnDecoder, BoundBatch};
use pg_lakebase_core::access::mutation::ModifyScanBinding;
use pg_lakebase_core::prelude::*;
use pgrx::pg_sys;

use crate::engine::scan::IcebergQueryCursor;
use crate::engine::scan::batch::{
    IcebergArrowBatchSource, RowLocationLayout, position_unchecked,
};
use crate::engine::write::IcebergFileId;
use crate::managed_table::access::mutation::{
    IcebergFileSource, IcebergModifyQueryState,
};

/// Query and mutation scans have different valid bound-batch states.
///
/// Keeping them as enum variants prevents query batches from carrying
/// row-location state and prevents mutation batches from existing without a
/// registered file identity.
pub struct IcebergBatchCursor {
    kind: CursorKind,
}

enum CursorKind {
    Query(IcebergQueryCursor),
    Mutation(MutationBatchCursor),
}

impl IcebergBatchCursor {
    pub(super) fn query(cursor: IcebergQueryCursor) -> Self {
        Self {
            kind: CursorKind::Query(cursor),
        }
    }

    pub(super) fn mutation(
        source: IcebergArrowBatchSource,
        decoder: ArrowColumnDecoder,
        binding: ModifyScanBinding<IcebergModifyQueryState>,
        table_oid: pg_sys::Oid,
    ) -> Self {
        Self {
            kind: CursorKind::Mutation(MutationBatchCursor {
                source,
                decoder,
                current: None,
                row_location_layout: None,
                row_index: 0,
                context: ModifyCursorContext {
                    binding,
                    table_oid,
                    last_file: None,
                },
            }),
        }
    }
}

impl ScanBatchDriver for IcebergBatchCursor {
    fn next_into_slot(
        &mut self,
        direction: ScanDirection,
        out: &mut SlotColumns<'_>,
    ) -> AmResult<bool> {
        if direction != ScanDirection::Forward {
            return unsupported_callback("non-forward Iceberg scan");
        }
        match &mut self.kind {
            CursorKind::Query(cursor) => cursor.next_into_slot(out),
            CursorKind::Mutation(cursor) => cursor.next_into_slot(out),
        }
    }
}

struct MutationBoundBatch {
    decoded: BoundBatch,
    positions: Int64Array,
    file_id: IcebergFileId,
}

struct MutationBatchCursor {
    source: IcebergArrowBatchSource,
    decoder: ArrowColumnDecoder,
    current: Option<MutationBoundBatch>,
    row_location_layout: Option<RowLocationLayout>,
    row_index: usize,
    context: ModifyCursorContext,
}

impl MutationBatchCursor {
    fn bind_batch(
        &mut self,
        batch: RecordBatch,
    ) -> AmResult<Option<MutationBoundBatch>> {
        let layout = match self.row_location_layout {
            Some(layout) => layout,
            None => {
                let layout = RowLocationLayout::try_new(&batch)?;
                self.row_location_layout = Some(layout);
                layout
            }
        };
        // SAFETY: this is the Iceberg reader projection used to create the
        // stable layout; `bind` handles empty batches without reading `_file`.
        let locations = unsafe { layout.bind(&batch) }?;
        let Some(locations) = locations else {
            // Preserve the old empty-batch schema validation without creating
            // a row-bearing mutation state.
            let _ = self.decoder.bind(batch)?;
            return Ok(None);
        };
        let file_id = self.context.register_file(locations.file_path())?;
        let positions = locations.into_positions();
        let decoded = self.decoder.bind(batch)?;
        Ok(Some(MutationBoundBatch {
            decoded,
            positions,
            file_id,
        }))
    }

    /// Emit one modification row and encode its Iceberg row identity into the
    /// PostgreSQL `ctid` carried by the plan.
    fn next_into_slot(&mut self, out: &mut SlotColumns<'_>) -> AmResult<bool> {
        loop {
            if let Some(bound) = self.current.as_ref()
                && self.row_index < self.decoder.num_rows(&bound.decoded)
            {
                let row_index = self.row_index;
                // SAFETY: ScanColumns compiled the decoder from the relation
                // layout used by this cursor and validated every destination
                // against the same slot width.
                unsafe {
                    self.decoder
                        .write_row_unchecked(&bound.decoded, row_index, out)
                }?;
                // SAFETY: the position array belongs to the same bound batch,
                // and the decoder row count is that RecordBatch's row count.
                let position =
                    unsafe { position_unchecked(&bound.positions, row_index) };
                let tid = IcebergModifyQueryState::encode_row_identity(
                    bound.file_id,
                    &position,
                )?;
                out.set_tid(&tid);
                out.set_table_oid(self.context.table_oid);
                self.row_index += 1;
                return Ok(true);
            }

            self.current = None;
            let Some(batch) = self.source.next_batch()? else {
                return Ok(false);
            };
            let Some(bound) = self.bind_batch(batch)? else {
                continue;
            };
            self.current = Some(bound);
            self.row_index = 0;
        }
    }
}

struct ModifyCursorContext {
    binding: ModifyScanBinding<IcebergModifyQueryState>,
    table_oid: pg_sys::Oid,
    /// Adjacent batches from one `FileReadRequest` reuse the registry result
    /// without hashing or allocating the path again.
    last_file: Option<(Box<str>, IcebergFileId)>,
}

impl ModifyCursorContext {
    fn register_file(&mut self, path: &str) -> AmResult<IcebergFileId> {
        if let Some((cached_path, file_id)) = self.last_file.as_ref()
            && cached_path.as_ref() == path
        {
            return Ok(*file_id);
        }

        // RelationRowRegistry remains the sole file-path interner and ID
        // authority. Its transaction/relation-scoped mapping guarantees that
        // every registration of the same path returns the same file ID.
        let source = IcebergFileSource::new(path);
        let file_id = self.binding.register_identity_source(&source)?;
        self.last_file = Some((path.into(), file_id));
        Ok(file_id)
    }
}
