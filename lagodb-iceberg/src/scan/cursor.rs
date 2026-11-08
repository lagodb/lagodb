//! PostgreSQL row-materialization cursor shared by scan adapters.

use arrow_array::RecordBatch;
use lagodb_arrow::{ArrowColumnDecoder, BoundBatch};
use lagodb_core::tuple::SlotDatumWriter;

use super::batch::ArrowBatches;
use crate::error::IcebergResult;

pub(crate) struct PgRowCursor<I = ArrowBatches> {
    source: I,
    decoder: ArrowColumnDecoder,
    current: Option<BoundBatch>,
    row_index: usize,
}

impl<I> PgRowCursor<I>
where
    I: Iterator<Item = IcebergResult<RecordBatch>>,
{
    /// # Safety
    /// Every batch from `source` must satisfy `decoder`'s complete projected
    /// schema, as required by `ArrowColumnDecoder::bind_batch_unchecked`.
    pub(crate) unsafe fn new(source: I, decoder: ArrowColumnDecoder) -> Self {
        Self {
            source,
            decoder,
            current: None,
            row_index: 0,
        }
    }

    /// Decode the next row of the forward-only source into `out`, or return
    /// `false` at end of input without writing `out`. Table AM adapters handle
    /// PostgreSQL direction requests before calling this cursor.
    ///
    /// # Safety
    /// `out` must write the slot layout this cursor's decoder was bound to:
    /// the slot width and attribute types its `PgReadPlan` validated every
    /// destination against. The PostgreSQL memory context that owns the row's
    /// pass-by-reference datums must be current.
    pub(crate) unsafe fn next_into_slot(
        &mut self,
        out: &mut SlotDatumWriter<'_>,
    ) -> IcebergResult<bool> {
        loop {
            if let Some(bound) = self.current.as_ref()
                && self.row_index < bound.num_rows()
            {
                // SAFETY: `bound` was produced by this decoder and `row_index`
                // is within it. The caller guarantees the slot layout and the
                // current memory context.
                unsafe {
                    self.decoder.write_row_unchecked(bound, self.row_index, out)
                }?;
                self.row_index += 1;
                return Ok(true);
            }

            self.current = None;
            let Some(batch) = self.source.next().transpose()? else {
                return Ok(false);
            };
            // SAFETY: the constructor pairs this source with its decoder.
            // Iceberg performs file-to-scan schema conversion before emission.
            self.current = Some(unsafe { self.decoder.bind_batch_unchecked(batch) });
            self.row_index = 0;
        }
    }
}
