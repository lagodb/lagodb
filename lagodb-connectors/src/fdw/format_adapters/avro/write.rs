//! FDW insert state over the shared Avro encoder.

use lagodb_core::handles::RelationHandle;
use lagodb_core::tuple::{SlotDatumIndex, TupleSlotRow};

use crate::error::ConnectorError;
use crate::fdw::format_adapters::FormatWriteState;
use crate::format::AvroWriteCompression;
use crate::format::avro::{AvroDatumRow, AvroObjectWriter, AvroWritePlan};
use crate::storage::StorageOutput;

pub(super) struct AvroWriteState {
    sources: Box<[SlotDatumIndex]>,
    row: AvroDatumRow,
    writer: AvroObjectWriter,
}

impl AvroWriteState {
    pub(super) fn begin(
        relation: &RelationHandle<'_>,
        output: StorageOutput,
        compression: AvroWriteCompression,
    ) -> Result<Self, ConnectorError> {
        let columns = relation.live_columns();
        let plan = AvroWritePlan::from_relation_columns(&columns)?;
        let sources = columns
            .iter()
            .map(|column| {
                SlotDatumIndex::new((column.attno() - 1) as usize, relation.natts())
                    .expect("a live relation attribute is within its tuple width")
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self {
            row: AvroDatumRow::new(sources.len()),
            sources,
            writer: AvroObjectWriter::new(output, plan, compression)?,
        })
    }
}

impl FormatWriteState for AvroWriteState {
    fn write_row(&mut self, row: TupleSlotRow<'_>) -> Result<(), ConnectorError> {
        let datums = row.datums();
        for (output, source) in self.sources.iter().copied().enumerate() {
            // SAFETY: every source token was validated against this relation's
            // tuple width during Begin, and the executor supplies that same
            // relation layout for the synchronous INSERT callback.
            let (datum, is_null) = unsafe { datums.datum_at_bound(source) };
            // SAFETY: output enumerates the row allocated from sources.len(); a
            // present slot Datum stays live through the immediate writer call.
            unsafe { self.row.set_at_bound(output, (!is_null).then_some(datum)) };
        }
        self.writer.write_row(&self.row)?;
        Ok(())
    }

    fn finish(&mut self) -> Result<(), ConnectorError> {
        self.writer.finish(false)
    }
}
