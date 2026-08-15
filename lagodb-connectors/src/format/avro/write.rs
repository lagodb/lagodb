//! Avro OCF write orchestration.

mod encoder;
mod plan;

use pg_lakebase_core::fdw::{
    ForeignModifyOutcome, ModifyPlanSlot, ModifySlot,
};
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::tuple::SlotDatumIndex;

use crate::error::ConnectorError;
use crate::format::{
    AvroWriteCompression, EmptyOutputPolicy, FormatKind, FormatWriteState,
    ObjectSetWriter,
};
use crate::storage::ObjectOutput;

pub(super) use encoder::AvroDatumRow;
use encoder::AvroEncoderFactory;
pub(super) use plan::{AvroValueKind, AvroWritePlan};

pub(super) struct AvroObjectWriter {
    writer: Option<ObjectSetWriter<AvroEncoderFactory>>,
}

impl AvroObjectWriter {
    pub(super) fn new(
        output: ObjectOutput,
        plan: AvroWritePlan,
        compression: AvroWriteCompression,
    ) -> Self {
        Self {
            writer: Some(ObjectSetWriter::new(
                output,
                AvroEncoderFactory::new(plan, compression),
            )),
        }
    }

    pub(super) fn write_row(
        &mut self,
        row: &AvroDatumRow,
    ) -> Result<(), ConnectorError> {
        self.writer
            .as_mut()
            .expect("the Avro writer is not used after finish")
            .write(row)
    }

    pub(super) fn finish(
        &mut self,
        emit_empty: bool,
    ) -> Result<(), ConnectorError> {
        let Some(writer) = self.writer.take() else {
            return Ok(());
        };
        writer.finish(if emit_empty {
            EmptyOutputPolicy::EmitFile
        } else {
            EmptyOutputPolicy::Skip
        })
    }
}

pub(super) struct AvroWriteState {
    sources: Box<[SlotDatumIndex]>,
    row: AvroDatumRow,
    writer: AvroObjectWriter,
}

impl AvroWriteState {
    pub(super) fn begin(
        relation: &RelationHandle<'_>,
        output: ObjectOutput,
        compression: AvroWriteCompression,
    ) -> Result<Self, ConnectorError> {
        let columns = relation.live_columns();
        let plan = AvroWritePlan::from_relation_columns(&columns)?;
        let sources = columns
            .iter()
            .map(|column| {
                SlotDatumIndex::new(
                    (column.attno() - 1) as usize,
                    relation.natts(),
                )
                .expect("a live relation attribute is within its tuple width")
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self {
            row: AvroDatumRow::new(sources.len()),
            sources,
            writer: AvroObjectWriter::new(output, plan, compression),
        })
    }
}

impl FormatWriteState for AvroWriteState {
    fn insert(
        &mut self,
        slot: &mut ModifySlot<'_>,
    ) -> Result<ForeignModifyOutcome, ConnectorError> {
        let datums = slot.tuple_row().datums();
        for (output, source) in self.sources.iter().copied().enumerate() {
            // SAFETY: every source token was validated against this relation's
            // tuple width during Begin, and the executor supplies that same
            // relation layout for the synchronous INSERT callback.
            let (datum, is_null) = unsafe { datums.datum_at_bound(source) };
            // SAFETY: output enumerates the row allocated from sources.len(); a
            // present slot Datum stays live through the immediate writer call.
            unsafe {
                self.row
                    .set_at_bound(output, (!is_null).then_some(datum))
            };
        }
        self.writer.write_row(&self.row)?;
        Ok(ForeignModifyOutcome::Applied)
    }

    fn update(
        &mut self,
        _slot: &mut ModifySlot<'_>,
        _plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ConnectorError> {
        Err(ConnectorError::modify_not_implemented(FormatKind::Avro))
    }

    fn delete(
        &mut self,
        _returned_slot: Option<&mut ModifySlot<'_>>,
        _plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ConnectorError> {
        Err(ConnectorError::modify_not_implemented(FormatKind::Avro))
    }

    fn finish(&mut self) -> Result<(), ConnectorError> {
        self.writer.finish(false)
    }
}
