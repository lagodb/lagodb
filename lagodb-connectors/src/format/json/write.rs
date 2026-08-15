//! Native NDJSON Foreign Table INSERT writer.

use pg_lakebase_core::fdw::{ForeignModifyOutcome, ModifyPlanSlot, ModifySlot};
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::tuple::{BoundJsonObjectEncoder, SlotDatumIndex};

use crate::error::ConnectorError;
use crate::storage::ObjectOutput;

use super::record::JsonColumnPlan;
use crate::format::{
    EmptyOutputPolicy, FormatKind, FormatWriteState, ObjectSetWriter,
    StreamCompression, StreamEncoderFactory,
};

pub(super) struct JsonWriteState {
    sources: Box<[SlotDatumIndex]>,
    encoder: BoundJsonObjectEncoder,
    writer: Option<ObjectSetWriter<StreamEncoderFactory>>,
}

impl JsonWriteState {
    pub(super) fn begin(
        relation: &RelationHandle<'_>,
        output: ObjectOutput,
        compression: StreamCompression,
    ) -> Result<Self, ConnectorError> {
        let columns = relation.live_columns();
        let fields = columns
            .iter()
            .map(|column| {
                let name = column.name().to_str().map_err(|_| {
                    ConnectorError::invalid_object_schema(
                        FormatKind::Json,
                        "PostgreSQL column names must be valid UTF-8 for JSON",
                    )
                })?;
                Ok((name, column.type_oid(), column.type_mod()))
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        let plan = JsonColumnPlan::bind(fields)?;
        let encoder = BoundJsonObjectEncoder::bind(
            plan.columns()
                .iter()
                .map(|column| (column.name(), column.output_encoder())),
        )
        .map_err(ConnectorError::json_datum)?;
        let sources = columns
            .iter()
            .map(|column| {
                SlotDatumIndex::new((column.attno() - 1) as usize, relation.natts())
                    .expect("a live relation attribute is within its tuple width")
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self {
            sources,
            encoder,
            writer: Some(ObjectSetWriter::new(
                output,
                StreamEncoderFactory::new(FormatKind::Json, compression),
            )),
        })
    }
}

impl FormatWriteState for JsonWriteState {
    fn insert(
        &mut self,
        slot: &mut ModifySlot<'_>,
    ) -> Result<ForeignModifyOutcome, ConnectorError> {
        let datums = slot.tuple_row().datums();
        let values = self.sources.iter().copied().map(|source| {
            // SAFETY: every source token was validated against this relation's
            // tuple width during Begin, and the callback supplies that same
            // relation layout for this synchronous INSERT.
            let (datum, is_null) = unsafe { datums.datum_at_bound(source) };
            (!is_null).then_some(datum)
        });
        // SAFETY: sources and output columns were built from the same live
        // relation column sequence, and slot Datums remain live for this call.
        let row = unsafe { self.encoder.encode_row(values) }
            .map_err(ConnectorError::json_datum)?;
        self.writer
            .as_mut()
            .expect("JSON writer is not used after finish")
            .write(row)?;
        Ok(ForeignModifyOutcome::Applied)
    }

    fn update(
        &mut self,
        _slot: &mut ModifySlot<'_>,
        _plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ConnectorError> {
        Err(ConnectorError::modify_not_implemented(FormatKind::Json))
    }

    fn delete(
        &mut self,
        _returned_slot: Option<&mut ModifySlot<'_>>,
        _plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ConnectorError> {
        Err(ConnectorError::modify_not_implemented(FormatKind::Json))
    }

    fn finish(&mut self) -> Result<(), ConnectorError> {
        let Some(writer) = self.writer.take() else {
            return Ok(());
        };
        writer.finish(EmptyOutputPolicy::Skip)
    }
}
