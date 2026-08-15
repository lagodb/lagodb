//! Native NDJSON Foreign Table INSERT writer.

use pg_lakebase_core::fdw::{ForeignModifyOutcome, ModifyPlanSlot, ModifySlot};
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::tuple::JsonRowEncoder;

use crate::error::ConnectorError;
use crate::storage::ObjectOutput;

use super::record::JsonColumnPlan;
use crate::format::{
    EmptyOutputPolicy, FormatKind, FormatWriteState, ObjectSetWriter,
    StreamCompression, StreamEncoderFactory,
};

pub(super) struct JsonWriteState {
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
        // Bind and validate the complete relation once. row_to_json itself
        // writes the object, so the plan has no row-path role after this point.
        let _ = JsonColumnPlan::bind(fields)?;
        Ok(Self {
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
        // SAFETY: ModifySlot exposes this live relation-shaped slot only for
        // the current synchronous callback.
        let row = unsafe { JsonRowEncoder::encode(slot.as_raw()) }
            .map_err(ConnectorError::json_datum)?;
        let bytes = row.as_bytes().map_err(ConnectorError::json_datum)?;
        self.writer
            .as_mut()
            .expect("JSON writer is not used after finish")
            .write(bytes)?;
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
