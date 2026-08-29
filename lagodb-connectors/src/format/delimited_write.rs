//! PG-native Text/CSV Foreign Table INSERT encoding.

use lagodb_core::copy::CopyRowEncoder;
use lagodb_core::fdw::{ForeignModifyOutcome, ModifyPlanSlot, ModifySlot};
use lagodb_core::handles::RelationHandle;
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::storage::ObjectOutput;

use super::delimited::DelimitedFormat;
use super::{
    EmptyOutputPolicy, FormatKind, FormatWriteState, ObjectSetWriter,
    StreamCompression, StreamEncoderFactory,
};

/// Prefix-object writer for one Text or CSV foreign-table INSERT statement.
pub(super) struct DelimitedWriteState {
    encoder: CopyRowEncoder,
    writer: Option<ObjectSetWriter<StreamEncoderFactory>>,
    format: FormatKind,
}

impl DelimitedWriteState {
    pub(super) fn begin(
        relation: &RelationHandle<'_>,
        output: ObjectOutput,
        format: DelimitedFormat,
        compression: StreamCompression,
        postgres_options: *mut pg_sys::List,
        write_header: bool,
    ) -> Result<Self, ConnectorError> {
        // SAFETY: the FDW begin context keeps the relation live until its
        // matching end callback. The selected format supplies a text/CSV
        // option list allocated in this PostgreSQL execution context.
        let mut encoder =
            unsafe { CopyRowEncoder::begin(relation.as_raw(), postgres_options) }?;
        let mut factory = StreamEncoderFactory::new(format.stream(), compression);
        if write_header {
            factory.set_header(encoder.header()?.into());
        }
        Ok(Self {
            encoder,
            writer: Some(ObjectSetWriter::new(output, factory)),
            format: format.kind(),
        })
    }
}

impl FormatWriteState for DelimitedWriteState {
    fn insert(
        &mut self,
        slot: &mut ModifySlot<'_>,
    ) -> Result<ForeignModifyOutcome, ConnectorError> {
        // SAFETY: ModifySlot exposes the live relation-shaped executor slot
        // for this synchronous callback; CopyRowEncoder was bound to the same
        // foreign relation in begin.
        let row = unsafe { self.encoder.row(slot.as_raw()) }?;
        self.writer
            .as_mut()
            .expect("delimited writer is not used after finish")
            .write(row)?;
        Ok(ForeignModifyOutcome::Applied)
    }

    fn update(
        &mut self,
        _slot: &mut ModifySlot<'_>,
        _plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ConnectorError> {
        Err(ConnectorError::modify_not_implemented(self.format))
    }

    fn delete(
        &mut self,
        _returned_slot: Option<&mut ModifySlot<'_>>,
        _plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ConnectorError> {
        Err(ConnectorError::modify_not_implemented(self.format))
    }

    fn finish(&mut self) -> Result<(), ConnectorError> {
        let Some(writer) = self.writer.take() else {
            return self.encoder.finish().map_err(ConnectorError::from);
        };
        writer.finish(EmptyOutputPolicy::Skip)?;
        self.encoder.finish().map_err(ConnectorError::from)
    }
}
