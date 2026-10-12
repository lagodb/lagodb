//! Avro OCF write orchestration.

mod encoder;
mod ocf;
mod plan;

use crate::error::ConnectorError;
use crate::format::{AvroWriteCompression, EmptyOutputPolicy, ObjectSetWriter};
use crate::storage::StorageOutput;

pub(crate) use encoder::AvroDatumRow;
use encoder::AvroEncoderFactory;
pub(super) use plan::AvroValueKind;
pub(crate) use plan::AvroWritePlan;

pub(crate) struct AvroObjectWriter {
    writer: Option<ObjectSetWriter<AvroEncoderFactory>>,
    completed_bytes: u64,
}

impl AvroObjectWriter {
    pub(crate) fn new(
        output: StorageOutput,
        plan: AvroWritePlan,
        compression: AvroWriteCompression,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            writer: Some(ObjectSetWriter::new(
                output,
                AvroEncoderFactory::new(plan, compression),
            )?),
            completed_bytes: 0,
        })
    }

    pub(crate) fn write_row(
        &mut self,
        row: &AvroDatumRow,
    ) -> Result<(), ConnectorError> {
        self.writer
            .as_mut()
            .expect("the Avro writer is not used after finish")
            .write(row)
    }

    pub(crate) fn finish(&mut self, emit_empty: bool) -> Result<(), ConnectorError> {
        let Some(writer) = self.writer.take() else {
            return Ok(());
        };
        self.completed_bytes = writer.finish_with_bytes(if emit_empty {
            EmptyOutputPolicy::EmitFile
        } else {
            EmptyOutputPolicy::Skip
        })?;
        Ok(())
    }

    pub(super) fn bytes_written(&self) -> u64 {
        self.writer
            .as_ref()
            .map_or(self.completed_bytes, ObjectSetWriter::bytes_written)
    }
}
