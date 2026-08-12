//! Prefix-only immutable Parquet Foreign Table writer.

use std::sync::Arc;

use arrow_schema::{Field, Schema};
use pg_arrow_conv::{
    BoundWriteBuffer, BoundWriteColumnPlan, PgColumnType, resolve_column_rule,
};
use pg_lakebase_core::batch::BatchBuffer;
use pg_lakebase_core::fdw::{ForeignModifyOutcome, ModifyPlanSlot, ModifySlot};
use pg_lakebase_core::handles::RelationHandle;

use crate::error::ConnectorError;
use crate::format::{FormatWriteState, ParquetWriteCompression};
use crate::storage::ObjectOutput;

use super::schema::parquet_arrow_type;
use super::writer::ParquetObjectWriter;

const INSERT_BATCH_SIZE: i32 = 1_000;
const BUFFER_FLUSH_BYTES: usize = 8 * 1024 * 1024;

pub(super) struct ParquetWriteState {
    buffer: BoundWriteBuffer,
    writer: ParquetObjectWriter,
}

impl ParquetWriteState {
    pub(super) fn begin(
        relation: &RelationHandle<'_>,
        output: ObjectOutput,
        compression: ParquetWriteCompression,
    ) -> Result<Self, ConnectorError> {
        let (schema, plans) = Self::bind_schema(relation)?;
        let buffer = BoundWriteBuffer::new(Arc::clone(&schema), plans)?;
        let writer =
            ParquetObjectWriter::new(output, Arc::clone(&schema), compression);
        Ok(Self { buffer, writer })
    }

    fn bind_schema(
        relation: &RelationHandle<'_>,
    ) -> Result<(Arc<Schema>, Box<[BoundWriteColumnPlan]>), ConnectorError> {
        let live = relation.live_columns();
        let attr_types = relation.attr_types();
        let mut fields = Vec::with_capacity(live.len());
        let mut sources = Vec::with_capacity(live.len());
        for (attno, name) in live {
            let source = (attno - 1) as usize;
            let (oid, typmod) = attr_types[source];
            fields.push(Field::new(name, parquet_arrow_type(oid, typmod)?, true));
            sources.push((source, oid));
        }
        let schema = Arc::new(Schema::new(fields));
        let plans = schema
            .fields()
            .iter()
            .zip(sources)
            .map(|(field, (source, oid))| {
                let pg = PgColumnType::from_pg_type(oid).ok_or_else(|| {
                    ConnectorError::invalid_object_schema(
                        crate::format::FormatKind::Parquet,
                        format!("PostgreSQL type OID {oid} has no Arrow conversion"),
                    )
                })?;
                let rule = resolve_column_rule(field.data_type(), pg)?;
                Ok(BoundWriteColumnPlan::bind(
                    rule,
                    Some(source),
                    Some(oid),
                    relation.natts(),
                )?)
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?
            .into_boxed_slice();
        Ok((schema, plans))
    }

    fn flush_batch(&mut self) -> Result<(), ConnectorError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let batch = self.buffer.finish_batch()?;
        self.writer.write_batch(&batch)
    }
}

impl FormatWriteState for ParquetWriteState {
    fn batch_size(&self) -> Result<core::ffi::c_int, ConnectorError> {
        Ok(INSERT_BATCH_SIZE)
    }

    fn insert(
        &mut self,
        slot: &mut ModifySlot<'_>,
    ) -> Result<ForeignModifyOutcome, ConnectorError> {
        unsafe { self.buffer.append_slot_row(slot.tuple_row()) }?;
        if self.buffer.should_flush(BUFFER_FLUSH_BYTES) {
            self.flush_batch()?;
        }
        Ok(ForeignModifyOutcome::Applied)
    }

    fn update(
        &mut self,
        _slot: &mut ModifySlot<'_>,
        _plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ConnectorError> {
        Err(ConnectorError::modify_not_implemented(
            crate::format::FormatKind::Parquet,
        ))
    }

    fn delete(
        &mut self,
        _returned_slot: Option<&mut ModifySlot<'_>>,
        _plan_slot: &ModifyPlanSlot<'_>,
    ) -> Result<ForeignModifyOutcome, ConnectorError> {
        Err(ConnectorError::modify_not_implemented(
            crate::format::FormatKind::Parquet,
        ))
    }

    fn finish(&mut self) -> Result<(), ConnectorError> {
        self.flush_batch()?;
        self.writer.finish(false)
    }
}
