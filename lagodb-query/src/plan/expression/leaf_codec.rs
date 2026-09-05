//! Wire encoding for exact-expression identity leaves.

use lagodb_core::expr::{
    ColumnRef, ExprType, ExpressionCodecError, ExpressionPlanDataDecode,
    ExpressionPlanDataEncode, RuntimeValueId,
};
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};
use lagodb_core::query_contract::{OutputId, ScanId};
use pgrx::pg_sys;

use super::ExecutionExpr;
use crate::plan::QueryPlanDataError;

const COLUMN: i32 = 1;
const VALUE: i32 = 2;
const OUTPUT: i32 = 3;

pub(super) struct ExecutionLeafCodec;

impl ExecutionLeafCodec {
    pub(super) fn encode_column(column: ColumnRef, writer: &mut PlanDataWriter) {
        writer
            .append_i32(COLUMN)
            .append_count(column.scan.index())
            .append_i32(i32::from(column.attno));
        column.declared_type.encode_plan_data(writer);
        column.value_type.encode_plan_data(writer);
    }

    pub(super) fn encode_value(value: RuntimeValueId, writer: &mut PlanDataWriter) {
        writer.append_i32(VALUE).append_count(value.index());
    }

    pub(super) fn encode_output(output: OutputId, writer: &mut PlanDataWriter) {
        writer.append_i32(OUTPUT).append_count(output.index());
    }

    pub(super) fn decode_column(
        reader: &mut PlanDataReader<'_>,
    ) -> Result<ColumnRef, QueryPlanDataError> {
        let tag = reader.read_i32()?;
        if tag != COLUMN {
            return Err(ExpressionCodecError::UnknownScalar(tag).into());
        }
        Self::decode_column_payload(reader)
    }

    pub(super) fn decode(
        reader: &mut PlanDataReader<'_>,
        runtime_value_count: usize,
    ) -> Result<ExecutionExpr, QueryPlanDataError> {
        match reader.read_i32()? {
            COLUMN => Ok(ExecutionExpr::Column(Self::decode_column_payload(reader)?)),
            VALUE => {
                let index = reader.read_count()?;
                if index >= runtime_value_count {
                    return Err(ExpressionCodecError::RuntimeValueOutOfBounds {
                        index,
                        count: runtime_value_count,
                    }
                    .into());
                }
                Ok(ExecutionExpr::Value(RuntimeValueId::from_index(index)))
            }
            OUTPUT => Ok(ExecutionExpr::Output(OutputId::from_index(
                reader.read_count()?,
            ))),
            tag => Err(ExpressionCodecError::UnknownScalar(tag).into()),
        }
    }

    fn decode_column_payload(
        reader: &mut PlanDataReader<'_>,
    ) -> Result<ColumnRef, QueryPlanDataError> {
        let scan = ScanId::from_index(reader.read_count()?);
        let raw_attno = reader.read_i32()?;
        let attno = pg_sys::AttrNumber::try_from(raw_attno)
            .map_err(|_| ExpressionCodecError::InvalidAttribute(raw_attno))?;
        if attno <= 0 {
            return Err(ExpressionCodecError::InvalidAttribute(raw_attno).into());
        }
        Ok(ColumnRef {
            scan,
            attno,
            declared_type: ExprType::decode_plan_data(reader, ())?,
            value_type: ExprType::decode_plan_data(reader, ())?,
        })
    }
}
