//! `copyObject`-safe wire contracts for expression-domain value metadata.

use crate::expr::contract::PgComparisonOp;
use crate::expr::{
    ExprType, RuntimeValueLayout, RuntimeValueSource, RuntimeValueSpec,
};
use crate::plan_data::{PlanDataError, PlanDataReader, PlanDataWriter};

#[derive(Debug, thiserror::Error)]
pub enum ExpressionCodecError {
    #[error("expression plan-data primitive failed: {0}")]
    PlanData(#[from] PlanDataError),
    #[error("unknown scalar expression tag {0}")]
    UnknownScalar(i32),
    #[error("unknown predicate expression tag {0}")]
    UnknownPredicate(i32),
    #[error("unknown runtime value source tag {0}")]
    UnknownRuntimeValueSource(i32),
    #[error("runtime value identity {index} exceeds layout length {count}")]
    RuntimeValueOutOfBounds { index: usize, count: usize },
    #[error("column expression contains invalid attribute number {0}")]
    InvalidAttribute(i32),
}

pub trait ExpressionPlanDataEncode {
    fn encode_plan_data(&self, writer: &mut PlanDataWriter);
}

pub trait ExpressionPlanDataDecode: Sized {
    type Context: Copy;

    fn decode_plan_data(
        reader: &mut PlanDataReader<'_>,
        context: Self::Context,
    ) -> Result<Self, ExpressionCodecError>;
}

impl ExpressionPlanDataEncode for RuntimeValueLayout {
    fn encode_plan_data(&self, writer: &mut PlanDataWriter) {
        writer.append_count(self.len());
        for value in self.values() {
            writer.append_nested(|record| {
                value.value_type.encode_plan_data(record);
                record.append_i32(match value.source_kind {
                    RuntimeValueSource::Constant => 1,
                    RuntimeValueSource::ExternalParam => 2,
                    RuntimeValueSource::ExecParam => 3,
                    RuntimeValueSource::OuterValue => 4,
                });
            });
        }
    }
}

impl ExpressionPlanDataDecode for RuntimeValueLayout {
    type Context = ();

    fn decode_plan_data(
        reader: &mut PlanDataReader<'_>,
        _context: Self::Context,
    ) -> Result<Self, ExpressionCodecError> {
        let count = reader.read_count()?;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(reader.read_nested(|record| {
                let value_type = ExprType::decode_plan_data(record, ())?;
                let source_kind = match record.read_i32()? {
                    1 => RuntimeValueSource::Constant,
                    2 => RuntimeValueSource::ExternalParam,
                    3 => RuntimeValueSource::ExecParam,
                    4 => RuntimeValueSource::OuterValue,
                    tag => {
                        return Err(ExpressionCodecError::UnknownRuntimeValueSource(
                            tag,
                        ));
                    }
                };
                Ok(RuntimeValueSpec {
                    value_type,
                    source_kind,
                })
            })?);
        }
        Ok(Self::new(values.into_boxed_slice()))
    }
}

impl ExpressionPlanDataEncode for ExprType {
    fn encode_plan_data(&self, writer: &mut PlanDataWriter) {
        writer
            .append_oid(self.type_oid)
            .append_i32(self.typmod)
            .append_oid(self.collation);
    }
}

impl ExpressionPlanDataDecode for ExprType {
    type Context = ();

    fn decode_plan_data(
        reader: &mut PlanDataReader<'_>,
        _context: Self::Context,
    ) -> Result<Self, ExpressionCodecError> {
        Ok(Self {
            type_oid: reader.read_oid()?,
            typmod: reader.read_i32()?,
            collation: reader.read_oid()?,
        })
    }
}

impl ExpressionPlanDataEncode for PgComparisonOp {
    fn encode_plan_data(&self, writer: &mut PlanDataWriter) {
        writer
            .append_oid(self.opno)
            .append_oid(self.opfuncid)
            .append_oid(self.opresulttype)
            .append_oid(self.opcollid)
            .append_oid(self.inputcollid);
    }
}

impl ExpressionPlanDataDecode for PgComparisonOp {
    type Context = ();

    fn decode_plan_data(
        reader: &mut PlanDataReader<'_>,
        _context: Self::Context,
    ) -> Result<Self, ExpressionCodecError> {
        Ok(Self {
            opno: reader.read_oid()?,
            opfuncid: reader.read_oid()?,
            opresulttype: reader.read_oid()?,
            opcollid: reader.read_oid()?,
            inputcollid: reader.read_oid()?,
        })
    }
}
