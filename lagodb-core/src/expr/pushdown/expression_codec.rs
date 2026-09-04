//! `copyObject`-safe wire contracts for shared expression IR.

use crate::expr::contract::PgComparisonOp;
use crate::expr::{
    ColumnRef, ExprType, ExpressionCodecError, ExpressionPlanDataDecode,
    ExpressionPlanDataEncode, RuntimeValueId, RuntimeValueLayout,
};
use crate::plan_data::{PlanDataReader, PlanDataWriter};
use crate::query_contract::ScanId;
use pgrx::pg_sys;

use super::{PredicateExpr, PredicateFragment, ScalarExpr};

const SCALAR_COLUMN: i32 = 1;
const SCALAR_VALUE: i32 = 2;
const PREDICATE_COMPARISON: i32 = 1;
const PREDICATE_IS_NULL: i32 = 2;
const PREDICATE_IS_NOT_NULL: i32 = 3;
const PREDICATE_AND: i32 = 4;
const PREDICATE_OR: i32 = 5;
const PREDICATE_NOT: i32 = 6;

impl PredicateFragment {
    pub fn encode_plan_data(
        &self,
    ) -> Result<*mut pg_sys::List, ExpressionCodecError> {
        PlanDataWriter::encode_list(|writer| {
            writer
                .append_nested(|record| self.root().encode_plan_data(record))
                .append_nested(|record| {
                    self.runtime_values().encode_plan_data(record)
                });
            Ok(())
        })
    }

    /// # Safety
    ///
    /// `list` must be a live, plan-owned expression plan-data list.
    pub unsafe fn decode_plan_data(
        list: *mut pg_sys::List,
    ) -> Result<Self, ExpressionCodecError> {
        unsafe {
            PlanDataReader::decode_checked_list(list, 0, |reader| {
                let predicate_record = reader.read_encoded_list()?;
                let layout = reader.read_nested(|record| {
                    RuntimeValueLayout::decode_plan_data(record, ())
                })?;
                let predicate = PlanDataReader::decode_checked_list(
                    predicate_record,
                    0,
                    |record| PredicateExpr::decode_plan_data(record, layout.len()),
                )?;
                Ok(Self::from_layout(predicate, layout))
            })
        }
    }
}

impl ExpressionPlanDataEncode for ScalarExpr {
    fn encode_plan_data(&self, writer: &mut PlanDataWriter) {
        match self {
            Self::Column(column) => {
                writer
                    .append_i32(SCALAR_COLUMN)
                    .append_count(column.scan.index())
                    .append_i32(i32::from(column.attno));
                column.declared_type.encode_plan_data(writer);
                column.value_type.encode_plan_data(writer);
            }
            Self::Value(value) => {
                writer.append_i32(SCALAR_VALUE).append_count(value.index());
            }
        }
    }
}

impl ExpressionPlanDataDecode for ScalarExpr {
    type Context = usize;

    fn decode_plan_data(
        reader: &mut PlanDataReader<'_>,
        runtime_value_count: usize,
    ) -> Result<Self, ExpressionCodecError> {
        match reader.read_i32()? {
            SCALAR_COLUMN => {
                let scan = ScanId::from_index(reader.read_count()?);
                let raw_attno = reader.read_i32()?;
                let attno = pg_sys::AttrNumber::try_from(raw_attno)
                    .map_err(|_| ExpressionCodecError::InvalidAttribute(raw_attno))?;
                if attno <= 0 {
                    return Err(ExpressionCodecError::InvalidAttribute(raw_attno));
                }
                Ok(Self::Column(ColumnRef {
                    scan,
                    attno,
                    declared_type: ExprType::decode_plan_data(reader, ())?,
                    value_type: ExprType::decode_plan_data(reader, ())?,
                }))
            }
            SCALAR_VALUE => {
                let index = reader.read_count()?;
                if index >= runtime_value_count {
                    return Err(ExpressionCodecError::RuntimeValueOutOfBounds {
                        index,
                        count: runtime_value_count,
                    });
                }
                Ok(Self::Value(RuntimeValueId::from_index(index)))
            }
            tag => Err(ExpressionCodecError::UnknownScalar(tag)),
        }
    }
}

impl ExpressionPlanDataEncode for PredicateExpr {
    fn encode_plan_data(&self, writer: &mut PlanDataWriter) {
        match self {
            Self::Comparison {
                operator,
                left,
                right,
            } => {
                writer.append_i32(PREDICATE_COMPARISON);
                operator.encode_plan_data(writer);
                writer.append_nested(|record| left.encode_plan_data(record));
                writer.append_nested(|record| right.encode_plan_data(record));
            }
            Self::IsNull(value) => {
                writer
                    .append_i32(PREDICATE_IS_NULL)
                    .append_nested(|record| value.encode_plan_data(record));
            }
            Self::IsNotNull(value) => {
                writer
                    .append_i32(PREDICATE_IS_NOT_NULL)
                    .append_nested(|record| value.encode_plan_data(record));
            }
            Self::And(children) | Self::Or(children) => {
                writer
                    .append_i32(if matches!(self, Self::And(_)) {
                        PREDICATE_AND
                    } else {
                        PREDICATE_OR
                    })
                    .append_count(children.len());
                for child in children {
                    writer.append_nested(|record| child.encode_plan_data(record));
                }
            }
            Self::Not(child) => {
                writer
                    .append_i32(PREDICATE_NOT)
                    .append_nested(|record| child.encode_plan_data(record));
            }
        }
    }
}

impl ExpressionPlanDataDecode for PredicateExpr {
    type Context = usize;

    fn decode_plan_data(
        reader: &mut PlanDataReader<'_>,
        runtime_value_count: usize,
    ) -> Result<Self, ExpressionCodecError> {
        let tag = reader.read_i32()?;
        match tag {
            PREDICATE_COMPARISON => Ok(Self::Comparison {
                operator: PgComparisonOp::decode_plan_data(reader, ())?,
                left: reader.read_nested(|record| {
                    ScalarExpr::decode_plan_data(record, runtime_value_count)
                })?,
                right: reader.read_nested(|record| {
                    ScalarExpr::decode_plan_data(record, runtime_value_count)
                })?,
            }),
            PREDICATE_IS_NULL => Ok(Self::IsNull(reader.read_nested(|record| {
                ScalarExpr::decode_plan_data(record, runtime_value_count)
            })?)),
            PREDICATE_IS_NOT_NULL => {
                Ok(Self::IsNotNull(reader.read_nested(|record| {
                    ScalarExpr::decode_plan_data(record, runtime_value_count)
                })?))
            }
            PREDICATE_AND | PREDICATE_OR => {
                let count = reader.read_count()?;
                let mut children = Vec::with_capacity(count);
                for _ in 0..count {
                    children.push(reader.read_nested(|record| {
                        Self::decode_plan_data(record, runtime_value_count)
                    })?);
                }
                Ok(if tag == PREDICATE_AND {
                    Self::And(children.into_boxed_slice())
                } else {
                    Self::Or(children.into_boxed_slice())
                })
            }
            PREDICATE_NOT => {
                Ok(Self::Not(Box::new(reader.read_nested(|record| {
                    Self::decode_plan_data(record, runtime_value_count)
                })?)))
            }
            tag => Err(ExpressionCodecError::UnknownPredicate(tag)),
        }
    }
}
