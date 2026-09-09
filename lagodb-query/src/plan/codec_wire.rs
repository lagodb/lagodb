//! Wire encoding for query IR nodes and output layouts.

use lagodb_core::expr::RuntimeValueId;
use lagodb_core::expr::{
    ExprType, ExpressionPlanDataDecode, ExpressionPlanDataEncode, PgComparisonOp,
};
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};
use lagodb_core::query_contract::{OutputId, ScanId};

use super::codec::{QueryPlanData, QueryPlanDataError};
use super::expression::ExecutionExprCodec;
use super::join::MarkFilter;
use super::{
    AggCall, AggregateArguments, AggregateNode, AggregateOrderExpr, DistinctExpr,
    DistinctNode, FilterNode, GroupExpr, JoinKey, JoinNode, JoinType, LimitEstimate,
    LimitNode, ProjectExpr, ProjectNode, QueryNode, QueryPlanError, QueryTupleLayout,
    QueryTupleSlot, ScanNode, SortDirection, SortExpr, SortNode,
};

const NODE_SCAN: i32 = 1;
const NODE_AGGREGATE: i32 = 2;
const NODE_FILTER: i32 = 3;
const NODE_PROJECT: i32 = 4;
const NODE_DISTINCT: i32 = 5;
const NODE_JOIN: i32 = 6;
const NODE_SORT: i32 = 7;
const NODE_LIMIT: i32 = 8;

const AGG_ARGUMENTS_NONE: i32 = 0;
const AGG_ARGUMENTS_UNARY: i32 = 1;
const AGG_ARGUMENTS_STRING_AGG: i32 = 2;

impl QueryPlanData {
    pub(super) fn encode_node(node: &QueryNode, writer: &mut PlanDataWriter) {
        match node {
            QueryNode::Scan(scan) => {
                writer
                    .append_i32(NODE_SCAN)
                    .append_count(scan.scan().index())
                    .append_count(scan.columns().len());
                for column in scan.columns() {
                    writer.append_nested(|record| {
                        ExecutionExprCodec::encode_column(*column, record)
                    });
                }
                writer.append_bool(scan.filter().is_some());
                if let Some(filter) = scan.filter() {
                    writer.append_nested(|record| {
                        ExecutionExprCodec::encode(filter, record)
                    });
                }
            }
            QueryNode::Aggregate(aggregate) => {
                writer
                    .append_i32(NODE_AGGREGATE)
                    .append_i64(aggregate.estimated_rows().to_bits() as i64)
                    .append_nested(|input| {
                        Self::encode_node(aggregate.input(), input)
                    })
                    .append_count(aggregate.groups().len());
                for group in aggregate.groups() {
                    writer.append_nested(|record| {
                        ExecutionExprCodec::encode(group.expression(), record);
                        group.result_type().encode_plan_data(record);
                        record.append_count(group.output().index());
                    });
                }
                writer.append_count(aggregate.aggregates().len());
                for aggregate in aggregate.aggregates() {
                    writer.append_nested(|record| {
                        Self::encode_aggregate(aggregate, record)
                    });
                }
            }
            QueryNode::Join(join) => {
                writer
                    .append_i32(NODE_JOIN)
                    .append_i32(join.join_type().wire_id())
                    .append_bool(join.null_aware())
                    .append_i64(join.estimated_key_rows().to_bits() as i64)
                    .append_i64(join.estimated_rows().to_bits() as i64)
                    .append_nested(|input| Self::encode_node(join.left(), input))
                    .append_nested(|input| Self::encode_node(join.right(), input))
                    .append_count(join.keys().len());
                for key in join.keys() {
                    writer.append_nested(|record| {
                        ExecutionExprCodec::encode_column(key.left(), record);
                        ExecutionExprCodec::encode_column(key.right(), record);
                        key.operator().encode_plan_data(record);
                    });
                }
                writer.append_bool(join.on_filter().is_some());
                if let Some(filter) = join.on_filter() {
                    writer.append_nested(|record| {
                        ExecutionExprCodec::encode(filter, record)
                    });
                }
                writer.append_bool(join.mark_filter().is_some());
                if let Some(filter) = join.mark_filter() {
                    writer.append_nested(|record| {
                        ExecutionExprCodec::encode_column(filter.null_test(), record);
                        record.append_bool(filter.anti());
                    });
                }
            }
            QueryNode::Distinct(distinct) => {
                writer
                    .append_i32(NODE_DISTINCT)
                    .append_i64(distinct.estimated_rows().to_bits() as i64)
                    .append_nested(|input| Self::encode_node(distinct.input(), input))
                    .append_count(distinct.keys().len());
                for key in distinct.keys() {
                    writer.append_nested(|record| {
                        ExecutionExprCodec::encode(key.expression(), record);
                        key.result_type().encode_plan_data(record);
                        record.append_count(key.output().index());
                    });
                }
            }
            QueryNode::Filter(filter) => {
                writer
                    .append_i32(NODE_FILTER)
                    .append_i64(filter.estimated_rows().to_bits() as i64)
                    .append_nested(|input| Self::encode_node(filter.input(), input))
                    .append_nested(|record| {
                        ExecutionExprCodec::encode(filter.predicate(), record)
                    });
            }
            QueryNode::Project(project) => {
                writer
                    .append_i32(NODE_PROJECT)
                    .append_nested(|input| Self::encode_node(project.input(), input))
                    .append_count(project.expressions().len());
                for expression in project.expressions() {
                    writer.append_nested(|record| {
                        ExecutionExprCodec::encode(expression.expression(), record);
                        expression.result_type().encode_plan_data(record);
                        record
                            .append_count(expression.output().index())
                            .append_bool(expression.nullable());
                    });
                }
            }
            QueryNode::Sort(sort) => {
                writer
                    .append_i32(NODE_SORT)
                    .append_nested(|input| Self::encode_node(sort.input(), input))
                    .append_count(sort.keys().len());
                for key in sort.keys() {
                    writer.append_count(key.output().index());
                    key.result_type().encode_plan_data(writer);
                    writer
                        .append_bool(matches!(
                            key.direction(),
                            SortDirection::Ascending
                        ))
                        .append_bool(key.nulls_first());
                }
            }
            QueryNode::Limit(limit) => {
                let estimate = limit.estimate();
                writer
                    .append_i32(NODE_LIMIT)
                    .append_i64(estimate.output_rows().to_bits() as i64)
                    .append_i64(estimate.offset_rows().to_bits() as i64)
                    .append_bool(estimate.count_rows().is_some());
                if let Some(count_rows) = estimate.count_rows() {
                    writer.append_i64(count_rows.to_bits() as i64);
                }
                writer
                    .append_nested(|input| Self::encode_node(limit.input(), input))
                    .append_bool(limit.offset().is_some());
                if let Some(offset) = limit.offset() {
                    writer.append_count(offset.index());
                }
                writer.append_bool(limit.count().is_some());
                if let Some(count) = limit.count() {
                    writer.append_count(count.index());
                }
            }
        }
    }

    pub(super) fn decode_node(
        reader: &mut PlanDataReader<'_>,
        runtime_value_count: usize,
    ) -> Result<QueryNode, QueryPlanDataError> {
        match reader.read_i32()? {
            NODE_SCAN => {
                let scan = ScanId::from_index(reader.read_count()?);
                let count = reader.read_count()?;
                let mut columns = Vec::with_capacity(count);
                for _ in 0..count {
                    columns
                        .push(reader.read_nested(ExecutionExprCodec::decode_column)?);
                }
                let filter = if reader.read_bool()? {
                    Some(reader.read_nested(|record| {
                        ExecutionExprCodec::decode(record, runtime_value_count)
                    })?)
                } else {
                    None
                };
                Ok(QueryNode::Scan(ScanNode::new(
                    scan,
                    columns.into_boxed_slice(),
                    filter,
                )))
            }
            NODE_AGGREGATE => {
                let estimated_rows = f64::from_bits(reader.read_i64()? as u64);
                let input = reader.read_nested(|record| {
                    Self::decode_node(record, runtime_value_count)
                })?;
                let group_count = reader.read_count()?;
                let mut groups = Vec::with_capacity(group_count);
                for _ in 0..group_count {
                    groups.push(reader.read_nested(|record| {
                        GroupExpr::try_new(
                            ExecutionExprCodec::decode(record, runtime_value_count)?,
                            ExprType::decode_plan_data(record, ())?,
                            OutputId::from_index(record.read_count()?),
                        )
                        .map_err(QueryPlanDataError::from)
                    })?);
                }
                let aggregate_count = reader.read_count()?;
                let mut aggregates = Vec::with_capacity(aggregate_count);
                for _ in 0..aggregate_count {
                    aggregates.push(reader.read_nested(|record| {
                        Self::decode_aggregate(record, runtime_value_count)
                    })?);
                }
                Ok(QueryNode::Aggregate(AggregateNode::new(
                    input,
                    groups.into_boxed_slice(),
                    aggregates.into_boxed_slice(),
                    estimated_rows,
                )?))
            }
            NODE_JOIN => {
                let raw_type = reader.read_i32()?;
                let join_type = JoinType::from_wire_id(raw_type)
                    .ok_or(QueryPlanDataError::UnknownJoinType { found: raw_type })?;
                let null_aware = reader.read_bool()?;
                let estimated_key_rows = f64::from_bits(reader.read_i64()? as u64);
                let estimated_rows = f64::from_bits(reader.read_i64()? as u64);
                let left = reader.read_nested(|record| {
                    Self::decode_node(record, runtime_value_count)
                })?;
                let right = reader.read_nested(|record| {
                    Self::decode_node(record, runtime_value_count)
                })?;
                let count = reader.read_count()?;
                let mut keys = Vec::with_capacity(count);
                for _ in 0..count {
                    keys.push(reader.read_nested(|record| {
                        JoinKey::try_new(
                            ExecutionExprCodec::decode_column(record)?,
                            ExecutionExprCodec::decode_column(record)?,
                            PgComparisonOp::decode_plan_data(record, ())?,
                        )
                        .map_err(QueryPlanDataError::from)
                    })?);
                }
                let on_filter = reader
                    .read_bool()?
                    .then(|| {
                        reader.read_nested(|record| {
                            ExecutionExprCodec::decode(record, runtime_value_count)
                        })
                    })
                    .transpose()?;
                let mark_filter = reader
                    .read_bool()?
                    .then(|| {
                        reader.read_nested(|record| {
                            Ok::<_, QueryPlanDataError>(MarkFilter::new(
                                ExecutionExprCodec::decode_column(record)?,
                                record.read_bool()?,
                            ))
                        })
                    })
                    .transpose()?;
                let join = match (null_aware, mark_filter) {
                    (true, None)
                        if join_type == JoinType::LeftAnti && on_filter.is_none() =>
                    {
                        let [key] = keys.as_slice() else {
                            return Err(QueryPlanError::UnsupportedTopology.into());
                        };
                        JoinNode::new_null_aware_anti(
                            left,
                            right,
                            *key,
                            estimated_key_rows,
                            estimated_rows,
                        )?
                    }
                    (false, Some(filter))
                        if join_type == JoinType::LeftMark && on_filter.is_none() =>
                    {
                        let [key] = keys.as_slice() else {
                            return Err(QueryPlanError::UnsupportedTopology.into());
                        };
                        JoinNode::new_filtered_mark_subplan(
                            left,
                            right,
                            *key,
                            filter.null_test(),
                            filter.anti(),
                            estimated_key_rows,
                            estimated_rows,
                        )?
                    }
                    (false, None) => JoinNode::new(
                        join_type,
                        left,
                        right,
                        keys.into_boxed_slice(),
                        on_filter,
                        estimated_key_rows,
                        estimated_rows,
                    )?,
                    _ => return Err(QueryPlanError::UnsupportedTopology.into()),
                };
                Ok(QueryNode::Join(join))
            }
            NODE_DISTINCT => {
                let estimated_rows = f64::from_bits(reader.read_i64()? as u64);
                let input = reader.read_nested(|record| {
                    Self::decode_node(record, runtime_value_count)
                })?;
                let count = reader.read_count()?;
                let mut keys = Vec::with_capacity(count);
                for _ in 0..count {
                    keys.push(reader.read_nested(|record| {
                        DistinctExpr::try_new(
                            ExecutionExprCodec::decode(record, runtime_value_count)?,
                            ExprType::decode_plan_data(record, ())?,
                            OutputId::from_index(record.read_count()?),
                        )
                        .map_err(QueryPlanDataError::from)
                    })?);
                }
                Ok(QueryNode::Distinct(DistinctNode::new(
                    input,
                    keys.into_boxed_slice(),
                    estimated_rows,
                )?))
            }
            NODE_FILTER => {
                let estimated_rows = f64::from_bits(reader.read_i64()? as u64);
                Ok(QueryNode::Filter(FilterNode::new(
                    reader.read_nested(|record| {
                        Self::decode_node(record, runtime_value_count)
                    })?,
                    reader.read_nested(|record| {
                        ExecutionExprCodec::decode(record, runtime_value_count)
                    })?,
                    estimated_rows,
                )?))
            }
            NODE_PROJECT => {
                let input = reader.read_nested(|record| {
                    Self::decode_node(record, runtime_value_count)
                })?;
                let count = reader.read_count()?;
                let mut expressions = Vec::with_capacity(count);
                for _ in 0..count {
                    expressions.push(reader.read_nested(|record| {
                        Ok::<_, QueryPlanDataError>(ProjectExpr::new(
                            ExecutionExprCodec::decode(record, runtime_value_count)?,
                            ExprType::decode_plan_data(record, ())?,
                            OutputId::from_index(record.read_count()?),
                            record.read_bool()?,
                        ))
                    })?);
                }
                Ok(QueryNode::Project(ProjectNode::new(
                    input,
                    expressions.into_boxed_slice(),
                )))
            }
            NODE_SORT => {
                let input = reader.read_nested(|record| {
                    Self::decode_node(record, runtime_value_count)
                })?;
                let count = reader.read_count()?;
                let mut keys = Vec::with_capacity(count);
                for _ in 0..count {
                    let output = OutputId::from_index(reader.read_count()?);
                    let result_type = ExprType::decode_plan_data(reader, ())?;
                    let direction = if reader.read_bool()? {
                        SortDirection::Ascending
                    } else {
                        SortDirection::Descending
                    };
                    keys.push(SortExpr::try_new(
                        output,
                        result_type,
                        direction,
                        reader.read_bool()?,
                    )?);
                }
                Ok(QueryNode::Sort(SortNode::new(
                    input,
                    keys.into_boxed_slice(),
                )?))
            }
            NODE_LIMIT => {
                let estimated_rows = f64::from_bits(reader.read_i64()? as u64);
                let estimated_offset_rows = f64::from_bits(reader.read_i64()? as u64);
                let estimated_count_rows = if reader.read_bool()? {
                    Some(f64::from_bits(reader.read_i64()? as u64))
                } else {
                    None
                };
                let input = reader.read_nested(|record| {
                    Self::decode_node(record, runtime_value_count)
                })?;
                let offset = if reader.read_bool()? {
                    Some(RuntimeValueId::from_index(reader.read_count()?))
                } else {
                    None
                };
                let count = if reader.read_bool()? {
                    Some(RuntimeValueId::from_index(reader.read_count()?))
                } else {
                    None
                };
                Ok(QueryNode::Limit(LimitNode::new(
                    input,
                    offset,
                    count,
                    LimitEstimate::try_new(
                        estimated_rows,
                        estimated_offset_rows,
                        estimated_count_rows,
                    )?,
                )?))
            }
            found => Err(QueryPlanDataError::UnknownNodeKind { found }),
        }
    }

    fn encode_aggregate(aggregate: &AggCall, writer: &mut PlanDataWriter) {
        writer.append_oid(aggregate.function_oid());
        match aggregate.arguments() {
            AggregateArguments::None => {
                writer.append_i32(AGG_ARGUMENTS_NONE);
            }
            AggregateArguments::Unary(argument) => {
                writer
                    .append_i32(AGG_ARGUMENTS_UNARY)
                    .append_nested(|record| {
                        ExecutionExprCodec::encode(argument, record)
                    });
            }
            AggregateArguments::StringAgg { value, delimiter } => {
                writer
                    .append_i32(AGG_ARGUMENTS_STRING_AGG)
                    .append_nested(|record| ExecutionExprCodec::encode(value, record))
                    .append_cstr(delimiter);
            }
        }
        writer.append_bool(aggregate.is_distinct());
        writer.append_count(aggregate.order_by().len());
        for order in aggregate.order_by() {
            writer.append_nested(|record| {
                ExecutionExprCodec::encode(order.expression(), record);
                record
                    .append_bool(matches!(
                        order.direction(),
                        SortDirection::Ascending
                    ))
                    .append_bool(order.nulls_first());
            });
        }
        writer.append_bool(aggregate.filter().is_some());
        if let Some(filter) = aggregate.filter() {
            writer.append_nested(|record| ExecutionExprCodec::encode(filter, record));
        }
        aggregate.result_type().encode_plan_data(writer);
        writer.append_count(aggregate.output().index());
    }

    fn decode_aggregate(
        reader: &mut PlanDataReader<'_>,
        runtime_value_count: usize,
    ) -> Result<AggCall, QueryPlanDataError> {
        let function_oid = reader.read_oid()?;
        let arguments = match reader.read_i32()? {
            AGG_ARGUMENTS_NONE => AggregateArguments::None,
            AGG_ARGUMENTS_UNARY => {
                AggregateArguments::Unary(reader.read_nested(|record| {
                    ExecutionExprCodec::decode(record, runtime_value_count)
                })?)
            }
            AGG_ARGUMENTS_STRING_AGG => AggregateArguments::StringAgg {
                value: reader.read_nested(|record| {
                    ExecutionExprCodec::decode(record, runtime_value_count)
                })?,
                delimiter: reader.read_cstr()?.to_owned(),
            },
            _ => return Err(QueryPlanError::UnsupportedAggregate.into()),
        };
        let distinct = reader.read_bool()?;
        let order_count = reader.read_count()?;
        let mut order_by = Vec::with_capacity(order_count);
        for _ in 0..order_count {
            order_by.push(reader.read_nested(|record| {
                let expression =
                    ExecutionExprCodec::decode(record, runtime_value_count)?;
                let direction = if record.read_bool()? {
                    SortDirection::Ascending
                } else {
                    SortDirection::Descending
                };
                AggregateOrderExpr::try_new(
                    expression,
                    direction,
                    record.read_bool()?,
                )
                .map_err(QueryPlanDataError::from)
            })?);
        }
        let filter = if reader.read_bool()? {
            Some(reader.read_nested(|record| {
                ExecutionExprCodec::decode(record, runtime_value_count)
            })?)
        } else {
            None
        };
        Ok(AggCall::try_new(
            function_oid,
            arguments,
            distinct,
            order_by.into_boxed_slice(),
            filter,
            ExprType::decode_plan_data(reader, ())?,
            OutputId::from_index(reader.read_count()?),
        )?)
    }

    pub(super) fn encode_layout(
        layout: &QueryTupleLayout,
        writer: &mut PlanDataWriter,
    ) {
        writer.append_count(layout.len());
        for slot in layout.slots() {
            writer.append_nested(|record| {
                record
                    .append_count(slot.output().index())
                    .append_oid(slot.type_oid())
                    .append_i32(slot.typmod())
                    .append_oid(slot.collation())
                    .append_bool(slot.nullable());
            });
        }
    }

    pub(super) fn decode_layout(
        reader: &mut PlanDataReader<'_>,
    ) -> Result<QueryTupleLayout, QueryPlanDataError> {
        let count = reader.read_count()?;
        let mut slots = Vec::with_capacity(count);
        for _ in 0..count {
            slots.push(reader.read_nested(|record| {
                Ok::<_, QueryPlanDataError>(QueryTupleSlot::new(
                    OutputId::from_index(record.read_count()?),
                    record.read_oid()?,
                    record.read_i32()?,
                    record.read_oid()?,
                    record.read_bool()?,
                ))
            })?);
        }
        Ok(QueryTupleLayout::from_slots(slots.into_boxed_slice()))
    }
}
