//! PostgreSQL predicate planning and Parquet `RowFilter` construction.

mod explain;
mod pruning;
mod runtime;
mod value;

use pg_lakebase_core::diag::PgReportError;
use pg_lakebase_core::expr::PushdownCosting;
use pg_lakebase_core::expr::pushdown::{
    FilterBindResult, FilterColumn, FilterFragment, FilterNode, FilterPlan,
    FilterPlanningContext, FilterScalar, FilterValueBindings, FilterValueSlotId,
};
use pg_lakebase_core::fdw::ForeignFilterExplainValues;
use pg_lakebase_core::handles::RelationGuard;
use pg_lakebase_core::plan_data::{PlanDataReader, PlanDataWriter};
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::format::{
    FormatBoundFilter, FormatFilterPlan, FormatFilterPlanner, FormatKind,
    FormatPlannedFilter,
};

use self::runtime::BoundNode;
use self::value::{ComparisonOperator, ValueType};

pub(crate) use self::pruning::ParquetFilePredicate;
pub(crate) use self::runtime::ParquetBoundPredicate;

const NODE_COMPARISON: i32 = 0;
const NODE_IS_NULL: i32 = 1;
const NODE_IS_NOT_NULL: i32 = 2;
const NODE_AND: i32 = 3;
const NODE_OR: i32 = 4;
const NODE_NOT: i32 = 5;

pub(super) struct ParquetFilterPlanner {
    columns: Box<[Option<PlannedColumn>]>,
}

impl ParquetFilterPlanner {
    pub(super) fn begin(
        context: &FilterPlanningContext,
    ) -> Result<Self, ConnectorError> {
        let relation = RelationGuard::open(
            context.relation_oid(),
            pg_sys::NoLock as pg_sys::LOCKMODE,
        )
        .map_err(|error| {
            ConnectorError::Postgres(PgReportError::from_pg_error(error))
        })?;
        let handle = relation.as_handle();
        let mut columns = vec![None; handle.natts()];
        for column in handle.live_columns().iter() {
            let name = column.name().to_str().map_err(|_| {
                ConnectorError::invalid_object_schema(
                    FormatKind::Parquet,
                    "PostgreSQL column names must be valid UTF-8 for Parquet filters",
                )
            })?;
            columns[(column.attno() - 1) as usize] = Some(PlannedColumn {
                attno: column.attno(),
                name: name.into(),
            });
        }
        Ok(Self {
            columns: columns.into_boxed_slice(),
        })
    }

    fn plan_node(
        &self,
        fragment: &FilterFragment,
        node: &FilterNode,
    ) -> Option<PlannedNode> {
        match node {
            FilterNode::Comparison {
                operator,
                left,
                right,
            } => {
                let (column, value, mirrored) = match (left, right) {
                    (FilterScalar::Column(column), FilterScalar::Value(value)) => {
                        (column, *value, false)
                    }
                    (FilterScalar::Value(value), FilterScalar::Column(column)) => {
                        (column, *value, true)
                    }
                    _ => return None,
                };
                let value_type = ValueType::for_comparison(
                    column,
                    fragment.value(value),
                    operator.opno,
                    operator.opcollid,
                    operator.inputcollid,
                )?;
                let mut operator = ComparisonOperator::from_oid(operator.opno)?;
                if mirrored {
                    operator = operator.mirrored();
                }
                Some(PlannedNode::Comparison {
                    operator,
                    column: self.column(column)?,
                    value,
                    value_type,
                })
            }
            FilterNode::IsNull(FilterScalar::Column(column)) => {
                Some(PlannedNode::IsNull(self.column(column)?))
            }
            FilterNode::IsNotNull(FilterScalar::Column(column)) => {
                Some(PlannedNode::IsNotNull(self.column(column)?))
            }
            FilterNode::And(children) => {
                self.plan_children(fragment, children).map(PlannedNode::And)
            }
            FilterNode::Or(children) => {
                self.plan_children(fragment, children).map(PlannedNode::Or)
            }
            FilterNode::Not(child) => self
                .plan_node(fragment, child)
                .map(|child| PlannedNode::Not(Box::new(child))),
            FilterNode::IsNull(_) | FilterNode::IsNotNull(_) => None,
        }
    }

    fn plan_children(
        &self,
        fragment: &FilterFragment,
        children: &[FilterNode],
    ) -> Option<Box<[PlannedNode]>> {
        let mut planned = Vec::with_capacity(children.len());
        for child in children {
            planned.push(self.plan_node(fragment, child)?);
        }
        (!planned.is_empty()).then(|| planned.into_boxed_slice())
    }

    fn column(&self, column: &FilterColumn) -> Option<PlannedColumn> {
        let index = usize::try_from(column.attno - 1).ok()?;
        self.columns.get(index)?.clone()
    }
}

impl FormatFilterPlanner for ParquetFilterPlanner {
    fn try_plan_filter(
        &mut self,
        fragment: &FilterFragment,
    ) -> Result<FilterPlan<FormatPlannedFilter>, ConnectorError> {
        let Some(root) = self.plan_node(fragment, fragment.root()) else {
            return Ok(FilterPlan::Unsupported);
        };
        Ok(FilterPlan::exact(
            Box::new(ParquetPlannedPredicate { root }),
            PushdownCosting::CostedPruning,
        ))
    }
}

pub(super) struct ParquetPlannedPredicate {
    root: PlannedNode,
}

impl ParquetPlannedPredicate {
    pub(super) fn decode(
        kind: FormatKind,
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
    ) -> Result<Self, ConnectorError> {
        if kind != FormatKind::Parquet {
            return Err(ConnectorError::invalid_filter_plan(kind));
        }
        let root = reader
            .read_nested(|nested| PlannedNode::decode(nested, binding_count))?;
        Ok(Self { root })
    }
}

impl FormatFilterPlan for ParquetPlannedPredicate {
    fn kind(&self) -> FormatKind {
        FormatKind::Parquet
    }

    fn encode(&self, writer: &mut PlanDataWriter) -> Result<(), ConnectorError> {
        writer.append_nested(|nested| self.root.encode(nested));
        Ok(())
    }

    fn explain(&self, values: ForeignFilterExplainValues<'_>) -> String {
        let mut output = String::new();
        self.root.write_explain(&mut output, values);
        output
    }

    fn bind(
        &self,
        values: FilterValueBindings<'_>,
    ) -> Result<FilterBindResult<FormatBoundFilter>, ConnectorError> {
        let root = self.root.bind(values)?;
        Ok(FilterBindResult::Bound(FormatBoundFilter::Parquet(
            ParquetBoundPredicate::new(root),
        )))
    }
}

#[derive(Clone)]
struct PlannedColumn {
    attno: pg_sys::AttrNumber,
    name: Box<str>,
}

impl PlannedColumn {
    fn encode(&self, writer: &mut PlanDataWriter) {
        writer.append_i32(self.attno as i32).append_str(&self.name);
    }

    fn decode(reader: &mut PlanDataReader<'_>) -> Result<Self, ConnectorError> {
        let attno = reader.read_i32()?;
        if attno <= 0 || attno > pg_sys::MaxHeapAttributeNumber as i32 {
            return Err(ConnectorError::invalid_filter_plan(FormatKind::Parquet));
        }
        Ok(Self {
            attno: attno as pg_sys::AttrNumber,
            name: reader.read_str()?.into(),
        })
    }
}

enum PlannedNode {
    Comparison {
        operator: ComparisonOperator,
        column: PlannedColumn,
        value: FilterValueSlotId,
        value_type: ValueType,
    },
    IsNull(PlannedColumn),
    IsNotNull(PlannedColumn),
    And(Box<[Self]>),
    Or(Box<[Self]>),
    Not(Box<Self>),
}

impl PlannedNode {
    fn encode(&self, writer: &mut PlanDataWriter) {
        match self {
            Self::Comparison {
                operator,
                column,
                value,
                value_type,
            } => {
                writer
                    .append_i32(NODE_COMPARISON)
                    .append_i32(operator.tag());
                column.encode(writer);
                writer
                    .append_count(value.index())
                    .append_i32(value_type.tag());
            }
            Self::IsNull(column) => {
                writer.append_i32(NODE_IS_NULL);
                column.encode(writer);
            }
            Self::IsNotNull(column) => {
                writer.append_i32(NODE_IS_NOT_NULL);
                column.encode(writer);
            }
            Self::And(children) => Self::encode_children(writer, NODE_AND, children),
            Self::Or(children) => Self::encode_children(writer, NODE_OR, children),
            Self::Not(child) => {
                writer
                    .append_i32(NODE_NOT)
                    .append_nested(|nested| child.encode(nested));
            }
        }
    }

    fn encode_children(writer: &mut PlanDataWriter, tag: i32, children: &[Self]) {
        writer.append_i32(tag).append_count(children.len());
        for child in children {
            writer.append_nested(|nested| child.encode(nested));
        }
    }

    fn decode(
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
    ) -> Result<Self, ConnectorError> {
        match reader.read_i32()? {
            NODE_COMPARISON => {
                let operator = ComparisonOperator::from_tag(reader.read_i32()?)?;
                let column = PlannedColumn::decode(reader)?;
                let index = reader.read_count()?;
                let value = FilterValueSlotId::from_plan_data(index, binding_count)
                    .ok_or_else(|| {
                    ConnectorError::invalid_filter_plan(FormatKind::Parquet)
                })?;
                let value_type = ValueType::from_tag(reader.read_i32()?)?;
                if !value_type.accepts_operator(operator) {
                    return Err(ConnectorError::invalid_filter_plan(
                        FormatKind::Parquet,
                    ));
                }
                Ok(Self::Comparison {
                    operator,
                    column,
                    value,
                    value_type,
                })
            }
            NODE_IS_NULL => Ok(Self::IsNull(PlannedColumn::decode(reader)?)),
            NODE_IS_NOT_NULL => Ok(Self::IsNotNull(PlannedColumn::decode(reader)?)),
            NODE_AND => Ok(Self::And(Self::decode_children(reader, binding_count)?)),
            NODE_OR => Ok(Self::Or(Self::decode_children(reader, binding_count)?)),
            NODE_NOT => {
                Ok(Self::Not(Box::new(reader.read_nested(|nested| {
                    Self::decode(nested, binding_count)
                })?)))
            }
            _ => Err(ConnectorError::invalid_filter_plan(FormatKind::Parquet)),
        }
    }

    fn decode_children(
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
    ) -> Result<Box<[Self]>, ConnectorError> {
        let count = reader.read_count()?;
        if count == 0 {
            return Err(ConnectorError::invalid_filter_plan(FormatKind::Parquet));
        }
        let mut children = Vec::with_capacity(count);
        for _ in 0..count {
            children.push(
                reader.read_nested(|nested| Self::decode(nested, binding_count))?,
            );
        }
        Ok(children.into_boxed_slice())
    }

    fn bind(
        &self,
        values: FilterValueBindings<'_>,
    ) -> Result<BoundNode, ConnectorError> {
        self.bind_negated(values, false)
    }

    fn bind_negated(
        &self,
        values: FilterValueBindings<'_>,
        negated: bool,
    ) -> Result<BoundNode, ConnectorError> {
        Ok(match self {
            Self::Comparison {
                operator,
                column,
                value,
                value_type,
            } => {
                let value = values.value(*value);
                if value.is_null() {
                    // A strict comparison with NULL is never TRUE, including
                    // below NOT. The pushed predicate represents the SQL WHERE
                    // truth set, so UNKNOWN can be folded to NeverTrue here.
                    BoundNode::NeverTrue
                } else {
                    BoundNode::Comparison {
                        operator: if negated {
                            operator.negated()
                        } else {
                            *operator
                        },
                        column: column.clone(),
                        value: unsafe { value_type.decode(value)? },
                    }
                }
            }
            Self::IsNull(column) if negated => BoundNode::IsNotNull(column.clone()),
            Self::IsNull(column) => BoundNode::IsNull(column.clone()),
            Self::IsNotNull(column) if negated => BoundNode::IsNull(column.clone()),
            Self::IsNotNull(column) => BoundNode::IsNotNull(column.clone()),
            Self::And(children) => {
                let bound = children
                    .iter()
                    .map(|child| child.bind_negated(values, negated))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_boxed_slice();
                if negated {
                    BoundNode::Or(bound)
                } else {
                    BoundNode::And(bound)
                }
            }
            Self::Or(children) => {
                let bound = children
                    .iter()
                    .map(|child| child.bind_negated(values, negated))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_boxed_slice();
                if negated {
                    BoundNode::And(bound)
                } else {
                    BoundNode::Or(bound)
                }
            }
            Self::Not(child) => child.bind_negated(values, !negated)?,
        })
    }
}
