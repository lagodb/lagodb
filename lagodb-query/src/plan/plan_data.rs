//! `copyObject`-safe codec for the minimal query plan and output layout.

use lagodb_core::plan_data::{PlanDataError, PlanDataReader, PlanDataWriter};
use lagodb_core::query_contract::SourceId;
use pgrx::pg_sys;

use super::OutputId;
use super::ir::{
    AggregateExpression, CountStar, QueryFragment, QueryPlanError, QueryTupleLayout,
    QueryTupleSlot, S1M_OUTPUT_COUNT, S1M_SOURCE_COUNT,
};

const QUERY_PLAN_KIND: i32 = 0x4c51_0001;
const QUERY_FRAGMENT_KIND: i32 = 0x4c51_0002;
const QUERY_TUPLE_LAYOUT_KIND: i32 = 0x4c51_0003;
const QUERY_PLAN_VERSION: i32 = 2;

const NODE_SOURCE: i32 = 1;
const NODE_AGGREGATE: i32 = 2;
const NODE_PROJECT: i32 = 3;
const AGGREGATE_COUNT_STAR: i32 = 1;

/// Failure while encoding or decoding query plan data.
#[derive(Debug, thiserror::Error)]
pub enum QueryPlanDataError {
    #[error("query plan-data primitive failed: {0}")]
    PlanData(#[from] PlanDataError),
    #[error("invalid query plan: {0}")]
    InvalidPlan(#[from] QueryPlanError),
    #[error("{frame} has kind {found}, expected {expected}")]
    WrongKind {
        frame: &'static str,
        found: i32,
        expected: i32,
    },
    #[error("query plan-data version {found} is unsupported; expected {expected}")]
    WrongVersion { found: i32, expected: i32 },
    #[error("query tuple layout has {found} outputs, expected {expected}")]
    WrongOutputCount { found: usize, expected: usize },
    #[error("aggregate node has {found} expressions, expected {expected}")]
    WrongAggregateCount { found: usize, expected: usize },
}

impl QueryPlanDataError {
    fn expect_kind(
        frame: &'static str,
        found: i32,
        expected: i32,
    ) -> Result<(), Self> {
        if found == expected {
            Ok(())
        } else {
            Err(Self::WrongKind {
                frame,
                found,
                expected,
            })
        }
    }

    fn expect_version(found: i32) -> Result<(), Self> {
        if found == QUERY_PLAN_VERSION {
            Ok(())
        } else {
            Err(Self::WrongVersion {
                found,
                expected: QUERY_PLAN_VERSION,
            })
        }
    }
}

/// Semantic fragment paired with its physical PostgreSQL output contract.
///
/// Provider source payloads and cost fields remain outside this core record;
/// their codecs are owned by the provider planning step that creates them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPlanData {
    fragment: QueryFragment,
    tuple_layout: QueryTupleLayout,
}

impl QueryPlanData {
    pub fn scalar_count(
        source: SourceId,
        function_oid: pg_sys::Oid,
        result_type: pg_sys::Oid,
    ) -> Result<Self, QueryPlanError> {
        let fragment =
            QueryFragment::scalar_count(source, function_oid, result_type)?;
        let (_, aggregate, project) = fragment.scalar_count_parts();
        let [expression] = aggregate.aggregates() else {
            unreachable!("validated scalar COUNT contains one aggregate expression")
        };
        let AggregateExpression::CountStar(count) = expression;
        let tuple_layout =
            QueryTupleLayout::scalar_count(project.output(), count.result_type());
        let plan = Self {
            fragment,
            tuple_layout,
        };
        plan.validate()?;
        Ok(plan)
    }

    #[inline]
    pub const fn fragment(&self) -> &QueryFragment {
        &self.fragment
    }

    #[inline]
    pub const fn tuple_layout(&self) -> &QueryTupleLayout {
        &self.tuple_layout
    }

    /// Encode one complete plan-data frame in the current planner memory
    /// context.
    pub fn encode(&self) -> Result<*mut pg_sys::List, QueryPlanDataError> {
        self.validate()?;
        PlanDataWriter::encode_list(|writer| {
            writer
                .append_i32(QUERY_PLAN_KIND)
                .append_i32(QUERY_PLAN_VERSION)
                .append_nested(|fragment| self.encode_fragment(fragment))
                .append_nested(|layout| self.encode_tuple_layout(layout));
            Ok(())
        })
    }

    /// Decode and validate one complete plan-data frame.
    ///
    /// # Safety
    ///
    /// `list` must point to a live PostgreSQL node for the duration of this
    /// call. A non-list node is rejected before any list cell is accessed.
    pub unsafe fn decode(
        list: *mut pg_sys::List,
    ) -> Result<Self, QueryPlanDataError> {
        unsafe {
            PlanDataReader::decode_checked_list(list, 0, |reader| {
                QueryPlanDataError::expect_kind(
                    "query plan",
                    reader.read_i32()?,
                    QUERY_PLAN_KIND,
                )?;
                QueryPlanDataError::expect_version(reader.read_i32()?)?;
                let fragment = reader.read_nested(Self::decode_fragment)?;
                let tuple_layout = reader.read_nested(Self::decode_tuple_layout)?;
                let plan = Self {
                    fragment,
                    tuple_layout,
                };
                plan.validate()?;
                Ok(plan)
            })
        }
    }

    fn validate(&self) -> Result<(), QueryPlanError> {
        let (_, aggregate, project) = self.fragment.scalar_count_parts();
        let [expression] = aggregate.aggregates() else {
            unreachable!("validated scalar COUNT contains one aggregate expression")
        };
        let AggregateExpression::CountStar(count) = expression;
        let slot = self.tuple_layout.slot();
        if project.output() != slot.output() {
            return Err(QueryPlanError::TupleLayoutOutputMismatch);
        }
        if count.result_type() != slot.type_oid() {
            return Err(QueryPlanError::TupleLayoutTypeMismatch);
        }
        Ok(())
    }

    fn encode_fragment(&self, writer: &mut PlanDataWriter) {
        let (source, aggregate, project) = self.fragment.scalar_count_parts();
        let [expression] = aggregate.aggregates() else {
            unreachable!("validated scalar COUNT contains one aggregate expression")
        };
        let AggregateExpression::CountStar(count) = expression;
        writer
            .append_i32(QUERY_FRAGMENT_KIND)
            .append_i32(QUERY_PLAN_VERSION)
            .append_nested(|project_record| {
                project_record
                    .append_i32(NODE_PROJECT)
                    .append_nested(|aggregate_record| {
                        aggregate_record
                            .append_i32(NODE_AGGREGATE)
                            .append_nested(|source_record| {
                                source_record
                                    .append_i32(NODE_SOURCE)
                                    .append_count(source.source().index());
                            })
                            .append_count(1)
                            .append_i32(AGGREGATE_COUNT_STAR)
                            .append_oid(count.function_oid())
                            .append_oid(count.result_type())
                            .append_count(count.output().index());
                    })
                    .append_count(project.output().index());
            });
    }

    fn encode_tuple_layout(&self, writer: &mut PlanDataWriter) {
        let slot = self.tuple_layout.slot();
        writer
            .append_i32(QUERY_TUPLE_LAYOUT_KIND)
            .append_i32(QUERY_PLAN_VERSION)
            .append_count(self.tuple_layout.len())
            .append_count(slot.output().index())
            .append_oid(slot.type_oid())
            .append_i32(slot.typmod())
            .append_oid(slot.collation());
    }

    fn decode_fragment(
        reader: &mut PlanDataReader<'_>,
    ) -> Result<QueryFragment, QueryPlanDataError> {
        QueryPlanDataError::expect_kind(
            "query fragment",
            reader.read_i32()?,
            QUERY_FRAGMENT_KIND,
        )?;
        QueryPlanDataError::expect_version(reader.read_i32()?)?;
        let (source, count, project_output) = reader
            .read_nested::<_, QueryPlanDataError>(|project| {
                QueryPlanDataError::expect_kind(
                    "query root node",
                    project.read_i32()?,
                    NODE_PROJECT,
                )?;
                let (source, count) =
                    project.read_nested::<_, QueryPlanDataError>(|aggregate| {
                        QueryPlanDataError::expect_kind(
                            "project input node",
                            aggregate.read_i32()?,
                            NODE_AGGREGATE,
                        )?;
                        let source = aggregate.read_nested::<_, QueryPlanDataError>(
                            |source| {
                                QueryPlanDataError::expect_kind(
                                    "aggregate input node",
                                    source.read_i32()?,
                                    NODE_SOURCE,
                                )?;
                                let index = source.read_count()?;
                                SourceId::from_plan_data(index, S1M_SOURCE_COUNT)
                                    .ok_or(QueryPlanDataError::InvalidPlan(
                                        QueryPlanError::SourceOutOfBounds { index },
                                    ))
                            },
                        )?;
                        let aggregate_count = aggregate.read_count()?;
                        if aggregate_count != 1 {
                            return Err(QueryPlanDataError::WrongAggregateCount {
                                found: aggregate_count,
                                expected: 1,
                            });
                        }
                        QueryPlanDataError::expect_kind(
                            "aggregate expression",
                            aggregate.read_i32()?,
                            AGGREGATE_COUNT_STAR,
                        )?;
                        let function_oid = aggregate.read_oid()?;
                        let result_type = aggregate.read_oid()?;
                        let output_index = aggregate.read_count()?;
                        let output =
                            OutputId::from_plan_data(output_index, S1M_OUTPUT_COUNT)
                                .ok_or(QueryPlanError::OutputOutOfBounds {
                                    index: output_index,
                                })?;
                        let count = CountStar::from_plan_data(
                            function_oid,
                            result_type,
                            output,
                        )?;
                        Ok((source, count))
                    })?;
                let output_index = project.read_count()?;
                let project_output =
                    OutputId::from_plan_data(output_index, S1M_OUTPUT_COUNT).ok_or(
                        QueryPlanError::OutputOutOfBounds {
                            index: output_index,
                        },
                    )?;
                Ok((source, count, project_output))
            })?;
        QueryFragment::from_decoded_parts(source, count, project_output)
            .map_err(QueryPlanDataError::from)
    }

    fn decode_tuple_layout(
        reader: &mut PlanDataReader<'_>,
    ) -> Result<QueryTupleLayout, QueryPlanDataError> {
        QueryPlanDataError::expect_kind(
            "query tuple layout",
            reader.read_i32()?,
            QUERY_TUPLE_LAYOUT_KIND,
        )?;
        QueryPlanDataError::expect_version(reader.read_i32()?)?;
        let output_count = reader.read_count()?;
        if output_count != S1M_OUTPUT_COUNT {
            return Err(QueryPlanDataError::WrongOutputCount {
                found: output_count,
                expected: S1M_OUTPUT_COUNT,
            });
        }
        let output_index = reader.read_count()?;
        let output = OutputId::from_plan_data(output_index, output_count).ok_or(
            QueryPlanError::OutputOutOfBounds {
                index: output_index,
            },
        )?;
        let slot = QueryTupleSlot::from_plan_data(
            output,
            reader.read_oid()?,
            reader.read_i32()?,
            reader.read_oid()?,
        );
        QueryTupleLayout::from_decoded_slot(slot).map_err(QueryPlanDataError::from)
    }
}
