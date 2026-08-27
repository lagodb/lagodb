//! Planned-filter facet for the backend-test FDW provider.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use lagodb_core::diag::SqlStateError;
use lagodb_core::expr::PushdownCosting;
use lagodb_core::expr::pushdown::{
    FilterBindResult, FilterFragment, FilterNode, FilterPlan, FilterPlanningContext,
    FilterPushdown, FilterPushdownPlanner, FilterScalar, FilterValueBindings,
    FilterValueSlotId,
};
use lagodb_core::plan_data::{PlanDataError, PlanDataReader, PlanDataWriter};
use pgrx::FromDatum;
use pgrx::pg_sys;
use pgrx::prelude::{PgSqlErrorCode, PgSqlErrorCode::ERRCODE_INTERNAL_ERROR};

use super::fixture::TestRow;
use super::provider::FrameworkTestFdw;

const INT4EQ_OPNO: u32 = 96;

#[derive(Clone, Copy, Debug)]
pub struct RuntimeFilter {
    attno: pg_sys::AttrNumber,
    value: Option<i32>,
}

impl RuntimeFilter {
    pub(super) fn matches(self, row: &TestRow) -> bool {
        self.value
            .is_some_and(|value| row.int4_value(self.attno) == Some(value))
    }

    pub(super) const fn trace(self) -> (pg_sys::AttrNumber, Option<i32>) {
        (self.attno, self.value)
    }
}

#[derive(Debug)]
pub enum TestFilterError {
    Codec(PlanDataError),
    InvalidSlot { index: usize },
    InvalidAttno { value: i32 },
    InvalidDatum,
}

impl From<PlanDataError> for TestFilterError {
    fn from(error: PlanDataError) -> Self {
        Self::Codec(error)
    }
}

impl Display for TestFilterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(error) => Display::fmt(error, formatter),
            Self::InvalidSlot { index } => {
                write!(formatter, "test FDW filter references invalid slot {index}")
            }
            Self::InvalidAttno { value } => {
                write!(formatter, "test FDW filter has invalid attno {value}")
            }
            Self::InvalidDatum => {
                formatter.write_str("test FDW filter datum is not int4")
            }
        }
    }
}

impl Error for TestFilterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Codec(error) => Some(error),
            Self::InvalidSlot { .. }
            | Self::InvalidAttno { .. }
            | Self::InvalidDatum => None,
        }
    }
}

impl SqlStateError for TestFilterError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        ERRCODE_INTERNAL_ERROR
    }
}

pub struct TestFilterPlanner;

#[derive(Debug)]
pub struct PlannedTestFilter {
    attno: pg_sys::AttrNumber,
    value: FilterValueSlotId,
}

impl PlannedTestFilter {
    pub(super) const fn attno(&self) -> pg_sys::AttrNumber {
        self.attno
    }
}

impl FilterPushdownPlanner for TestFilterPlanner {
    type PlannedPredicate = PlannedTestFilter;
    type Error = TestFilterError;

    fn try_plan_filter(
        &mut self,
        fragment: &FilterFragment,
    ) -> Result<FilterPlan<Self::PlannedPredicate>, Self::Error> {
        let FilterNode::Comparison {
            operator,
            left,
            right,
        } = fragment.root()
        else {
            return Ok(FilterPlan::Unsupported);
        };
        let (column, value) = match (left, right) {
            (FilterScalar::Column(column), FilterScalar::Value(value))
            | (FilterScalar::Value(value), FilterScalar::Column(column)) => {
                (column, *value)
            }
            _ => return Ok(FilterPlan::Unsupported),
        };
        if column.declared_type.type_oid != pg_sys::INT4OID
            || column.value_type.type_oid != pg_sys::INT4OID
            || !matches!(column.attno, 1 | 2)
            || operator.opno != pg_sys::Oid::from(INT4EQ_OPNO)
        {
            return Ok(FilterPlan::Unsupported);
        }
        let predicate = PlannedTestFilter {
            attno: column.attno,
            value,
        };
        if column.attno == 2 {
            Ok(FilterPlan::conservative(
                predicate,
                PushdownCosting::CostedPruning,
            ))
        } else {
            Ok(FilterPlan::exact(predicate, PushdownCosting::CostedPruning))
        }
    }
}

impl FilterPushdown for FrameworkTestFdw {
    type Planner = TestFilterPlanner;
    type PlannedPredicate = PlannedTestFilter;
    type BoundPredicate = RuntimeFilter;
    type Error = TestFilterError;

    fn begin_filter_planning(
        _context: &FilterPlanningContext,
    ) -> Result<Self::Planner, Self::Error> {
        Ok(TestFilterPlanner)
    }

    fn encode_planned(
        predicate: &Self::PlannedPredicate,
        writer: &mut PlanDataWriter,
    ) -> Result<(), Self::Error> {
        writer
            .append_i32(predicate.attno as i32)
            .append_count(predicate.value.index());
        Ok(())
    }

    fn decode_planned(
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
    ) -> Result<Self::PlannedPredicate, Self::Error> {
        let raw_attno = reader.read_i32()?;
        let attno = pg_sys::AttrNumber::try_from(raw_attno)
            .map_err(|_| TestFilterError::InvalidAttno { value: raw_attno })?;
        if !matches!(attno, 1 | 2) {
            return Err(TestFilterError::InvalidAttno { value: raw_attno });
        }
        let index = reader.read_count()?;
        let value = FilterValueSlotId::from_plan_data(index, binding_count)
            .ok_or(TestFilterError::InvalidSlot { index })?;
        Ok(PlannedTestFilter { attno, value })
    }

    fn bind_filter(
        predicate: &Self::PlannedPredicate,
        values: FilterValueBindings<'_>,
    ) -> Result<FilterBindResult<Self::BoundPredicate>, Self::Error> {
        let value = values.value(predicate.value);
        let value = if value.is_null() {
            None
        } else {
            Some(
                unsafe { i32::from_datum(value.datum(), false) }
                    .ok_or(TestFilterError::InvalidDatum)?,
            )
        };
        Ok(FilterBindResult::Bound(RuntimeFilter {
            attno: predicate.attno,
            value,
        }))
    }
}
