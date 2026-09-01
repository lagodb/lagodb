//! Selected-path envelope combining engine semantics with one opaque source.

use std::marker::PhantomData;

use lagodb_core::plan_data::{PlanDataError, PlanDataReader, PlanDataWriter};
use lagodb_core::query_contract::{
    ProviderId, SourceEstimate, SourceEstimateError, SourceId,
};
use pgrx::pg_sys;

use crate::{ExecutionProfile, ExecutionProfileError};

use super::{QueryPlanData, QueryPlanDataError};

const QUERY_ENVELOPE_KIND: i32 = 0x4c51_1001;
const QUERY_ENVELOPE_VERSION: i32 = 3;
const S1M_SOURCE_COUNT: usize = 1;

/// One decoded provider source whose opaque payload remains PostgreSQL-owned.
pub struct DecodedQuerySource<'plan> {
    provider: ProviderId,
    source: SourceId,
    range_table_index: pg_sys::Index,
    estimate: SourceEstimate,
    provider_plan: *mut pg_sys::List,
    _plan: PhantomData<&'plan pg_sys::List>,
}

impl DecodedQuerySource<'_> {
    #[inline]
    pub const fn provider(&self) -> ProviderId {
        self.provider
    }

    #[inline]
    pub const fn source(&self) -> SourceId {
        self.source
    }

    #[inline]
    pub const fn range_table_index(&self) -> pg_sys::Index {
        self.range_table_index
    }

    #[inline]
    pub const fn estimate(&self) -> SourceEstimate {
        self.estimate
    }

    #[inline]
    pub const fn provider_plan(&self) -> *mut pg_sys::List {
        self.provider_plan
    }
}

/// Complete S1M selected-path plan decoded at executor Begin.
pub struct QueryPlanEnvelope<'plan> {
    query: QueryPlanData,
    execution: ExecutionProfile,
    source: DecodedQuerySource<'plan>,
}

impl<'plan> QueryPlanEnvelope<'plan> {
    /// Encode the engine plan and one provider-owned source frame.
    ///
    /// # Safety
    ///
    /// `provider_plan` must be a live, non-NIL, `copyObject`-safe PostgreSQL
    /// `T_List` in the current planner memory context.
    pub unsafe fn encode(
        query: &QueryPlanData,
        execution: ExecutionProfile,
        provider: ProviderId,
        source: SourceId,
        range_table_index: pg_sys::Index,
        estimate: SourceEstimate,
        provider_plan: *mut pg_sys::List,
    ) -> Result<*mut pg_sys::List, QueryPlanEnvelopeError> {
        if provider_plan.is_null() {
            return Err(PlanDataError::NullList.into());
        }
        if range_table_index == 0 {
            return Err(QueryPlanEnvelopeError::InvalidRangeTableIndex);
        }
        let query_plan = query.encode()?;
        PlanDataWriter::encode_list(|writer| {
            writer
                .append_i32(QUERY_ENVELOPE_KIND)
                .append_i32(QUERY_ENVELOPE_VERSION)
                .append_count(execution.maximum_batch_rows().get())
                .append_count(provider.index())
                .append_count(source.index())
                .append_count(range_table_index as usize);
            writer
                .append_i64(estimate.estimated_rows().to_bits() as i64)
                .append_i64(estimate.estimated_scan_bytes().to_bits() as i64);
            unsafe {
                writer
                    .append_encoded_list(query_plan)
                    .append_encoded_list(provider_plan);
            }
            Ok(())
        })
    }

    /// Decode the complete selected-path payload and reject trailing fields.
    ///
    /// # Safety
    ///
    /// `list` must point to a live PostgreSQL plan-data `T_List` for `'plan`.
    pub unsafe fn decode(
        list: *mut pg_sys::List,
    ) -> Result<Self, QueryPlanEnvelopeError> {
        let decode = |reader: &mut PlanDataReader<'_>| {
            let kind = reader.read_i32()?;
            if kind != QUERY_ENVELOPE_KIND {
                return Err(QueryPlanEnvelopeError::WrongKind {
                    found: kind,
                    expected: QUERY_ENVELOPE_KIND,
                });
            }
            let version = reader.read_i32()?;
            if version != QUERY_ENVELOPE_VERSION {
                return Err(QueryPlanEnvelopeError::WrongVersion {
                    found: version,
                    expected: QUERY_ENVELOPE_VERSION,
                });
            }
            let execution = ExecutionProfile::try_new(reader.read_count()?)?;
            let provider = ProviderId::from_index(reader.read_count()?);
            let source_index = reader.read_count()?;
            let source = SourceId::from_plan_data(source_index, S1M_SOURCE_COUNT)
                .ok_or(QueryPlanEnvelopeError::SourceOutOfBounds {
                    index: source_index,
                })?;
            let range_table_index = pg_sys::Index::try_from(reader.read_count()?)
                .map_err(|_| QueryPlanEnvelopeError::InvalidRangeTableIndex)?;
            if range_table_index == 0 {
                return Err(QueryPlanEnvelopeError::InvalidRangeTableIndex);
            }
            let estimate = SourceEstimate::try_new(
                f64::from_bits(reader.read_i64()? as u64),
                f64::from_bits(reader.read_i64()? as u64),
            )?;
            let query_plan = reader.read_encoded_list()?;
            let provider_plan = reader.read_encoded_list()?;
            // SAFETY: `read_encoded_list` returned a checked nested List
            // borrowed from the live envelope supplied to this decode.
            let query = unsafe { QueryPlanData::decode(query_plan) }?;
            let fragment_source = query.fragment().scalar_count_source();
            if fragment_source != source {
                return Err(QueryPlanEnvelopeError::SourceMismatch {
                    envelope: source.index(),
                    fragment: fragment_source.index(),
                });
            }
            Ok(QueryPlanEnvelope {
                query,
                execution,
                source: DecodedQuerySource {
                    provider,
                    source,
                    range_table_index,
                    estimate,
                    provider_plan,
                    _plan: PhantomData,
                },
            })
        };
        // SAFETY: the caller guarantees that `list` is a live PostgreSQL
        // plan-data List for the duration of this synchronous decode.
        unsafe { PlanDataReader::decode_checked_list(list, 0, decode) }
    }

    #[inline]
    pub const fn query(&self) -> &QueryPlanData {
        &self.query
    }

    #[inline]
    pub const fn execution(&self) -> ExecutionProfile {
        self.execution
    }

    #[inline]
    pub const fn source(&self) -> &DecodedQuerySource<'plan> {
        &self.source
    }

    pub fn into_parts(
        self,
    ) -> (QueryPlanData, ExecutionProfile, DecodedQuerySource<'plan>) {
        (self.query, self.execution, self.source)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum QueryPlanEnvelopeError {
    #[error("query envelope plan-data primitive failed: {0}")]
    PlanData(#[from] PlanDataError),
    #[error("query engine plan is invalid: {0}")]
    QueryPlan(#[from] QueryPlanDataError),
    #[error("query envelope has kind {found}, expected {expected}")]
    WrongKind { found: i32, expected: i32 },
    #[error("query envelope version {found} is unsupported; expected {expected}")]
    WrongVersion { found: i32, expected: i32 },
    #[error("query source identity {index} is outside the S1M source table")]
    SourceOutOfBounds { index: usize },
    #[error("query source range-table index is invalid")]
    InvalidRangeTableIndex,
    #[error(
        "query envelope source {envelope} differs from fragment source {fragment}"
    )]
    SourceMismatch { envelope: usize, fragment: usize },
    #[error("query source estimate is invalid: {0}")]
    InvalidSourceEstimate(#[from] SourceEstimateError),
    #[error("query execution profile is invalid: {0}")]
    InvalidExecutionProfile(#[from] ExecutionProfileError),
}
