//! Shared planned-filter and value-slot records for CustomScan and FDW plans.

use core::marker::PhantomData;
use core::ops::Range;
use core::ptr;

use pgrx::pg_sys;

use crate::expr::contract::{PushdownContract, PushdownCosting};
use crate::expr::{
    ExpressionCodecError, ExpressionPlanDataDecode, ExpressionPlanDataEncode,
    RuntimeValueLayout, RuntimeValueSpec,
};
use crate::plan_data::{PlanDataError, PlanDataReader, PlanDataWriter};

use super::{FilterPushdown, NegotiatedFilterSet, PlannedFilterRecord};

const CONTRACT_EXACT: i32 = 0;
const CONTRACT_CONSERVATIVE: i32 = 1;
const COSTING_COSTED: i32 = 0;
const COSTING_UNCOSTED: i32 = 1;

pub(crate) struct EncodedFilterData {
    pub planned: *mut pg_sys::List,
    pub bindings: *mut pg_sys::List,
}

type DecodedFilterData<P> = (Vec<PlannedFilterRecord<P>>, Vec<RuntimeValueSpec>);

#[derive(Debug, thiserror::Error)]
pub(crate) enum FilterDataError<E> {
    #[error("planned-filter plan-data codec failed: {0}")]
    PlanData(#[from] PlanDataError),
    #[error("provider planned-filter codec failed: {0}")]
    Provider(E),
    #[error("shared expression codec failed: {0}")]
    Expression(#[from] ExpressionCodecError),
    #[error("invalid planned-filter record: {0}")]
    Invalid(#[from] FilterRecordError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum FilterRecordError {
    #[error("record count is {found}, expected {expected}")]
    RecordCount { found: usize, expected: usize },
    #[error("binding record count is {found}, expected {expected}")]
    BindingRecordCount { found: usize, expected: usize },
    #[error("record {record} has unknown contract tag {value}")]
    UnknownContract { record: usize, value: i32 },
    #[error("record {record} has unknown costing tag {value}")]
    UnknownCosting { record: usize, value: i32 },
    #[error(
        "record {record} binding range {start}..{end} exceeds binding count {binding_count}"
    )]
    BindingRangeOutOfBounds {
        record: usize,
        start: usize,
        end: usize,
        binding_count: usize,
    },
}

pub(crate) struct FilterDataCodec<P>(PhantomData<fn() -> P>);

struct FilterRecordCodec;

impl FilterRecordCodec {
    fn planned_count(
        found: usize,
        expected: usize,
    ) -> Result<usize, FilterRecordError> {
        if found == expected {
            Ok(found)
        } else {
            Err(FilterRecordError::RecordCount { found, expected })
        }
    }

    fn binding_count(
        found: usize,
        expected: usize,
    ) -> Result<usize, FilterRecordError> {
        if found == expected {
            Ok(found)
        } else {
            Err(FilterRecordError::BindingRecordCount { found, expected })
        }
    }

    fn binding_range(
        record: usize,
        start: usize,
        count: usize,
        binding_count: usize,
    ) -> Result<Range<usize>, FilterRecordError> {
        // Both operands came from PlanDataReader::read_count(), whose source is
        // a non-negative i32. Their sum fits usize on every PostgreSQL target.
        let end = start + count;
        if end <= binding_count {
            Ok(start..end)
        } else {
            Err(FilterRecordError::BindingRangeOutOfBounds {
                record,
                start,
                end,
                binding_count,
            })
        }
    }

    fn contract_tag(contract: PushdownContract) -> i32 {
        match contract {
            PushdownContract::ExactRowFilter => CONTRACT_EXACT,
            PushdownContract::ConservativePruning => CONTRACT_CONSERVATIVE,
        }
    }

    fn contract_from_tag(
        record: usize,
        value: i32,
    ) -> Result<PushdownContract, FilterRecordError> {
        match value {
            CONTRACT_EXACT => Ok(PushdownContract::ExactRowFilter),
            CONTRACT_CONSERVATIVE => Ok(PushdownContract::ConservativePruning),
            value => Err(FilterRecordError::UnknownContract { record, value }),
        }
    }

    fn costing_tag(costing: PushdownCosting) -> i32 {
        match costing {
            PushdownCosting::CostedPruning => COSTING_COSTED,
            PushdownCosting::UncostedBestEffort => COSTING_UNCOSTED,
        }
    }

    fn costing_from_tag(
        record: usize,
        value: i32,
    ) -> Result<PushdownCosting, FilterRecordError> {
        match value {
            COSTING_COSTED => Ok(PushdownCosting::CostedPruning),
            COSTING_UNCOSTED => Ok(PushdownCosting::UncostedBestEffort),
            value => Err(FilterRecordError::UnknownCosting { record, value }),
        }
    }
}

impl<P: FilterPushdown> FilterDataCodec<P> {
    pub(crate) fn encode(
        filters: &NegotiatedFilterSet<P::PlannedPredicate>,
    ) -> Result<EncodedFilterData, FilterDataError<P::Error>> {
        let mut planned = ptr::null_mut();
        for filter in &filters.planned {
            let payload =
                PlanDataWriter::encode_list::<FilterDataError<P::Error>>(|writer| {
                    P::encode_planned(&filter.planned, writer)
                        .map_err(FilterDataError::Provider)
                })?;

            let record =
                PlanDataWriter::encode_list::<FilterDataError<P::Error>>(|writer| {
                    writer
                        .append_i32(FilterRecordCodec::contract_tag(
                            filter.effective.contract,
                        ))
                        .append_i32(FilterRecordCodec::costing_tag(
                            filter.effective.costing,
                        ))
                        .append_count(filter.binding_start)
                        .append_count(filter.binding_count);
                    unsafe { writer.append_list(payload) };
                    Ok(())
                })?;
            planned = unsafe { pg_sys::lappend(planned, record.cast()) };
        }

        let binding_layout = RuntimeValueLayout::new(
            filters
                .bindings
                .iter()
                .map(|binding| binding.metadata)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        );
        let bindings =
            PlanDataWriter::encode_list::<FilterDataError<P::Error>>(|writer| {
                binding_layout.encode_plan_data(writer);
                Ok(())
            })?;

        Ok(EncodedFilterData { planned, bindings })
    }

    /// `planned_raw` is an unframed record list, so NIL is its canonical empty
    /// encoding. `bindings_raw` is a counted [`RuntimeValueLayout`] frame; its
    /// canonical empty encoding contains the zero count and NIL is rejected as
    /// a missing frame even when `expected_binding_count` is zero.
    ///
    /// # Safety
    ///
    /// Every non-NIL list must be a plan-owned `T_List` that remains live for
    /// the duration of provider decoding.
    pub(crate) unsafe fn decode(
        planned_raw: *mut pg_sys::List,
        expected_count: usize,
        bindings_raw: *mut pg_sys::List,
        expected_binding_count: usize,
    ) -> Result<DecodedFilterData<P::PlannedPredicate>, FilterDataError<P::Error>>
    {
        let bindings =
            unsafe { Self::decode_bindings(bindings_raw, expected_binding_count) }?;
        let planned = unsafe {
            Self::decode_planned(planned_raw, expected_count, bindings.len())
        }?;
        Ok((planned, bindings))
    }

    unsafe fn decode_planned(
        raw: *mut pg_sys::List,
        expected_count: usize,
        binding_count: usize,
    ) -> Result<
        Vec<PlannedFilterRecord<P::PlannedPredicate>>,
        FilterDataError<P::Error>,
    > {
        if raw.is_null() {
            FilterRecordCodec::planned_count(0, expected_count)?;
            return Ok(Vec::new());
        }

        unsafe {
            PlanDataReader::decode_checked_list::<_, FilterDataError<P::Error>>(
                raw,
                0,
                |records| {
                    let found = FilterRecordCodec::planned_count(
                        records.remaining(),
                        expected_count,
                    )?;
                    let mut filters = Vec::with_capacity(found);
                    for record_index in 0..found {
                        filters.push(
                            records.read_nested::<_, FilterDataError<P::Error>>(
                                |record| {
                                    let contract =
                                        FilterRecordCodec::contract_from_tag(
                                            record_index,
                                            record.read_i32()?,
                                        )?;
                                    FilterRecordCodec::costing_from_tag(
                                        record_index,
                                        record.read_i32()?,
                                    )?;
                                    let start = record.read_count()?;
                                    let count = record.read_count()?;
                                    let binding_range =
                                        FilterRecordCodec::binding_range(
                                            record_index,
                                            start,
                                            count,
                                            binding_count,
                                        )?;
                                    let planned = record.read_nested(|payload| {
                                        P::decode_planned(payload, count)
                                            .map_err(FilterDataError::Provider)
                                    })?;
                                    Ok(PlannedFilterRecord {
                                        planned,
                                        contract,
                                        binding_range,
                                    })
                                },
                            )?,
                        );
                    }
                    Ok(filters)
                },
            )
        }
    }

    unsafe fn decode_bindings(
        raw: *mut pg_sys::List,
        expected_count: usize,
    ) -> Result<Vec<RuntimeValueSpec>, FilterDataError<P::Error>> {
        let layout = unsafe {
            PlanDataReader::decode_checked_list::<_, FilterDataError<P::Error>>(
                raw,
                0,
                |reader| Ok(RuntimeValueLayout::decode_plan_data(reader, ())?),
            )
        }?;
        FilterRecordCodec::binding_count(layout.len(), expected_count)?;
        Ok(layout.values().to_vec())
    }
}

#[cfg(test)]
mod test;
