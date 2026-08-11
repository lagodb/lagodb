//! Shared planned-filter and value-slot records for CustomScan and FDW plans.

use core::marker::PhantomData;
use core::ptr;

use pgrx::pg_sys;

use crate::expr::contract::{PushdownContract, PushdownCosting};
use crate::plan_data::{PlanDataError, PlanDataReader, PlanDataWriter};

use super::{
    FilterPushdown, FilterValueSlot, FilterValueSourceKind, NegotiatedFilterSet,
    PlannedFilterRecord,
};

const CONTRACT_EXACT: i32 = 0;
const CONTRACT_CONSERVATIVE: i32 = 1;
const COSTING_COSTED: i32 = 0;
const COSTING_UNCOSTED: i32 = 1;

const SOURCE_CONSTANT: i32 = 0;
const SOURCE_EXTERNAL_PARAM: i32 = 1;
const SOURCE_EXEC_PARAM: i32 = 2;
const SOURCE_OUTER_VALUE: i32 = 3;

pub(crate) struct EncodedFilterData {
    pub planned: *mut pg_sys::List,
    pub bindings: *mut pg_sys::List,
}

type DecodedFilterData<P> = (Vec<PlannedFilterRecord<P>>, Vec<FilterValueSlot>);

#[derive(Debug, thiserror::Error)]
pub(crate) enum FilterDataError<E> {
    #[error("planned-filter plan-data codec failed: {0}")]
    PlanData(#[from] PlanDataError),
    #[error("provider planned-filter codec failed: {0}")]
    Provider(E),
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
    #[error("record {record} binding range overflows usize")]
    BindingRangeOverflow { record: usize },
    #[error(
        "record {record} binding range {start}..{end} exceeds binding count {binding_count}"
    )]
    BindingRangeOutOfBounds {
        record: usize,
        start: usize,
        end: usize,
        binding_count: usize,
    },
    #[error("binding {binding} has unknown source tag {value}")]
    UnknownValueSource { binding: usize, value: i32 },
}

pub(crate) struct FilterDataCodec<P>(PhantomData<fn() -> P>);

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
                        .append_i32(Self::contract_tag(filter.effective.contract))
                        .append_i32(Self::costing_tag(filter.effective.costing))
                        .append_count(filter.binding_start)
                        .append_count(filter.binding_count);
                    unsafe { writer.append_list(payload) };
                    Ok(())
                })?;
            planned = unsafe { pg_sys::lappend(planned, record.cast()) };
        }

        let mut bindings = ptr::null_mut();
        for binding in &filters.bindings {
            let record =
                PlanDataWriter::encode_list::<FilterDataError<P::Error>>(|writer| {
                    writer
                        .append_oid(binding.metadata.value_type.type_oid)
                        .append_i32(binding.metadata.value_type.typmod)
                        .append_oid(binding.metadata.value_type.collation)
                        .append_i32(Self::source_tag(binding.metadata.source_kind));
                    Ok(())
                })?;
            bindings = unsafe { pg_sys::lappend(bindings, record.cast()) };
        }

        Ok(EncodedFilterData { planned, bindings })
    }

    /// # Safety
    ///
    /// Both lists must be plan-owned `T_List` nodes (or NIL) that remain live
    /// for the duration of provider decoding.
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
            return if expected_count == 0 {
                Ok(Vec::new())
            } else {
                Err(FilterRecordError::RecordCount {
                    found: 0,
                    expected: expected_count,
                }
                .into())
            };
        }

        unsafe {
            PlanDataReader::decode_checked_list::<_, FilterDataError<P::Error>>(
                raw,
                0,
                |records| {
                    let found = records.remaining();
                    if found != expected_count {
                        return Err(FilterRecordError::RecordCount {
                            found,
                            expected: expected_count,
                        }
                        .into());
                    }
                    let mut filters = Vec::with_capacity(found);
                    for record_index in 0..found {
                        filters.push(
                            records.read_nested::<_, FilterDataError<P::Error>>(
                                |record| {
                                    let contract = Self::contract_from_tag(
                                        record_index,
                                        record.read_i32()?,
                                    )?;
                                    Self::costing_from_tag(
                                        record_index,
                                        record.read_i32()?,
                                    )?;
                                    let start = record.read_count()?;
                                    let count = record.read_count()?;
                                    let end = start.checked_add(count).ok_or(
                                        FilterRecordError::BindingRangeOverflow {
                                            record: record_index,
                                        },
                                    )?;
                                    if end > binding_count {
                                        return Err(
                                        FilterRecordError::BindingRangeOutOfBounds {
                                            record: record_index,
                                            start,
                                            end,
                                            binding_count,
                                        }
                                        .into(),
                                    );
                                    }
                                    let planned = record.read_nested(|payload| {
                                        P::decode_planned(payload, count)
                                            .map_err(FilterDataError::Provider)
                                    })?;
                                    Ok(PlannedFilterRecord {
                                        planned,
                                        contract,
                                        binding_range: start..end,
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
    ) -> Result<Vec<FilterValueSlot>, FilterDataError<P::Error>> {
        if raw.is_null() {
            return if expected_count == 0 {
                Ok(Vec::new())
            } else {
                Err(FilterRecordError::BindingRecordCount {
                    found: 0,
                    expected: expected_count,
                }
                .into())
            };
        }

        unsafe {
            PlanDataReader::decode_checked_list::<_, FilterDataError<P::Error>>(
                raw,
                0,
                |records| {
                    let count = records.remaining();
                    if count != expected_count {
                        return Err(FilterRecordError::BindingRecordCount {
                            found: count,
                            expected: expected_count,
                        }
                        .into());
                    }
                    let mut bindings = Vec::with_capacity(count);
                    for binding_index in 0..count {
                        bindings.push(
                            records.read_nested::<_, FilterDataError<P::Error>>(
                                |record| {
                                    let type_oid = record.read_oid()?;
                                    let typmod = record.read_i32()?;
                                    let collation = record.read_oid()?;
                                    let source = Self::source_from_tag(
                                        binding_index,
                                        record.read_i32()?,
                                    )?;
                                    Ok(FilterValueSlot {
                                        value_type: super::FilterTypeMetadata {
                                            type_oid,
                                            typmod,
                                            collation,
                                        },
                                        source_kind: source,
                                    })
                                },
                            )?,
                        );
                    }
                    Ok(bindings)
                },
            )
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

    fn source_tag(source: FilterValueSourceKind) -> i32 {
        match source {
            FilterValueSourceKind::Constant => SOURCE_CONSTANT,
            FilterValueSourceKind::ExternalParam => SOURCE_EXTERNAL_PARAM,
            FilterValueSourceKind::ExecParam => SOURCE_EXEC_PARAM,
            FilterValueSourceKind::OuterValue => SOURCE_OUTER_VALUE,
        }
    }

    fn source_from_tag(
        binding: usize,
        value: i32,
    ) -> Result<FilterValueSourceKind, FilterRecordError> {
        match value {
            SOURCE_CONSTANT => Ok(FilterValueSourceKind::Constant),
            SOURCE_EXTERNAL_PARAM => Ok(FilterValueSourceKind::ExternalParam),
            SOURCE_EXEC_PARAM => Ok(FilterValueSourceKind::ExecParam),
            SOURCE_OUTER_VALUE => Ok(FilterValueSourceKind::OuterValue),
            value => Err(FilterRecordError::UnknownValueSource { binding, value }),
        }
    }
}
