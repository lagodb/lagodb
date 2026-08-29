use super::*;

#[test]
fn planned_record_count_must_match_envelope() {
    assert_eq!(FilterRecordCodec::planned_count(2, 2).unwrap(), 2);
    assert!(matches!(
        FilterRecordCodec::planned_count(1, 2),
        Err(FilterRecordError::RecordCount {
            found: 1,
            expected: 2
        })
    ));
}

#[test]
fn binding_record_count_must_match_envelope() {
    assert_eq!(FilterRecordCodec::binding_count(3, 3).unwrap(), 3);
    assert!(matches!(
        FilterRecordCodec::binding_count(2, 3),
        Err(FilterRecordError::BindingRecordCount {
            found: 2,
            expected: 3
        })
    ));
}

#[test]
fn binding_range_must_stay_within_binding_records() {
    assert_eq!(FilterRecordCodec::binding_range(4, 2, 3, 5).unwrap(), 2..5);
    assert!(matches!(
        FilterRecordCodec::binding_range(4, 2, 4, 5),
        Err(FilterRecordError::BindingRangeOutOfBounds {
            record: 4,
            start: 2,
            end: 6,
            binding_count: 5
        })
    ));
}

#[test]
fn contract_tags_round_trip_and_unknown_tag_is_rejected() {
    for contract in [
        PushdownContract::ExactRowFilter,
        PushdownContract::ConservativePruning,
    ] {
        assert_eq!(
            FilterRecordCodec::contract_from_tag(
                0,
                FilterRecordCodec::contract_tag(contract),
            )
            .unwrap(),
            contract
        );
    }
    assert!(matches!(
        FilterRecordCodec::contract_from_tag(7, 99),
        Err(FilterRecordError::UnknownContract {
            record: 7,
            value: 99
        })
    ));
}

#[test]
fn costing_tags_round_trip_and_unknown_tag_is_rejected() {
    for costing in [
        PushdownCosting::CostedPruning,
        PushdownCosting::UncostedBestEffort,
    ] {
        assert_eq!(
            FilterRecordCodec::costing_from_tag(
                0,
                FilterRecordCodec::costing_tag(costing),
            )
            .unwrap(),
            costing
        );
    }
    assert!(matches!(
        FilterRecordCodec::costing_from_tag(8, 99),
        Err(FilterRecordError::UnknownCosting {
            record: 8,
            value: 99
        })
    ));
}

#[test]
fn value_source_tags_round_trip_and_unknown_tag_is_rejected() {
    for source in [
        FilterValueSourceKind::Constant,
        FilterValueSourceKind::ExternalParam,
        FilterValueSourceKind::ExecParam,
        FilterValueSourceKind::OuterValue,
    ] {
        assert_eq!(
            FilterRecordCodec::source_from_tag(
                0,
                FilterRecordCodec::source_tag(source),
            )
            .unwrap(),
            source
        );
    }
    assert!(matches!(
        FilterRecordCodec::source_from_tag(9, 99),
        Err(FilterRecordError::UnknownValueSource {
            binding: 9,
            value: 99
        })
    ));
}
