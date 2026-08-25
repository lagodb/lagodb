use std::rc::Rc;
use std::sync::Arc;

use iceberg_lite::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, Struct,
};

use crate::engine::write::{
    EffectiveCommitAction, TxTableActionLog as SharedActionLog,
};
use crate::managed_table::maintenance::PreparedVacuum;

type TxTableActionLog = SharedActionLog<PreparedVacuum, String>;

#[test]
fn combined_delta_keeps_only_data_after_last_truncate() {
    let mut actions = TxTableActionLog::default();
    actions
        .record_data_files(vec![data_file("before.parquet")])
        .unwrap();
    actions.stage_truncate("metadata-v1.json".to_owned());
    actions
        .record_data_files(vec![data_file("after.parquet")])
        .unwrap();

    let delta = actions.combined_delta().unwrap().unwrap();
    assert!(delta.truncates_base());
    assert_eq!(
        delta
            .added_data_files()
            .iter()
            .map(DataFile::file_path)
            .collect::<Vec<_>>(),
        vec!["after.parquet"]
    );
    assert_eq!(
        actions.commit_plan().unwrap().canceled_created_paths,
        vec!["before.parquet".to_owned()]
    );
    let plan = actions.commit_plan().unwrap();
    assert!(matches!(
        plan.actions.as_slice(),
        [EffectiveCommitAction::Data {
            truncate_base: true,
            ..
        }]
    ));
}

#[test]
fn commit_plan_preserves_data_only_epochs_without_truncate() {
    let mut actions = TxTableActionLog::default();
    actions
        .record_data_files(vec![data_file("data.parquet")])
        .unwrap();

    let plan = actions.commit_plan().unwrap();
    assert!(plan.truncate_guard.is_none());
    assert!(plan.canceled_created_paths.is_empty());
    assert!(matches!(
        plan.actions.as_slice(),
        [EffectiveCommitAction::Data {
            truncate_base: false,
            ..
        }]
    ));
}

#[test]
fn commit_plan_replaces_pre_truncate_data_with_truncate_only() {
    let mut actions = TxTableActionLog::default();
    actions
        .record_data_files(vec![data_file("discarded.parquet")])
        .unwrap();
    actions.stage_truncate("metadata-v1.json".to_owned());

    let plan = actions.commit_plan().unwrap();
    assert_eq!(
        plan.truncate_guard.map(String::as_str),
        Some("metadata-v1.json")
    );
    assert_eq!(
        plan.canceled_created_paths,
        vec!["discarded.parquet".to_owned()]
    );
    assert!(matches!(
        plan.actions.as_slice(),
        [EffectiveCommitAction::TruncateOnly]
    ));
}

#[test]
fn combined_delta_is_shared_until_the_action_log_changes() {
    let mut actions = TxTableActionLog::default();
    actions
        .record_data_files(vec![data_file("first.parquet")])
        .unwrap();

    let first = actions.combined_delta().unwrap().unwrap();
    let cached = actions.combined_delta().unwrap().unwrap();
    assert!(Arc::ptr_eq(&first, &cached));

    actions
        .record_data_files(vec![data_file("second.parquet")])
        .unwrap();
    let rebuilt = actions.combined_delta().unwrap().unwrap();
    assert!(!Arc::ptr_eq(&first, &rebuilt));
    assert!(rebuilt.has_live_added_data_file_path("second.parquet"));
}

#[test]
fn populated_combined_delta_cache_is_invalidated_by_savepoint_rollback() {
    let mut actions = TxTableActionLog::default();
    actions
        .record_data_files(vec![data_file("before.parquet")])
        .unwrap();
    let marker = actions.mark();
    let before = actions.combined_delta().unwrap().unwrap();

    actions.stage_truncate("metadata-v1.json".to_owned());
    let truncated = actions.combined_delta().unwrap().unwrap();
    assert!(truncated.truncates_base());
    assert!(!Arc::ptr_eq(&before, &truncated));

    actions.truncate(marker);
    let restored = actions.combined_delta().unwrap().unwrap();
    assert!(!restored.truncates_base());
    assert!(restored.has_live_added_data_file_path("before.parquet"));
    assert!(!Arc::ptr_eq(&truncated, &restored));
}

#[test]
fn shared_action_log_snapshot_is_copy_on_write() {
    let mut current = Rc::new(TxTableActionLog::default());
    Rc::make_mut(&mut current)
        .record_data_files(vec![data_file("before.parquet")])
        .unwrap();
    let commit_snapshot = Rc::clone(&current);

    Rc::make_mut(&mut current)
        .record_data_files(vec![data_file("after.parquet")])
        .unwrap();

    let snapshotted_delta = commit_snapshot.combined_delta().unwrap().unwrap();
    assert!(snapshotted_delta.has_live_added_data_file_path("before.parquet"));
    assert!(!snapshotted_delta.has_live_added_data_file_path("after.parquet"));
    let current_delta = current.combined_delta().unwrap().unwrap();
    assert!(current_delta.has_live_added_data_file_path("after.parquet"));
}

#[test]
fn last_truncate_controls_baseline_and_canceled_regions() {
    let mut actions = TxTableActionLog::default();
    actions.stage_truncate("metadata-v1.json".to_owned());
    actions
        .record_data_files(vec![data_file("middle.parquet")])
        .unwrap();
    actions.stage_truncate("metadata-v2.json".to_owned());
    actions
        .record_data_files(vec![data_file("after.parquet")])
        .unwrap();

    let (index, truncate) = actions.last_truncate().unwrap();
    assert_eq!(index, 2);
    assert_eq!(truncate.guard, "metadata-v2.json");
    assert_eq!(
        actions.commit_plan().unwrap().canceled_created_paths,
        vec!["middle.parquet".to_owned()]
    );
}

#[test]
fn action_log_marker_rolls_back_truncate_and_later_data() {
    let mut actions = TxTableActionLog::default();
    actions
        .record_data_files(vec![data_file("before.parquet")])
        .unwrap();
    let marker = actions.mark();
    actions.stage_truncate("metadata-v1.json".to_owned());
    actions
        .record_data_files(vec![data_file("after.parquet")])
        .unwrap();

    actions.truncate(marker);

    assert!(actions.last_truncate().is_none());
    let delta = actions.combined_delta().unwrap().unwrap();
    assert!(!delta.truncates_base());
    assert!(delta.has_live_added_data_file_path("before.parquet"));
    assert!(!delta.has_live_added_data_file_path("after.parquet"));
}

#[test]
fn drop_suppresses_commit_but_marker_can_restore_actions() {
    let mut actions = TxTableActionLog::default();
    actions.stage_truncate("metadata-v1.json".to_owned());
    let marker = actions.mark();
    actions.stage_drop();

    assert!(actions.is_dropped());
    assert!(actions.combined_delta().unwrap().is_none());

    actions.truncate(marker);
    assert!(!actions.is_dropped());
    assert!(actions.combined_delta().unwrap().unwrap().truncates_base());
}

fn data_file(path: &str) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(path.to_owned())
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .partition_spec_id(0)
        .record_count(1)
        .file_size_in_bytes(100)
        .build()
        .unwrap()
}
