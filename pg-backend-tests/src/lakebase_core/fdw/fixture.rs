use core::num::NonZeroU16;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use pg_lakebase_core::handles::ValidItemPointer;
use pgrx::pg_sys;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct TestRow {
    pub(super) id: i32,
    pub(super) sort_key: i32,
    pub(super) payload: String,
}

impl TestRow {
    pub(super) fn item_pointer(&self) -> ValidItemPointer {
        let offset = (self.id + 1)
            .try_into()
            .ok()
            .and_then(NonZeroU16::new)
            .expect("test row id must be a valid ItemPointer offset");
        ValidItemPointer::new(1, offset)
    }

    pub(super) fn int4_value(&self, attno: pg_sys::AttrNumber) -> Option<i32> {
        match attno {
            1 => Some(self.id),
            2 => Some(self.sort_key),
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
struct StoreData {
    tables: HashMap<pg_sys::Oid, Vec<TestRow>>,
}

impl StoreData {
    fn table_mut(&mut self, relation_oid: pg_sys::Oid) -> &mut Vec<TestRow> {
        self.tables
            .entry(relation_oid)
            .or_insert_with(Self::default_rows)
    }

    fn default_rows() -> Vec<TestRow> {
        vec![
            TestRow {
                id: 1,
                sort_key: 30,
                payload: "zulu".to_owned(),
            },
            TestRow {
                id: 2,
                sort_key: 10,
                payload: "alpha".to_owned(),
            },
            TestRow {
                id: 3,
                sort_key: 20,
                payload: "mike".to_owned(),
            },
        ]
    }
}

pub(super) struct TestStore;

impl TestStore {
    fn global() -> &'static Mutex<StoreData> {
        static STORE: OnceLock<Mutex<StoreData>> = OnceLock::new();
        STORE.get_or_init(|| Mutex::new(StoreData::default()))
    }

    pub(super) fn ensure(relation_oid: pg_sys::Oid) {
        let mut store = Self::global().lock().expect("test store mutex poisoned");
        let _ = store.table_mut(relation_oid);
    }

    pub(super) fn snapshot(relation_oid: pg_sys::Oid) -> Vec<TestRow> {
        let mut store = Self::global().lock().expect("test store mutex poisoned");
        store.table_mut(relation_oid).clone()
    }

    pub(super) fn replace(relation_oid: pg_sys::Oid, rows: Vec<TestRow>) {
        let mut store = Self::global().lock().expect("test store mutex poisoned");
        store.tables.insert(relation_oid, rows);
    }

    pub(super) fn insert(
        relation_oid: pg_sys::Oid,
        row: TestRow,
    ) -> Result<TestRow, &'static str> {
        let mut store = Self::global().lock().expect("test store mutex poisoned");
        let table = store.table_mut(relation_oid);
        if table.iter().any(|existing| existing.id == row.id) {
            return Err("test store rejected a duplicate row id");
        }
        table.push(row.clone());
        Ok(row)
    }

    pub(super) fn update(
        relation_oid: pg_sys::Oid,
        old_id: i32,
        row: TestRow,
    ) -> Option<TestRow> {
        let mut store = Self::global().lock().expect("test store mutex poisoned");
        let table = store.table_mut(relation_oid);
        let position = table.iter().position(|existing| existing.id == old_id)?;
        table[position] = row.clone();
        Some(row)
    }

    pub(super) fn delete(relation_oid: pg_sys::Oid, id: i32) -> Option<TestRow> {
        let mut store = Self::global().lock().expect("test store mutex poisoned");
        let table = store.table_mut(relation_oid);
        let position = table.iter().position(|existing| existing.id == id)?;
        Some(table.remove(position))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum TraceEvent {
    ScanBegin {
        ordered: bool,
        pushed_count: usize,
        filters: Vec<(pg_sys::AttrNumber, Option<i32>)>,
        projection: &'static str,
    },
    ScanRescan {
        params_changed: bool,
        filters: Vec<(pg_sys::AttrNumber, Option<i32>)>,
    },
    Pathkeys {
        candidate_count: usize,
        selected_candidate: usize,
        selected_attno: pg_sys::AttrNumber,
    },
    Modify {
        operation: &'static str,
        identity: &'static str,
        id: i32,
        returned_item_pointer: bool,
    },
}

pub(super) struct TestTrace;

impl TestTrace {
    fn global() -> &'static Mutex<Vec<TraceEvent>> {
        static TRACE: OnceLock<Mutex<Vec<TraceEvent>>> = OnceLock::new();
        TRACE.get_or_init(|| Mutex::new(Vec::new()))
    }

    pub(super) fn clear() {
        Self::global()
            .lock()
            .expect("test trace mutex poisoned")
            .clear();
    }

    pub(super) fn record(event: TraceEvent) {
        Self::global()
            .lock()
            .expect("test trace mutex poisoned")
            .push(event);
    }

    pub(super) fn take() -> Vec<TraceEvent> {
        let mut trace = Self::global().lock().expect("test trace mutex poisoned");
        std::mem::take(&mut *trace)
    }
}
