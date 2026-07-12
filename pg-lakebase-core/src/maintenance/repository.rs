//! Direct PostgreSQL catalog access for `lakebase.maintenance_queue`.

use std::collections::HashSet;

use crate::catalog::{
    CatalogRelation, CatalogScanKey, CatalogSnapshot, CatalogUpdateResult,
    MaintenanceCatalogIds, get_maintenance_catalog_ids,
};
use crate::handles::HeapTupleGuard;
use pgrx::prelude::TimestampWithTimeZone;
use pgrx::{FromDatum, IntoDatum, pg_sys};

use super::error::{MaintenanceCatalogOperation, MaintenanceError};
use super::gucs;
use super::item::{
    MaintenanceItem, MaintenanceItemId, MaintenanceItemRef, MaintenanceOperation,
    MaintenanceTarget,
};
use super::target::ObjectTreeTarget;

const MAX_PRODUCER_BYTES: usize = 128;
const MAX_SOURCE_NAME_BYTES: usize = 256;
const MAX_ERROR_BYTES: usize = 2_048;
const COLUMN_COUNT: usize = 13;

mod column {
    pub const ITEM_ID: i16 = 1;
    pub const OPERATION: i16 = 2;
    pub const STORE_ID: i16 = 3;
    pub const OBJECT_NAMESPACE: i16 = 4;
    pub const OBJECT_PATH: i16 = 5;
    pub const PRODUCER: i16 = 6;
    pub const SOURCE_RELID: i16 = 7;
    pub const SOURCE_NAME: i16 = 8;
    pub const ATTEMPT_COUNT: i16 = 9;
    pub const NOT_BEFORE: i16 = 10;
    pub const FAILED: i16 = 11;
    pub const LAST_ERROR: i16 = 12;
    pub const CREATED_AT: i16 = 13;
}

pub struct MaintenanceQueue;

impl MaintenanceQueue {
    pub fn enqueue(
        item: MaintenanceItemRef<'_>,
    ) -> Result<MaintenanceItemId, MaintenanceError> {
        validate_context(&item)?;
        let catalog =
            MaintenanceQueueCatalog::open_required(pg_sys::RowExclusiveLock as _)?;
        let row = QueueRow::new(&item);
        let tuple = row.encode(catalog.relation.as_handle().tuple_desc());
        catalog.relation.catalog_insert(&tuple).map_err(|source| {
            MaintenanceError::catalog(MaintenanceCatalogOperation::Insert, source)
        })?;
        Ok(row.id)
    }

    pub fn enqueue_batch(
        items: &[MaintenanceItemRef<'_>],
    ) -> Result<usize, MaintenanceError> {
        let limit = gucs::batch_items();
        if items.len() > limit {
            return Err(MaintenanceError::BatchTooLarge(limit));
        }
        for item in items {
            validate_context(item)?;
        }
        if items.is_empty() {
            return Ok(0);
        }

        let catalog =
            MaintenanceQueueCatalog::open_required(pg_sys::RowExclusiveLock as _)?;
        let tuple_desc = catalog.relation.as_handle().tuple_desc();
        let mut writer = catalog.relation.writer().map_err(|source| {
            MaintenanceError::catalog(MaintenanceCatalogOperation::Insert, source)
        })?;
        for item in items {
            let tuple = QueueRow::new(item).encode(tuple_desc);
            writer.insert(&tuple).map_err(|source| {
                MaintenanceError::catalog(MaintenanceCatalogOperation::Insert, source)
            })?;
        }
        Ok(items.len())
    }

    pub fn has_tree_target(
        target: &ObjectTreeTarget,
    ) -> Result<bool, MaintenanceError> {
        let Some(catalog) =
            MaintenanceQueueCatalog::open_if_available(pg_sys::AccessShareLock as _)?
        else {
            return Ok(false);
        };
        let mut scan = catalog
            .relation
            .begin_scan(
                catalog.ids.target_index,
                true,
                CatalogSnapshot::Default,
                target_keys(
                    MaintenanceOperation::DeleteTree,
                    target.store_id().as_str(),
                    target.namespace(),
                    target.prefix(),
                ),
            )
            .map_err(|source| {
                MaintenanceError::catalog(MaintenanceCatalogOperation::Scan, source)
            })?;
        Ok(scan
            .get_next()
            .map_err(|source| {
                MaintenanceError::catalog(MaintenanceCatalogOperation::Scan, source)
            })?
            .is_some())
    }

    pub fn retry_failed(
        item_id: MaintenanceItemId,
    ) -> Result<bool, MaintenanceError> {
        MaintenanceRepository::retry_failed(item_id)
    }
}

pub(crate) struct InvalidMaintenanceRecord {
    pub(crate) id: MaintenanceItemId,
    pub(crate) error: String,
}

pub(crate) struct ReadyMaintenanceBatch {
    pub(crate) tasks: Vec<MaintenanceItem>,
    pub(crate) invalid: Vec<InvalidMaintenanceRecord>,
}

pub(crate) enum QueuePoll {
    Unavailable,
    Ready(ReadyMaintenanceBatch),
}

pub(crate) struct MaintenanceRepository;

impl MaintenanceRepository {
    pub(crate) fn fetch_ready_batch(
        limit: usize,
        in_flight: &HashSet<MaintenanceItemId>,
    ) -> Result<QueuePoll, MaintenanceError> {
        let Some(catalog) =
            MaintenanceQueueCatalog::open_if_available(pg_sys::AccessShareLock as _)?
        else {
            return Ok(QueuePoll::Unavailable);
        };
        let mut batch = ReadyMaintenanceBatch {
            tasks: Vec::with_capacity(limit),
            invalid: Vec::new(),
        };
        if limit == 0 {
            return Ok(QueuePoll::Ready(batch));
        }
        let now = current_timestamp();
        let mut scan = catalog
            .relation
            .begin_ordered_scan(
                catalog.ids.ready_index,
                CatalogSnapshot::Default,
                [
                    CatalogScanKey::bool_eq(column::FAILED as _, false),
                    CatalogScanKey::timestamptz_le(column::NOT_BEFORE as _, now),
                ],
            )
            .map_err(|source| {
                MaintenanceError::catalog(MaintenanceCatalogOperation::Scan, source)
            })?;
        let tuple_desc = catalog.relation.as_handle().tuple_desc();
        let max_examined = limit.saturating_add(in_flight.len());
        let mut examined = 0_usize;
        while examined < max_examined && batch.tasks.len() < limit {
            let Some(tuple) = scan.get_next().map_err(|source| {
                MaintenanceError::catalog(MaintenanceCatalogOperation::Scan, source)
            })?
            else {
                break;
            };
            examined = examined.saturating_add(1);
            let id = MaintenanceItemId(unsafe {
                required_attr(tuple.as_raw(), tuple_desc, column::ITEM_ID, "item_id")?
            });
            if in_flight.contains(&id) {
                continue;
            }
            let decoded = unsafe { QueueRow::decode(tuple.as_raw(), tuple_desc, id) }
                .and_then(QueueRow::into_item);
            match decoded {
                Ok(task) => batch.tasks.push(task),
                Err(error) => batch.invalid.push(InvalidMaintenanceRecord {
                    id,
                    error: bounded_error(&error.to_string()),
                }),
            }
        }
        Ok(QueuePoll::Ready(batch))
    }

    pub(crate) fn complete(item: &MaintenanceItem) -> Result<(), MaintenanceError> {
        let catalog =
            MaintenanceQueueCatalog::open_required(pg_sys::RowExclusiveLock as _)?;
        catalog.delete(item.id)
    }

    pub(crate) fn retry(
        item: &MaintenanceItem,
        error: &str,
    ) -> Result<(), MaintenanceError> {
        let attempt = item.attempt_count.saturating_add(1);
        if attempt >= gucs::retry_max_attempts() {
            return Self::fail_with_attempt(item.id, attempt, error);
        }
        let exponent =
            u32::try_from(attempt.saturating_sub(1).clamp(0, 30)).unwrap_or(30);
        let delay_ms = gucs::retry_base_ms()
            .saturating_mul(1_u64.checked_shl(exponent).unwrap_or(u64::MAX))
            .min(gucs::retry_max_ms());
        let error = bounded_error(error);
        Self::update(item.id, |row| {
            row.attempt_count = attempt;
            row.not_before = timestamp_after(delay_ms);
            row.failed = false;
            row.last_error = Some(error);
        })
    }

    pub(crate) fn fail(
        item: &MaintenanceItem,
        error: &str,
    ) -> Result<(), MaintenanceError> {
        Self::fail_with_attempt(item.id, item.attempt_count, error)
    }

    pub(crate) fn fail_invalid(
        item_id: MaintenanceItemId,
        error: &str,
    ) -> Result<(), MaintenanceError> {
        Self::fail_with_attempt(item_id, 0, error)
    }

    fn fail_with_attempt(
        item_id: MaintenanceItemId,
        attempt: i32,
        error: &str,
    ) -> Result<(), MaintenanceError> {
        let error = bounded_error(error);
        Self::update(item_id, |row| {
            row.attempt_count = attempt;
            row.failed = true;
            row.last_error = Some(error);
        })
    }

    fn retry_failed(item_id: MaintenanceItemId) -> Result<bool, MaintenanceError> {
        let catalog =
            MaintenanceQueueCatalog::open_required(pg_sys::RowExclusiveLock as _)?;
        let Some(updated) = catalog.mutate_row(item_id, |row| {
            if !row.failed {
                return false;
            }
            row.attempt_count = 0;
            row.not_before = current_timestamp();
            row.failed = false;
            row.last_error = None;
            true
        })?
        else {
            return Ok(false);
        };
        Ok(updated)
    }

    fn update(
        item_id: MaintenanceItemId,
        mutate: impl FnOnce(&mut QueueRow),
    ) -> Result<(), MaintenanceError> {
        let catalog =
            MaintenanceQueueCatalog::open_required(pg_sys::RowExclusiveLock as _)?;
        let Some(updated) = catalog.mutate_row(item_id, |row| {
            mutate(row);
            true
        })?
        else {
            return Err(MaintenanceError::InvalidRecord(format!(
                "maintenance item {item_id} disappeared"
            )));
        };
        debug_assert!(updated);
        Ok(())
    }
}

struct MaintenanceQueueCatalog {
    relation: CatalogRelation,
    ids: MaintenanceCatalogIds,
}

impl MaintenanceQueueCatalog {
    fn open_if_available(
        lock_mode: pg_sys::LOCKMODE,
    ) -> Result<Option<Self>, MaintenanceError> {
        let Some(ids) = get_maintenance_catalog_ids().map_err(|source| {
            MaintenanceError::catalog(MaintenanceCatalogOperation::Resolve, source)
        })?
        else {
            return Ok(None);
        };
        let relation =
            CatalogRelation::open(ids.table, lock_mode).map_err(|source| {
                MaintenanceError::catalog(MaintenanceCatalogOperation::Open, source)
            })?;
        Ok(Some(Self { relation, ids }))
    }

    fn open_required(lock_mode: pg_sys::LOCKMODE) -> Result<Self, MaintenanceError> {
        Self::open_if_available(lock_mode)?.ok_or(MaintenanceError::QueueUnavailable)
    }

    fn mutate_row(
        &self,
        item_id: MaintenanceItemId,
        mutate: impl FnOnce(&mut QueueRow) -> bool,
    ) -> Result<Option<bool>, MaintenanceError> {
        let mut scan = self
            .relation
            .begin_scan(
                self.ids.pkey,
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::uuid_eq(column::ITEM_ID as _, item_id.0)],
            )
            .map_err(|source| {
                MaintenanceError::catalog(MaintenanceCatalogOperation::Scan, source)
            })?;
        let Some(old_tuple) = scan.get_next().map_err(|source| {
            MaintenanceError::catalog(MaintenanceCatalogOperation::Scan, source)
        })?
        else {
            return Ok(None);
        };
        let mut row = unsafe {
            QueueRow::decode(
                old_tuple.as_raw(),
                self.relation.as_handle().tuple_desc(),
                item_id,
            )?
        };
        if !mutate(&mut row) {
            return Ok(Some(false));
        }
        let new_tuple = row.encode(self.relation.as_handle().tuple_desc());
        match self
            .relation
            .catalog_update_optimistic(old_tuple, &new_tuple)
            .map_err(|source| {
                MaintenanceError::catalog(MaintenanceCatalogOperation::Update, source)
            })? {
            CatalogUpdateResult::Success => Ok(Some(true)),
            CatalogUpdateResult::Conflict => {
                Err(MaintenanceError::ConcurrentUpdate(row.id))
            }
        }
    }

    fn delete(&self, item_id: MaintenanceItemId) -> Result<(), MaintenanceError> {
        let mut scan = self
            .relation
            .begin_scan(
                self.ids.pkey,
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::uuid_eq(column::ITEM_ID as _, item_id.0)],
            )
            .map_err(|source| {
                MaintenanceError::catalog(MaintenanceCatalogOperation::Scan, source)
            })?;
        let Some(tuple) = scan.get_next().map_err(|source| {
            MaintenanceError::catalog(MaintenanceCatalogOperation::Scan, source)
        })?
        else {
            return Err(MaintenanceError::InvalidRecord(format!(
                "maintenance item {item_id} disappeared"
            )));
        };
        self.relation.catalog_delete(tuple).map_err(|source| {
            MaintenanceError::catalog(MaintenanceCatalogOperation::Delete, source)
        })
    }
}

#[derive(Debug)]
struct QueueRow {
    id: MaintenanceItemId,
    operation: i16,
    store_id: String,
    namespace: String,
    path: String,
    producer: String,
    source_relid: Option<pg_sys::Oid>,
    source_name: Option<String>,
    attempt_count: i32,
    not_before: pg_sys::TimestampTz,
    failed: bool,
    last_error: Option<String>,
    created_at: pg_sys::TimestampTz,
}

impl QueueRow {
    fn new(item: &MaintenanceItemRef<'_>) -> Self {
        let (store_id, namespace, path, context) = item.fields();
        let now = current_timestamp();
        Self {
            id: MaintenanceItemId::new(),
            operation: item.operation() as i16,
            store_id: store_id.to_owned(),
            namespace: namespace.to_owned(),
            path: path.to_owned(),
            producer: context.producer.to_owned(),
            source_relid: context.source_relid,
            source_name: context.source_name.map(str::to_owned),
            attempt_count: 0,
            not_before: now,
            failed: false,
            last_error: None,
            created_at: now,
        }
    }

    fn encode(&self, tuple_desc: pg_sys::TupleDesc) -> HeapTupleGuard {
        let mut fields = TupleFields::new();
        fields.set(column::ITEM_ID, Some(self.id.0));
        fields.set(column::OPERATION, Some(self.operation));
        fields.set(column::STORE_ID, Some(self.store_id.as_str()));
        fields.set(column::OBJECT_NAMESPACE, Some(self.namespace.as_str()));
        fields.set(column::OBJECT_PATH, Some(self.path.as_str()));
        fields.set(column::PRODUCER, Some(self.producer.as_str()));
        fields.set(column::SOURCE_RELID, self.source_relid);
        fields.set(column::SOURCE_NAME, self.source_name.as_deref());
        fields.set(column::ATTEMPT_COUNT, Some(self.attempt_count));
        fields.set(column::NOT_BEFORE, Some(self.not_before));
        fields.set(column::FAILED, Some(self.failed));
        fields.set(column::LAST_ERROR, self.last_error.as_deref());
        fields.set(column::CREATED_AT, Some(self.created_at));
        unsafe {
            HeapTupleGuard::new(pg_sys::heap_form_tuple(
                tuple_desc,
                fields.values.as_mut_ptr(),
                fields.nulls.as_mut_ptr(),
            ))
        }
    }

    unsafe fn decode(
        tuple: pg_sys::HeapTuple,
        tuple_desc: pg_sys::TupleDesc,
        id: MaintenanceItemId,
    ) -> Result<Self, MaintenanceError> {
        let not_before: TimestampWithTimeZone = unsafe {
            required_attr(tuple, tuple_desc, column::NOT_BEFORE, "not_before")?
        };
        let created_at: TimestampWithTimeZone = unsafe {
            required_attr(tuple, tuple_desc, column::CREATED_AT, "created_at")?
        };
        Ok(Self {
            id,
            operation: unsafe {
                required_attr(tuple, tuple_desc, column::OPERATION, "operation")?
            },
            store_id: unsafe {
                required_attr(tuple, tuple_desc, column::STORE_ID, "store_id")?
            },
            namespace: unsafe {
                required_attr(
                    tuple,
                    tuple_desc,
                    column::OBJECT_NAMESPACE,
                    "object_namespace",
                )?
            },
            path: unsafe {
                required_attr(tuple, tuple_desc, column::OBJECT_PATH, "object_path")?
            },
            producer: unsafe {
                required_attr(tuple, tuple_desc, column::PRODUCER, "producer")?
            },
            source_relid: unsafe {
                optional_attr(tuple, tuple_desc, column::SOURCE_RELID)
            },
            source_name: unsafe {
                optional_attr(tuple, tuple_desc, column::SOURCE_NAME)
            },
            attempt_count: unsafe {
                required_attr(
                    tuple,
                    tuple_desc,
                    column::ATTEMPT_COUNT,
                    "attempt_count",
                )?
            },
            not_before: not_before.into_inner(),
            failed: unsafe {
                required_attr(tuple, tuple_desc, column::FAILED, "failed")?
            },
            last_error: unsafe {
                optional_attr(tuple, tuple_desc, column::LAST_ERROR)
            },
            created_at: created_at.into_inner(),
        })
    }

    fn into_item(self) -> Result<MaintenanceItem, MaintenanceError> {
        if self.attempt_count < 0 {
            return Err(MaintenanceError::InvalidRecord(
                "maintenance attempt count is negative".to_owned(),
            ));
        }
        if self.producer.is_empty() || self.producer.len() > MAX_PRODUCER_BYTES {
            return Err(MaintenanceError::InvalidRecord(
                "maintenance producer is invalid".to_owned(),
            ));
        }
        if self
            .source_name
            .as_ref()
            .is_some_and(|name| name.len() > MAX_SOURCE_NAME_BYTES)
        {
            return Err(MaintenanceError::InvalidRecord(
                "maintenance source name is too long".to_owned(),
            ));
        }
        let operation =
            MaintenanceOperation::try_from(self.operation).map_err(|raw| {
                MaintenanceError::InvalidRecord(format!(
                    "unknown maintenance operation {raw}"
                ))
            })?;
        let target = match operation {
            MaintenanceOperation::DeleteObject => MaintenanceTarget::Object {
                store_id: self.store_id,
                namespace: self.namespace,
                path: self.path,
            },
            MaintenanceOperation::DeleteTree => {
                if self.path.is_empty()
                    || self.path == "/"
                    || self.path.starts_with('/')
                    || !self.path.ends_with('/')
                {
                    return Err(MaintenanceError::InvalidRecord(
                        "maintenance tree prefix is not a scoped normalized root"
                            .to_owned(),
                    ));
                }
                MaintenanceTarget::Tree {
                    store_id: self.store_id,
                    namespace: self.namespace,
                    prefix: self.path,
                }
            }
        };
        Ok(MaintenanceItem {
            id: self.id,
            target,
            producer: self.producer,
            attempt_count: self.attempt_count,
        })
    }
}

struct TupleFields {
    values: [pg_sys::Datum; COLUMN_COUNT],
    nulls: [bool; COLUMN_COUNT],
}

impl TupleFields {
    fn new() -> Self {
        Self {
            values: [pg_sys::Datum::from(0usize); COLUMN_COUNT],
            nulls: [true; COLUMN_COUNT],
        }
    }

    fn set<T: IntoDatum>(&mut self, attno: i16, value: Option<T>) {
        let index = usize::try_from(attno - 1)
            .expect("positive maintenance catalog attribute number");
        if let Some(value) = value
            && let Some(datum) = value.into_datum()
        {
            self.values[index] = datum;
            self.nulls[index] = false;
        }
    }
}

fn target_keys(
    operation: MaintenanceOperation,
    store_id: &str,
    namespace: &str,
    path: &str,
) -> [CatalogScanKey; 4] {
    [
        CatalogScanKey::i16_eq(column::OPERATION as _, operation as i16),
        CatalogScanKey::text_eq(column::STORE_ID as _, store_id),
        CatalogScanKey::text_eq(column::OBJECT_NAMESPACE as _, namespace),
        CatalogScanKey::text_eq(column::OBJECT_PATH as _, path),
    ]
}

fn validate_context(item: &MaintenanceItemRef<'_>) -> Result<(), MaintenanceError> {
    let context = item.fields().3;
    if context.producer.is_empty() || context.producer.len() > MAX_PRODUCER_BYTES {
        return Err(MaintenanceError::InvalidProducer);
    }
    if context
        .source_name
        .is_some_and(|name| name.len() > MAX_SOURCE_NAME_BYTES)
    {
        return Err(MaintenanceError::InvalidSourceName);
    }
    Ok(())
}

fn current_timestamp() -> pg_sys::TimestampTz {
    unsafe { pg_sys::GetCurrentTimestamp() }
}

fn timestamp_after(delay_ms: u64) -> pg_sys::TimestampTz {
    let micros = delay_ms.saturating_mul(1_000);
    current_timestamp().saturating_add(i64::try_from(micros).unwrap_or(i64::MAX))
}

fn bounded_error(error: &str) -> String {
    if error.len() <= MAX_ERROR_BYTES {
        return error.to_owned();
    }
    let mut end = MAX_ERROR_BYTES;
    while !error.is_char_boundary(end) {
        end -= 1;
    }
    error[..end].to_owned()
}

unsafe fn optional_attr<T: FromDatum>(
    tuple: pg_sys::HeapTuple,
    tuple_desc: pg_sys::TupleDesc,
    attno: i16,
) -> Option<T> {
    let mut is_null = false;
    let datum =
        unsafe { pg_sys::heap_getattr(tuple, attno as _, tuple_desc, &mut is_null) };
    unsafe { T::from_datum(datum, is_null) }
}

unsafe fn required_attr<T: FromDatum>(
    tuple: pg_sys::HeapTuple,
    tuple_desc: pg_sys::TupleDesc,
    attno: i16,
    name: &'static str,
) -> Result<T, MaintenanceError> {
    unsafe { optional_attr(tuple, tuple_desc, attno) }.ok_or_else(|| {
        MaintenanceError::InvalidRecord(format!(
            "maintenance queue column {name} is null or invalid"
        ))
    })
}
