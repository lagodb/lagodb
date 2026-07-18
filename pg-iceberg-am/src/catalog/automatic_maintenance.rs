//! Catalog repository for runtime-scheduled Iceberg maintenance.
//!
//! The worker owns scheduling policy and maintenance execution. This repository
//! owns PostgreSQL catalog discovery and persistence for
//! `iceberg.automatic_maintenance_state`; no SQL or SPI crosses that boundary.

use std::borrow::Cow;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::ffi::CStr;

use pg_lakebase_core::catalog::{
    CatalogRelation, CatalogScanKey, CatalogSnapshot, get_namespace_oid,
    get_relation_oid,
};
use pg_lakebase_core::diag::PgError;
use pg_lakebase_core::handles::HeapTupleGuard;
use pgrx::prelude::TimestampWithTimeZone;
use pgrx::{FromDatum, IntoDatum, pg_sys};

use super::IcebergAccessMethod;
use crate::error::{
    IcebergError, IcebergResult, MetadataCatalogOperation as CatalogOp,
};

const ICEBERG_SCHEMA: &CStr = c"iceberg";
const STATE_TABLE: &CStr = c"automatic_maintenance_state";
const STATE_PKEY: &CStr = c"automatic_maintenance_state_pkey";
const MAX_ERROR_BYTES: usize = 2_048;

mod column {
    pub const RELID: i16 = 1;
    pub const CONSECUTIVE_FAILURES: i16 = 2;
    pub const NEXT_ATTEMPT_AT: i16 = 3;
    pub const LAST_ATTEMPT_AT: i16 = 4;
    pub const LAST_SUCCESS_AT: i16 = 5;
    pub const LAST_OUTCOME: i16 = 6;
    pub const LAST_ERROR: i16 = 7;
    pub const COUNT: usize = 7;
}

trait CatalogResultExt<T> {
    fn map_maintenance_catalog_err(self, operation: CatalogOp)
    -> IcebergResult<T>;
}

impl<T> CatalogResultExt<T> for Result<T, PgError> {
    fn map_maintenance_catalog_err(
        self,
        operation: CatalogOp,
    ) -> IcebergResult<T> {
        self.map_err(|source| IcebergError::AutomaticMaintenanceCatalog {
            operation,
            source,
        })
    }
}

#[derive(Clone, Copy)]
struct SchedulingState {
    next_attempt_at: pg_sys::TimestampTz,
    last_attempt_at: Option<pg_sys::TimestampTz>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SchedulingCandidate {
    last_attempt_at: Option<pg_sys::TimestampTz>,
    fairness_key: u64,
    relid: u32,
}

/// Keeps only the best scheduling candidates while pg_class is scanned.
/// Memory is O(limit), independent of the number of Iceberg relations.
struct CandidateSet {
    limit: usize,
    candidates: BinaryHeap<SchedulingCandidate>,
}

impl CandidateSet {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            candidates: BinaryHeap::with_capacity(limit),
        }
    }

    fn consider(&mut self, candidate: SchedulingCandidate) {
        if self.limit == 0 {
            return;
        }
        if self.candidates.len() < self.limit {
            self.candidates.push(candidate);
            return;
        }
        if self
            .candidates
            .peek()
            .is_some_and(|worst| candidate < *worst)
        {
            let _ = self.candidates.pop();
            self.candidates.push(candidate);
        }
    }

    fn into_relations(self) -> Vec<pg_sys::Oid> {
        let mut candidates = self.candidates.into_vec();
        candidates.sort_unstable();
        candidates
            .into_iter()
            .map(|candidate| pg_sys::Oid::from(candidate.relid))
            .collect()
    }
}

struct BoundedErrorText<'a>(Cow<'a, str>);

impl<'a> BoundedErrorText<'a> {
    fn new(error: &'a str) -> Self {
        if error.len() <= MAX_ERROR_BYTES {
            return Self(Cow::Borrowed(error));
        }
        let mut end = MAX_ERROR_BYTES;
        while !error.is_char_boundary(end) {
            end -= 1;
        }
        Self(Cow::Owned(error[..end].to_owned()))
    }

    fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

struct StateRecord<'a> {
    relid: pg_sys::Oid,
    consecutive_failures: i32,
    next_attempt_at: pg_sys::TimestampTz,
    last_attempt_at: pg_sys::TimestampTz,
    last_success_at: Option<pg_sys::TimestampTz>,
    last_outcome: &'a str,
    last_error: Option<&'a str>,
}

/// Successful worker outcome persisted by this catalog schema.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AutomaticMaintenanceOutcome {
    LockSkipped,
    NoWork,
    Maintained,
}

impl AutomaticMaintenanceOutcome {
    const fn label(self) -> &'static str {
        match self {
            Self::LockSkipped => "lock-skipped",
            Self::NoWork => "not-eligible",
            Self::Maintained => "maintained",
        }
    }

    const fn completed(self) -> bool {
        !matches!(self, Self::LockSkipped)
    }
}

impl StateRecord<'_> {
    fn encode(
        &self,
        tuple_desc: pg_sys::TupleDesc,
    ) -> IcebergResult<HeapTupleGuard> {
        // SAFETY: tuple_desc belongs to the repository's open relation.
        let attribute_count = unsafe { (*tuple_desc).natts as usize };
        if attribute_count != column::COUNT {
            return Err(
                IcebergError::AutomaticMaintenanceCatalogInvalidRecord(format!(
                    "expected {} columns, found {attribute_count}",
                    column::COUNT,
                )),
            );
        }
        let mut values = vec![pg_sys::Datum::from(0); attribute_count];
        let mut nulls = vec![true; attribute_count];
        Self::set(&mut values, &mut nulls, column::RELID, Some(self.relid));
        Self::set(
            &mut values,
            &mut nulls,
            column::CONSECUTIVE_FAILURES,
            Some(self.consecutive_failures),
        );
        Self::set(
            &mut values,
            &mut nulls,
            column::NEXT_ATTEMPT_AT,
            Some(self.next_attempt_at),
        );
        Self::set(
            &mut values,
            &mut nulls,
            column::LAST_ATTEMPT_AT,
            Some(self.last_attempt_at),
        );
        Self::set(
            &mut values,
            &mut nulls,
            column::LAST_SUCCESS_AT,
            self.last_success_at,
        );
        Self::set(
            &mut values,
            &mut nulls,
            column::LAST_OUTCOME,
            Some(self.last_outcome),
        );
        Self::set(
            &mut values,
            &mut nulls,
            column::LAST_ERROR,
            self.last_error,
        );

        // SAFETY: the arrays have one entry for every live attribute in the
        // repository-owned table schema, and tuple_desc belongs to the open
        // state relation used by the caller.
        Ok(unsafe {
            HeapTupleGuard::new(pg_sys::heap_form_tuple(
                tuple_desc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
            ))
        })
    }

    fn set<T: IntoDatum>(
        values: &mut [pg_sys::Datum],
        nulls: &mut [bool],
        attribute_number: i16,
        value: Option<T>,
    ) {
        let index = usize::try_from(attribute_number - 1)
            .expect("positive automatic maintenance attribute number");
        let datum = value.into_datum();
        values[index] = datum.unwrap_or(pg_sys::Datum::from(0));
        nulls[index] = datum.is_none();
    }
}

/// Repository over the AM-owned automatic-maintenance catalog.
pub(crate) struct AutomaticMaintenanceCatalog {
    state: CatalogRelation,
    pkey_oid: pg_sys::Oid,
}

impl AutomaticMaintenanceCatalog {
    /// Open the repository for one scheduling/state transaction.
    ///
    /// Every public operation may delete stale state or upsert an outcome, so
    /// the repository owns the required RowExclusiveLock rather than allowing
    /// callers to accidentally choose a read-only lock.
    pub(crate) fn open() -> IcebergResult<Self> {
        Self::open_if_available()?.ok_or_else(|| {
            IcebergError::AutomaticMaintenanceCatalogInvalidRecord(
                "automatic maintenance catalog is unavailable".to_owned(),
            )
        })
    }

    fn open_if_available() -> IcebergResult<Option<Self>> {
        let namespace = get_namespace_oid(ICEBERG_SCHEMA, true)
            .map_maintenance_catalog_err(CatalogOp::Access)?;
        if namespace == pg_sys::InvalidOid {
            return Ok(None);
        }
        let table_oid = get_relation_oid(STATE_TABLE, namespace)
            .map_maintenance_catalog_err(CatalogOp::Access)?;
        let pkey_oid = get_relation_oid(STATE_PKEY, namespace)
            .map_maintenance_catalog_err(CatalogOp::Access)?;
        if table_oid == pg_sys::InvalidOid || pkey_oid == pg_sys::InvalidOid {
            return Ok(None);
        }
        let state = CatalogRelation::open(
            table_oid,
            pg_sys::RowExclusiveLock as pg_sys::LOCKMODE,
        )
        .map_maintenance_catalog_err(CatalogOp::Access)?;
        // SAFETY: the tuple descriptor belongs to the live state relation.
        let attribute_count = unsafe {
            (*state.as_handle().tuple_desc()).natts as usize
        };
        if attribute_count != column::COUNT {
            return Err(
                IcebergError::AutomaticMaintenanceCatalogInvalidRecord(format!(
                    "expected {} columns, found {attribute_count}",
                    column::COUNT,
                )),
            );
        }
        Ok(Some(Self { state, pkey_oid }))
    }

    /// Discover eligible Iceberg heap relations and remove state whose
    /// relation no longer exists. Ordering preserves NULLS FIRST for tables
    /// never attempted and uses a backend-specific tie breaker for fairness.
    pub(crate) fn eligible_relations(
        &self,
        limit: usize,
        now: pg_sys::TimestampTz,
        backend_seed: u64,
    ) -> IcebergResult<Vec<pg_sys::Oid>> {
        let Some(iceberg_am_oid) = IcebergAccessMethod::oid() else {
            return Ok(Vec::new());
        };
        let pg_class = CatalogRelation::open(
            pg_sys::RelationRelationId,
            pg_sys::AccessShareLock as _,
        )
        .map_maintenance_catalog_err(CatalogOp::Read)?;
        let mut class_scan = pg_class
            .begin_scan(
                pg_sys::InvalidOid,
                false,
                CatalogSnapshot::Default,
                std::iter::empty(),
            )
            .map_maintenance_catalog_err(CatalogOp::Read)?;
        let mut live_relations = HashSet::new();
        while let Some(tuple) = class_scan
            .get_next()
            .map_maintenance_catalog_err(CatalogOp::Read)?
        {
            // SAFETY: this tuple comes from a live pg_class scan and therefore
            // has PostgreSQL's fixed Form_pg_class layout.
            let form = unsafe {
                pg_sys::GETSTRUCT(tuple.as_raw()) as pg_sys::Form_pg_class
            };
            if form.is_null() {
                continue;
            }
            let class = unsafe { &*form };
            if class.relam == iceberg_am_oid
                && class.relkind as u8 == pg_sys::RELKIND_RELATION
            {
                live_relations.insert(class.oid);
            }
        }
        drop(class_scan);

        let tuple_desc = self.state.as_handle().tuple_desc();
        let mut state_scan = self
            .state
            .begin_scan(
                pg_sys::InvalidOid,
                false,
                CatalogSnapshot::Default,
                std::iter::empty(),
            )
            .map_maintenance_catalog_err(CatalogOp::Read)?;
        let mut scheduling = HashMap::new();
        while let Some(tuple) = state_scan
            .get_next()
            .map_maintenance_catalog_err(CatalogOp::Read)?
        {
            let relid = unsafe {
                Self::required_attr::<pg_sys::Oid>(
                    tuple.as_raw(),
                    tuple_desc,
                    column::RELID,
                    "relid",
                )?
            };
            if !live_relations.contains(&relid) {
                self.state
                    .catalog_delete(tuple)
                    .map_maintenance_catalog_err(CatalogOp::Delete)?;
                continue;
            }
            let next_attempt_at = unsafe {
                Self::required_timestamp(
                    tuple.as_raw(),
                    tuple_desc,
                    column::NEXT_ATTEMPT_AT,
                    "next_attempt_at",
                )?
            };
            let last_attempt_at = unsafe {
                Self::optional_timestamp(
                    tuple.as_raw(),
                    tuple_desc,
                    column::LAST_ATTEMPT_AT,
                )
            };
            scheduling.insert(
                relid,
                SchedulingState {
                    next_attempt_at,
                    last_attempt_at,
                },
            );
        }

        let mut candidates = CandidateSet::new(limit);
        for relid in live_relations {
            let state = scheduling.get(&relid);
            if state.is_some_and(|state| state.next_attempt_at > now) {
                continue;
            }
            candidates.consider(SchedulingCandidate {
                last_attempt_at: state.and_then(|state| state.last_attempt_at),
                fairness_key: Self::fairness_key(relid.to_u32(), backend_seed),
                relid: relid.to_u32(),
            });
        }
        Ok(candidates.into_relations())
    }

    pub(crate) fn consecutive_failures(
        &self,
        relid: pg_sys::Oid,
    ) -> IcebergResult<u32> {
        let mut scan = self.scan_relid(relid, CatalogOp::Read)?;
        let Some(tuple) = scan
            .get_next()
            .map_maintenance_catalog_err(CatalogOp::Read)?
        else {
            return Ok(0);
        };
        let failures = unsafe {
            Self::required_attr::<i32>(
                tuple.as_raw(),
                self.state.as_handle().tuple_desc(),
                column::CONSECUTIVE_FAILURES,
                "consecutive_failures",
            )?
        };
        Ok(u32::try_from(failures).unwrap_or(0))
    }

    pub(crate) fn record_success(
        &self,
        relid: pg_sys::Oid,
        outcome: AutomaticMaintenanceOutcome,
        attempted_at: pg_sys::TimestampTz,
        next_attempt_at: pg_sys::TimestampTz,
    ) -> IcebergResult<()> {
        let previous_success = if outcome.completed() {
            None
        } else {
            self.last_success_at(relid)?
        };
        self.upsert(StateRecord {
            relid,
            consecutive_failures: 0,
            next_attempt_at,
            last_attempt_at: attempted_at,
            last_success_at: outcome
                .completed()
                .then_some(attempted_at)
                .or(previous_success),
            last_outcome: outcome.label(),
            last_error: None,
        })
    }

    pub(crate) fn record_failure(
        &self,
        relid: pg_sys::Oid,
        consecutive_failures: u32,
        error: &str,
        attempted_at: pg_sys::TimestampTz,
        next_attempt_at: pg_sys::TimestampTz,
    ) -> IcebergResult<()> {
        let last_success_at = self.last_success_at(relid)?;
        let error = BoundedErrorText::new(error);
        self.upsert(StateRecord {
            relid,
            consecutive_failures: i32::try_from(consecutive_failures)
                .unwrap_or(i32::MAX),
            next_attempt_at,
            last_attempt_at: attempted_at,
            last_success_at,
            last_outcome: "failed",
            last_error: Some(error.as_str()),
        })
    }

    /// Remove one relation's scheduler state in the same transaction as DROP.
    /// Missing catalog objects are accepted for DROP EXTENSION ordering; the
    /// periodic stale-row sweep remains a crash/recovery safety net.
    pub(crate) fn delete_relation_if_available(
        relid: pg_sys::Oid,
    ) -> IcebergResult<()> {
        let Some(repository) = Self::open_if_available()? else {
            return Ok(());
        };
        repository.delete_relation(relid)
    }

    fn delete_relation(&self, relid: pg_sys::Oid) -> IcebergResult<()> {
        let mut scan = self.scan_relid(relid, CatalogOp::Delete)?;
        if let Some(tuple) = scan
            .get_next()
            .map_maintenance_catalog_err(CatalogOp::Delete)?
        {
            self.state
                .catalog_delete(tuple)
                .map_maintenance_catalog_err(CatalogOp::Delete)?;
        }
        Ok(())
    }

    fn upsert(&self, record: StateRecord<'_>) -> IcebergResult<()> {
        if !Self::lock_live_iceberg_relation(record.relid) {
            return self.delete_relation(record.relid);
        }
        let tuple_desc = self.state.as_handle().tuple_desc();
        let new_tuple = record.encode(tuple_desc)?;
        let mut scan = self.scan_relid(record.relid, CatalogOp::Update)?;
        if let Some(old_tuple) = scan
            .get_next()
            .map_maintenance_catalog_err(CatalogOp::Update)?
        {
            self.state
                .catalog_update(old_tuple, &new_tuple)
                .map_maintenance_catalog_err(CatalogOp::Update)
        } else {
            self.state
                .catalog_insert(&new_tuple)
                .map_maintenance_catalog_err(CatalogOp::Insert)
        }
    }

    /// Hold an object lock through the state transaction so DROP cannot pass
    /// the liveness check and then leave a newly inserted orphan row behind.
    fn lock_live_iceberg_relation(relid: pg_sys::Oid) -> bool {
        unsafe {
            pg_sys::LockRelationOid(relid, pg_sys::AccessShareLock as _);
            let relkind = pg_sys::get_rel_relkind(relid) as u8;
            let relam = pg_sys::get_rel_relam(relid);
            matches!(
                relkind,
                pg_sys::RELKIND_RELATION | pg_sys::RELKIND_MATVIEW
            ) && IcebergAccessMethod::oid() == Some(relam)
        }
    }

    fn last_success_at(
        &self,
        relid: pg_sys::Oid,
    ) -> IcebergResult<Option<pg_sys::TimestampTz>> {
        let mut scan = self.scan_relid(relid, CatalogOp::Read)?;
        let Some(tuple) = scan
            .get_next()
            .map_maintenance_catalog_err(CatalogOp::Read)?
        else {
            return Ok(None);
        };
        Ok(unsafe {
            Self::optional_timestamp(
                tuple.as_raw(),
                self.state.as_handle().tuple_desc(),
                column::LAST_SUCCESS_AT,
            )
        })
    }

    fn scan_relid(
        &self,
        relid: pg_sys::Oid,
        operation: CatalogOp,
    ) -> IcebergResult<pg_lakebase_core::catalog::CatalogScan<'_>> {
        self.state
            .begin_scan(
                self.pkey_oid,
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::oid_eq(column::RELID as _, relid)],
            )
            .map_maintenance_catalog_err(operation)
    }

    fn fairness_key(relid: u32, seed: u64) -> u64 {
        let value = u64::from(relid) ^ seed;
        value
            .wrapping_add(0x9e37_79b9_7f4a_7c15)
            .wrapping_mul(0xbf58_476d_1ce4_e5b9)
            ^ value.rotate_left(23)
    }

    unsafe fn optional_attr<T: FromDatum>(
        tuple: pg_sys::HeapTuple,
        tuple_desc: pg_sys::TupleDesc,
        attribute_number: i16,
    ) -> Option<T> {
        let mut is_null = false;
        let datum = unsafe {
            pg_sys::heap_getattr(
                tuple,
                attribute_number as _,
                tuple_desc,
                &mut is_null,
            )
        };
        unsafe { T::from_datum(datum, is_null) }
    }

    unsafe fn required_attr<T: FromDatum>(
        tuple: pg_sys::HeapTuple,
        tuple_desc: pg_sys::TupleDesc,
        attribute_number: i16,
        name: &'static str,
    ) -> IcebergResult<T> {
        unsafe { Self::optional_attr(tuple, tuple_desc, attribute_number) }
            .ok_or_else(|| {
                IcebergError::AutomaticMaintenanceCatalogInvalidRecord(
                    format!("{name} is null or undecodable"),
                )
            })
    }

    unsafe fn optional_timestamp(
        tuple: pg_sys::HeapTuple,
        tuple_desc: pg_sys::TupleDesc,
        attribute_number: i16,
    ) -> Option<pg_sys::TimestampTz> {
        unsafe {
            Self::optional_attr::<TimestampWithTimeZone>(
                tuple,
                tuple_desc,
                attribute_number,
            )
        }
        .map(TimestampWithTimeZone::into_inner)
    }

    unsafe fn required_timestamp(
        tuple: pg_sys::HeapTuple,
        tuple_desc: pg_sys::TupleDesc,
        attribute_number: i16,
        name: &'static str,
    ) -> IcebergResult<pg_sys::TimestampTz> {
        unsafe { Self::optional_timestamp(tuple, tuple_desc, attribute_number) }
            .ok_or_else(|| {
                IcebergError::AutomaticMaintenanceCatalogInvalidRecord(
                    format!("{name} is null or undecodable"),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_set_retains_only_the_lowest_ordered_items() {
        let mut candidates = CandidateSet::new(2);
        for (last_attempt_at, fairness_key, relid) in [
            (Some(30), 0, 3),
            (None, 20, 2),
            (Some(10), 0, 4),
            (None, 10, 1),
        ] {
            candidates.consider(SchedulingCandidate {
                last_attempt_at,
                fairness_key,
                relid,
            });
        }

        assert_eq!(
            candidates
                .into_relations()
                .into_iter()
                .map(|oid| oid.to_u32())
                .collect::<Vec<_>>(),
            vec![1, 2],
        );
    }

    #[test]
    fn bounded_error_preserves_utf8_boundaries() {
        let error = "界".repeat(MAX_ERROR_BYTES);
        let bounded = BoundedErrorText::new(&error);

        assert!(bounded.as_str().len() <= MAX_ERROR_BYTES);
        assert!(error.starts_with(bounded.as_str()));
    }
}
