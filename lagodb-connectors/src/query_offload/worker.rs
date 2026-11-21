//! Parallel-worker payload: the leader's inventory, filters and split groups.
//!
//! A worker resolves the relation's location again for the leader's
//! effective user, so storage credentials never enter the payload, and
//! rebinds the leader's listed members without listing or probing storage.

use std::rc::Rc;
use std::sync::Mutex;

use bincode::Options;
use lagodb_core::handles::RelationGuard;
use lagodb_core::runtime_api::SourceWorkId;
use pgrx::pg_sys;
use serde::{Deserialize, Serialize};

use crate::fdw::{PlannedFormat, ResolvedForeignRelation};
use crate::format::FormatKind;
use crate::format::parquet::ParquetBoundPredicate;
use crate::storage::{InputInventory, StorageInput};

use super::BackendLocal;
use super::columns::ScanColumns;
use super::error::Error;
use super::scan::{BoundScan, PlannedTasks, SplitGroup};
use super::stream::{Stream, TaskSplits};

#[derive(Serialize, Deserialize)]
pub(super) struct WorkerPayload {
    relation_oid: u32,
    effective_user_oid: u32,
    attnos: Box<[pg_sys::AttrNumber]>,
    inventory: InputInventory,
    filters: Box<[ParquetBoundPredicate]>,
    groups: Box<[SplitGroup]>,
}

impl WorkerPayload {
    fn codec() -> impl Options {
        bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .reject_trailing_bytes()
    }

    /// Encode one run's task plan and its split groups.
    pub(super) fn encode(
        bound: &BoundScan,
        planned: &PlannedTasks,
        groups: Vec<SplitGroup>,
    ) -> Result<Box<[u8]>, Error> {
        let payload = Self {
            relation_oid: u32::from(bound.relation_oid()),
            effective_user_oid: u32::from(bound.effective_user_oid()),
            attnos: planned.columns().attnos().into(),
            inventory: planned.input().inventory(),
            filters: planned.filters().into(),
            groups: groups.into_boxed_slice(),
        };
        Ok(Self::codec().serialize(&payload)?.into_boxed_slice())
    }
}

pub(super) struct WorkerSource {
    input: BackendLocal<Rc<StorageInput>>,
    columns: ScanColumns,
    filters: Box<[ParquetBoundPredicate]>,
    /// Each work id's splits, taken once.
    groups: Mutex<Box<[Option<SplitGroup>]>>,
}

impl WorkerSource {
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let payload: WorkerPayload = WorkerPayload::codec().deserialize(bytes)?;
        let relation_oid = pg_sys::Oid::from(payload.relation_oid);
        // A parallel worker takes its own relation lock before reading any
        // catalog state and keeps it to transaction end, as PostgreSQL's
        // `ExecGetRangeTableRelation` does, so the relation stays protected
        // if the leader exits first.
        let relation = RelationGuard::open_table_retain_lock(
            relation_oid,
            pg_sys::AccessShareLock as pg_sys::LOCKMODE,
        )?;
        let resolved = ResolvedForeignRelation::resolve(relation_oid)?;
        resolved.ensure_planned(PlannedFormat::new(FormatKind::Parquet))?;
        let location = resolved
            .into_read_location(pg_sys::Oid::from(payload.effective_user_oid))?;
        let input = StorageInput::reopen(&location, payload.inventory)?;
        let columns = ScanColumns::load(&relation.as_handle(), &payload.attnos)?
            .ok_or(Error::Invariant(
                "a planned column lost its columnar representation",
            ))?;
        Ok(Self {
            input: BackendLocal::new(Rc::new(input)),
            columns,
            filters: payload.filters,
            groups: Mutex::new(
                payload.groups.into_vec().into_iter().map(Some).collect(),
            ),
        })
    }

    pub(super) fn open(
        &self,
        work_ids: &[SourceWorkId],
        batch_rows: u64,
    ) -> Result<Stream, Error> {
        let mut splits = Vec::new();
        {
            let mut groups = self.groups.lock().map_err(|_| {
                Error::worker_payload("parallel split inventory lock was poisoned")
            })?;
            for work_id in work_ids {
                let group = usize::try_from(*work_id)
                    .ok()
                    .and_then(|index| groups.get_mut(index))
                    .and_then(Option::take)
                    .ok_or_else(|| {
                        Error::worker_payload(format!(
                            "parallel work id {work_id} is unknown or already taken"
                        ))
                    })?;
                splits.extend(group.into_vec());
            }
        }
        Stream::open(
            &self.columns,
            TaskSplits::new(Rc::clone(self.input.get()), splits, None),
            self.filters.clone(),
            batch_rows,
        )
    }
}
