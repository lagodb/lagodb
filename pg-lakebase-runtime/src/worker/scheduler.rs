use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use crate::worker::state::INVALID_OID;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DatabaseSchedule {
    Idle,
    Pending,
    RetryAt(Instant),
}

pub(super) struct Scheduler {
    pending: VecDeque<u32>,
    databases: HashMap<u32, DatabaseSchedule>,
    due_retries: Vec<u32>,
}

impl Scheduler {
    pub(super) fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            databases: HashMap::new(),
            due_retries: Vec::new(),
        }
    }

    pub(super) fn enqueue(&mut self, database_oid: u32) {
        if database_oid == INVALID_OID {
            return;
        }
        // Only a successful pg_database snapshot inserts candidates. Shared
        // requests can schedule known candidates but cannot expand this set.
        let Some(schedule) = self.databases.get_mut(&database_oid) else {
            return;
        };
        if *schedule == DatabaseSchedule::Idle {
            *schedule = DatabaseSchedule::Pending;
            self.pending.push_back(database_oid);
        }
    }

    pub(super) fn defer_retry(&mut self, database_oid: u32, delay: Duration) {
        self.pending
            .retain(|queued_oid| *queued_oid != database_oid);
        if let Some(schedule) = self.databases.get_mut(&database_oid) {
            *schedule = DatabaseSchedule::RetryAt(Instant::now() + delay);
        }
    }

    pub(super) fn promote_due_retries(&mut self) {
        let now = Instant::now();
        self.due_retries.clear();
        self.due_retries.extend(self.databases.iter().filter_map(
            |(&database_oid, &schedule)| {
                matches!(schedule, DatabaseSchedule::RetryAt(not_before) if not_before <= now)
                    .then_some(database_oid)
            },
        ));
        for database_oid in self.due_retries.drain(..) {
            self.databases
                .insert(database_oid, DatabaseSchedule::Pending);
            self.pending.push_back(database_oid);
        }
    }

    pub(super) fn next_deadline_delay(&self) -> Option<Duration> {
        let now = Instant::now();
        self.databases
            .values()
            .filter_map(|schedule| match schedule {
                DatabaseSchedule::RetryAt(deadline) => {
                    Some(deadline.saturating_duration_since(now))
                }
                DatabaseSchedule::Idle | DatabaseSchedule::Pending => None,
            })
            .min()
    }

    pub(super) fn reconcile_live(&mut self, live: &HashSet<u32>) -> Vec<u32> {
        self.pending
            .retain(|database_oid| live.contains(database_oid));
        self.databases
            .retain(|database_oid, _| live.contains(database_oid));
        let mut discovered = Vec::new();
        for &database_oid in live {
            let is_new = !self.databases.contains_key(&database_oid);
            self.databases
                .entry(database_oid)
                .or_insert(DatabaseSchedule::Idle);
            if is_new {
                discovered.push(database_oid);
            }
        }
        discovered
    }

    pub(super) fn len(&self) -> usize {
        self.pending.len()
    }

    pub(super) fn pop_front(&mut self) -> Option<u32> {
        let database_oid = self.pending.pop_front()?;
        *self
            .databases
            .get_mut(&database_oid)
            .expect("queued database must belong to the candidate snapshot") =
            DatabaseSchedule::Idle;
        Some(database_oid)
    }
}
