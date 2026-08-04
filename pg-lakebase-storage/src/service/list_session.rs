//! Server-side state for paginated `List` calls.
//!
//! A [`ListSession`] owns the `'static` [`BoxStream`] returned by
//! [`crate::backend::ObjectBackend::list`] plus enough context to drain the next page on demand.
//! Sessions are stored in the owning connection's [`ListSessionTable`] keyed by an opaque
//! [`ListCursor`]; clients echo the cursor on that same connection to fetch the next page.
//!
//! ## Lifecycle
//!
//! * Created on the **first** `List` call where `cursor = None`.
//! * Each subsequent `List` call with the same cursor drains up to `page_size` entries from the
//!   underlying stream.
//! * The session is removed from the table when the underlying stream is fully drained
//!   (`next_cursor = None` is returned), the cursor idles past [`SESSION_IDLE_TTL`], or the table
//!   is explicitly closed by its client or shutdown.
//!
//! ## Why a stateful cursor and not a stateless offset
//!
//! `object_store::list` does not guarantee key ordering and `list_with_offset` is implementation
//! defined (LocalFileSystem simulates it, others delegate to the backend). Carrying the live
//! stream over the cursor matches what every backend can express natively (S3
//! `continuation-token`, GCS `pageToken`, Azure `marker`, in-memory iterator) and gives us a
//! single uniform paging surface.
//!
//! Sessions are connection-local. Dropping the connection releases its streams, and a cursor
//! cannot be resumed through a different attached context.
//!
//! ## Resource accounting
//!
//! Each live session pins one backend list iterator (which in turn may hold an HTTP connection
//! out to the object store) and one entry in the [`ListSessionTable`] map. There is **no upper
//! bound on the number of concurrent sessions** today: the only reclamation paths are draining
//! to end and idle expiry. A misbehaving or forgetful client that opens many list cursors and
//! never finishes them will pin proportional resources for up to `LIST_CURSOR_IDLE_TTL_MS`.
//!
//! Paging clients must consume or explicitly close their cursor. If the service grows a
//! multi-tenant or high-fanout list workload, the sensible follow-ups in priority order are:
//!
//! 1. A hard cap on `len()` with eviction of the oldest idle session when full,
//! 2. Per-tenant quotas keyed off whatever identity the connection layer carries,
//! 3. A metric for `len()` plus session-age percentiles so leaks are visible.
//!
//! None of these change the on-the-wire protocol, so there is no reason to do them
//! preemptively.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::StreamExt;
use futures::stream::BoxStream;

use crate::error::StorageResult;
use crate::object::ListEntry;
use crate::protocol::{ListCursor, MAX_LIST_PAGE_SIZE};
use crate::service::LIST_CURSOR_IDLE_TTL_MS;

/// Idle timeout after which a [`ListSession`] is reaped.
///
/// Five minutes mirrors the design Q&A: long enough that a slow client iterating page-by-page
/// stays alive between calls, short enough that a forgotten cursor does not pin a backend
/// stream forever.
const SESSION_IDLE_TTL: Duration =
    Duration::from_millis(LIST_CURSOR_IDLE_TTL_MS as u64);

/// Default `page_size` when the client passes `0`.
pub(crate) const DEFAULT_PAGE_SIZE: u32 = 1000;

/// Maximum `page_size` we are willing to serve in a single response.
///
/// Capped to keep one wire frame from blowing the framing budget; clients that need more than
/// 10000 entries in one shot should iterate.
pub(crate) const MAX_PAGE_SIZE: u32 = MAX_LIST_PAGE_SIZE;

/// Active list iterator parked between successive `List` calls.
struct ListSession {
    stream: BoxStream<'static, StorageResult<ListEntry>>,
    last_used: Instant,
}

/// Connection-local table of live list sessions.
///
/// Cheap to construct; cloning is intentionally not supported to make ownership explicit (the
/// table lives on one [`crate::session::StorageContext`] for the socket lifetime).
pub(crate) struct ListSessionTable {
    inner: Mutex<HashMap<ListCursor, ListSession>>,
    next_id: AtomicU64,
    idle_ttl: Duration,
}

impl ListSessionTable {
    pub(crate) fn new() -> Self {
        Self::with_idle_ttl(SESSION_IDLE_TTL)
    }

    pub(crate) fn with_idle_ttl(idle_ttl: Duration) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            idle_ttl,
        }
    }

    /// Register a fresh stream and return the cursor that names it.
    pub(crate) fn insert(
        &self,
        stream: BoxStream<'static, StorageResult<ListEntry>>,
    ) -> ListCursor {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        // The cursor is opaque to the client. The "ls-" prefix is purely so a stray log line
        // surfaces what the bytes are — the client must not parse it.
        let cursor = ListCursor::from_wire(format!("ls-{id:016x}"));
        let mut sessions = self.lock();
        self.expire_idle_locked(&mut sessions);
        sessions.insert(
            cursor.clone(),
            ListSession {
                stream,
                last_used: Instant::now(),
            },
        );
        cursor
    }

    /// Drain up to `count` entries from the session named by `cursor`.
    ///
    /// Returns:
    /// * `Ok(DrainOutcome { entries, exhausted })` on a clean drain. `entries` are all `Ok`
    ///   values; `exhausted = true` when the underlying stream produced `None`.
    /// * `Err(ListSessionError::UnknownCursor)` if the cursor is unknown (expired or never
    ///   created).
    /// * `Err(ListSessionError::StreamError(e))` if the underlying stream yielded an `Err`.
    ///   Successfully-pulled entries before the error are discarded because the current wire
    ///   response shape has no partial-progress slot; the session is **consumed** (removed
    ///   from the table) because further drains on the same cursor are meaningless once the
    ///   stream errors.
    ///
    /// On drain-to-end the session is removed from the table.
    pub(crate) async fn drain(
        &self,
        cursor: &ListCursor,
        count: usize,
    ) -> Result<DrainOutcome, ListSessionError> {
        // Take exclusive ownership of the stream for the drain. Two concurrent `List` calls on
        // the same cursor would otherwise interleave reads from the same iterator; pulling the
        // stream out of the table guarantees serial use, and we put it back at the end if we
        // did not exhaust it and did not see an error.
        let mut session = {
            let mut sessions = self.lock();
            self.expire_idle_locked(&mut sessions);
            sessions
                .remove(cursor)
                .ok_or(ListSessionError::UnknownCursor)?
        };

        let mut entries = Vec::with_capacity(count.min(64));
        let mut exhausted = false;
        for _ in 0..count {
            match session.stream.next().await {
                Some(Ok(entry)) => entries.push(entry),
                Some(Err(error)) => {
                    // Once the stream errors, the session is no longer useful: subsequent
                    // polls of an errored object_store list stream are not guaranteed to
                    // recover. Drop the session here (we already removed it from the table
                    // above) and surface the error.
                    return Err(ListSessionError::StreamError(error));
                }
                None => {
                    exhausted = true;
                    break;
                }
            }
        }

        if !exhausted {
            session.last_used = Instant::now();
            self.lock().insert(cursor.clone(), session);
        }

        Ok(DrainOutcome { entries, exhausted })
    }

    /// Drop a session without draining (e.g. server shutdown). No-op if the cursor is unknown.
    pub(crate) fn forget(&self, cursor: &ListCursor) {
        self.lock().remove(cursor);
    }

    /// Number of currently registered sessions. Used by tests and observability.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lock().len()
    }

    fn expire_idle_locked(&self, sessions: &mut HashMap<ListCursor, ListSession>) {
        let now = Instant::now();
        sessions.retain(|_, session| {
            now.duration_since(session.last_used) < self.idle_ttl
        });
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<ListCursor, ListSession>> {
        self.inner
            .lock()
            .expect("list session table mutex poisoned; list cursor state is no longer trustworthy")
    }
}

impl Default for ListSessionTable {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub(crate) struct DrainOutcome {
    pub entries: Vec<ListEntry>,
    pub exhausted: bool,
}

#[derive(Debug)]
pub(crate) enum ListSessionError {
    /// The cursor does not name any active session (expired or fabricated by the client).
    UnknownCursor,
    /// The underlying stream yielded an error. The session has been removed from the table;
    /// any successful entries pulled before the error are dropped on the floor because the
    /// wire response shape has no partial-progress slot. If we ever grow such a slot, return
    /// the prior entries here.
    StreamError(crate::error::StorageError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;

    fn entry(key: &str) -> StorageResult<ListEntry> {
        Ok(ListEntry {
            key: key.to_string(),
            size: 1,
            etag: None,
            last_modified_ms: None,
        })
    }

    #[tokio::test]
    async fn drain_returns_entries_in_pages_and_finishes_when_exhausted() {
        let table = ListSessionTable::new();
        let stream = stream::iter(vec![entry("a"), entry("b"), entry("c")]).boxed();
        let cursor = table.insert(stream);

        let page1 = table.drain(&cursor, 2).await.unwrap();
        assert_eq!(page1.entries.len(), 2);
        assert!(!page1.exhausted);
        assert_eq!(table.len(), 1, "session must remain after a partial drain");

        let page2 = table.drain(&cursor, 2).await.unwrap();
        assert_eq!(page2.entries.len(), 1);
        assert!(page2.exhausted);
        assert_eq!(table.len(), 0, "exhausted session must be removed");
    }

    #[tokio::test]
    async fn drain_reports_unknown_cursor_after_idle_expiry() {
        let table = ListSessionTable::with_idle_ttl(Duration::from_millis(1));
        let cursor = table.insert(stream::iter(vec![entry("a")]).boxed());

        tokio::time::sleep(Duration::from_millis(10)).await;
        let err = table.drain(&cursor, 1).await.unwrap_err();
        assert!(matches!(err, ListSessionError::UnknownCursor));
    }

    #[tokio::test]
    async fn forget_removes_session_immediately() {
        let table = ListSessionTable::new();
        let cursor = table.insert(stream::iter(vec![entry("a")]).boxed());
        table.forget(&cursor);
        assert_eq!(table.len(), 0);
        let err = table.drain(&cursor, 1).await.unwrap_err();
        assert!(matches!(err, ListSessionError::UnknownCursor));
    }

    #[tokio::test]
    async fn drain_consumes_session_on_stream_error() {
        use crate::error::StorageError;
        let table = ListSessionTable::new();
        let cursor = table.insert(
            stream::iter(vec![
                entry("a"),
                Err(StorageError::backend("simulated mid-stream failure")),
                entry("never-reached"),
            ])
            .boxed(),
        );

        let err = table.drain(&cursor, 5).await.unwrap_err();
        assert!(
            matches!(err, ListSessionError::StreamError(_)),
            "expected StreamError, got {err:?}"
        );
        assert_eq!(
            table.len(),
            0,
            "errored session must be removed from the table"
        );
    }
}
