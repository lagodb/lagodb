//! Paginated list iterator over object keys.

use crate::error::StorageResult;
use crate::object::ListEntry;
use crate::protocol::{ListCursor, WireRequestPayload, WireResponsePayload};

use super::{ListPage, StorageClient};

impl StorageClient {
    /// Fetches a single page of a `list` operation.
    ///
    /// `page_size = 0` lets the server pick a default. A returned cursor is
    /// connection-local and must be passed to this same client generation.
    pub fn list_page(
        &self,
        bucket: impl Into<String>,
        prefix: Option<&str>,
        cursor: Option<ListCursor>,
        page_size: u32,
    ) -> StorageResult<ListPage> {
        let (response, _) = self.request(WireRequestPayload::List {
            bucket: bucket.into(),
            prefix: prefix.map(str::to_string),
            page_size,
            cursor,
        })?;
        match response {
            WireResponsePayload::List {
                entries,
                next_cursor,
            } => Ok(ListPage {
                entries: entries
                    .into_iter()
                    .map(|entry| ListEntry {
                        key: entry.key,
                        size: entry.size,
                        etag: entry.etag,
                        last_modified_ms: entry.last_modified_ms,
                    })
                    .collect(),
                next_cursor,
            }),
            other => self.reject_unexpected("list", &other),
        }
    }

    /// Releases a retained list cursor. Closing an expired cursor is idempotent.
    pub fn close_list_cursor(&self, cursor: ListCursor) -> StorageResult<()> {
        let (response, _) = self.request(WireRequestPayload::CloseList { cursor })?;
        match response {
            WireResponsePayload::CloseList => Ok(()),
            other => self.reject_unexpected("close-list", &other),
        }
    }

    /// Returns an iterator over every object whose key starts with `prefix`.
    pub fn list(
        &self,
        bucket: impl Into<String>,
        prefix: Option<&str>,
    ) -> ListIter<'_> {
        ListIter::new(self, bucket.into(), prefix.map(str::to_string))
    }
}

/// Streaming iterator over a `list` operation. Constructed via [`StorageClient::list`].
///
/// The iterator owns the listing's pagination state on the client side: it buffers one page at
/// a time and refills transparently as entries are consumed. Pages are fetched at the server's
/// default page size (use [`StorageClient::list_page`] if you need a custom page size or to
/// drive cursor handling yourself).
///
/// Iteration stops after the final page (`next_cursor` returned by the server is `None`) or at
/// the first error. Once exhausted, [`Iterator::next`] returns `None` indefinitely.
pub struct ListIter<'a> {
    client: &'a StorageClient,
    bucket: String,
    prefix: Option<String>,
    /// Pagination state. `BeforeFirstPage` is the initial state; the iterator transitions to
    /// `Open(cursor)` after the first refill that returned a `next_cursor`, to `Exhausted` after
    /// any refill that returned `next_cursor = None`, and to `Failed` at the first error.
    state: ListIterState,
    /// Buffered page entries, stored in reverse order so we can `pop()` in O(1).
    buffered: Vec<ListEntry>,
}

enum ListIterState {
    BeforeFirstPage,
    Open(ListCursor),
    Exhausted,
    Failed,
}

impl<'a> ListIter<'a> {
    pub(super) fn new(
        client: &'a StorageClient,
        bucket: String,
        prefix: Option<String>,
    ) -> Self {
        Self {
            client,
            bucket,
            prefix,
            state: ListIterState::BeforeFirstPage,
            buffered: Vec::new(),
        }
    }

    fn refill(&mut self) -> StorageResult<()> {
        let cursor = match std::mem::replace(&mut self.state, ListIterState::Failed) {
            ListIterState::BeforeFirstPage => None,
            ListIterState::Open(cursor) => Some(cursor),
            ListIterState::Exhausted | ListIterState::Failed => {
                unreachable!("refill called in terminal state")
            }
        };
        let page = self.client.list_page(
            self.bucket.clone(),
            self.prefix.as_deref(),
            cursor,
            0,
        )?;
        self.buffered = page.entries.into_iter().rev().collect();
        self.state = match page.next_cursor {
            Some(cursor) => ListIterState::Open(cursor),
            None => ListIterState::Exhausted,
        };
        Ok(())
    }
}

impl Iterator for ListIter<'_> {
    type Item = StorageResult<ListEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(entry) = self.buffered.pop() {
                return Some(Ok(entry));
            }
            match &self.state {
                ListIterState::Exhausted | ListIterState::Failed => return None,
                ListIterState::BeforeFirstPage | ListIterState::Open(_) => {
                    match self.refill() {
                        Ok(()) => continue,
                        Err(error) => {
                            return Some(Err(error));
                        }
                    }
                }
            }
        }
    }
}
