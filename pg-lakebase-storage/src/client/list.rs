//! Connection-bound object listing APIs.

use std::thread;

use crate::error::StorageResult;
use crate::object::ListEntry;
use crate::protocol::{ListCursor, WireRequestPayload, WireResponsePayload};

use super::StorageClient;

pub(super) struct ListPage {
    pub(super) entries: Vec<ListEntry>,
    pub(super) next_cursor: Option<ListCursor>,
}

impl StorageClient {
    /// Fetches a single page of a `list` operation.
    ///
    /// `page_size = 0` lets the server pick a default. A returned cursor is
    /// connection-local and must be passed to this same client generation.
    pub(super) fn list_page(
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
    pub(super) fn close_list_cursor(&self, cursor: ListCursor) -> StorageResult<()> {
        let (response, _) = self.request(WireRequestPayload::CloseList { cursor })?;
        match response {
            WireResponsePayload::CloseList => Ok(()),
            other => self.reject_unexpected("close-list", &other),
        }
    }

    /// Best-effort cursor release for cleanup paths such as `Drop`.
    ///
    /// Cleanup never reports an error to its caller. A connection that cannot
    /// complete the bounded request is invalidated so connection teardown
    /// releases every remaining server-side list session.
    fn close_list_cursor_cleanup(&self, cursor: ListCursor) {
        if thread::panicking() || !self.is_usable() {
            let _ = self.invalidate();
            return;
        }

        match self.request_cleanup(WireRequestPayload::CloseList { cursor }) {
            Ok((WireResponsePayload::CloseList, _)) => {}
            Ok((other, _)) => {
                let _ = self.reject_unexpected::<()>("close-list", &other);
            }
            Err(_) => {
                let _ = self.invalidate();
            }
        }
    }

    /// Returns an iterator over every object whose key starts with `prefix`.
    pub fn list(
        &self,
        bucket: impl Into<String>,
        prefix: Option<&str>,
    ) -> ListIter {
        ListIter::new(self.list_session(bucket, prefix, 0))
    }

    /// Starts a page-oriented listing bound to this connection generation.
    ///
    /// The returned session owns a client clone so every page and cursor cleanup
    /// uses the same connection that created the server-side list session.
    pub fn list_session(
        &self,
        bucket: impl Into<String>,
        prefix: Option<&str>,
        page_size: u32,
    ) -> ListSession {
        ListSession::new(
            self.clone(),
            bucket.into(),
            prefix.map(str::to_string),
            page_size,
        )
    }
}

/// Connection-bound, page-oriented object listing.
///
/// A server cursor is meaningful only on the connection that created it. This
/// session retains that exact client generation for pagination, explicit close,
/// and `Drop` cleanup.
pub struct ListSession {
    client: StorageClient,
    bucket: String,
    prefix: Option<String>,
    page_size: u32,
    cursor: Option<ListCursor>,
    exhausted: bool,
}

impl ListSession {
    fn new(
        client: StorageClient,
        bucket: String,
        prefix: Option<String>,
        page_size: u32,
    ) -> Self {
        Self {
            client,
            bucket,
            prefix,
            page_size,
            cursor: None,
            exhausted: false,
        }
    }

    /// Fetches the next page, or returns `None` after the listing is exhausted.
    ///
    /// On an error the current cursor remains owned by this session so `Drop`
    /// can release it through the same connection generation.
    pub fn next_page(&mut self) -> StorageResult<Option<Vec<ListEntry>>> {
        if self.exhausted {
            return Ok(None);
        }

        let page = self.client.list_page(
            self.bucket.clone(),
            self.prefix.as_deref(),
            self.cursor.clone(),
            self.page_size,
        )?;
        self.cursor = page.next_cursor;
        self.exhausted = self.cursor.is_none();
        Ok(Some(page.entries))
    }

    /// Returns whether the server reported that the last fetched page was final.
    pub const fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    /// Releases an unfinished server-side listing immediately.
    pub fn close(&mut self) -> StorageResult<()> {
        if let Some(cursor) = self.cursor.as_ref().cloned() {
            self.client.close_list_cursor(cursor)?;
            self.cursor = None;
        }
        self.exhausted = true;
        Ok(())
    }
}

impl Drop for ListSession {
    fn drop(&mut self) {
        if let Some(cursor) = self.cursor.take() {
            self.client.close_list_cursor_cleanup(cursor);
        }
    }
}

/// Streaming iterator over a `list` operation. Constructed via [`StorageClient::list`].
///
/// The iterator owns the listing's pagination state on the client side: it buffers one page at
/// a time and refills transparently as entries are consumed. Pages are fetched at the server's
/// default page size (use [`StorageClient::list_session`] for page-oriented access with a custom
/// page size).
///
/// Iteration stops after the final page (`next_cursor` returned by the server is `None`) or at
/// the first error. Once exhausted, [`Iterator::next`] returns `None` indefinitely.
/// Dropping an unfinished iterator performs bounded best-effort cursor cleanup.
pub struct ListIter {
    session: ListSession,
    /// Buffered page entries, stored in reverse order so we can `pop()` in O(1).
    buffered: Vec<ListEntry>,
    failed: bool,
}

impl ListIter {
    fn new(session: ListSession) -> Self {
        Self {
            session,
            buffered: Vec::new(),
            failed: false,
        }
    }

    fn refill(&mut self) -> StorageResult<bool> {
        let Some(entries) = self.session.next_page()? else {
            return Ok(false);
        };
        self.buffered = entries.into_iter().rev().collect();
        Ok(true)
    }
}

impl Iterator for ListIter {
    type Item = StorageResult<ListEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        loop {
            if let Some(entry) = self.buffered.pop() {
                return Some(Ok(entry));
            }
            match self.refill() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(error) => {
                    self.failed = true;
                    return Some(Err(error));
                }
            }
        }
    }
}
