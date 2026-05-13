//! Paginated list iterator over object keys.

use crate::error::StorageResult;
use crate::object::ListEntry;
use crate::protocol::ListCursor;

use super::StorageClient;

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
    store_id: String,
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
        store_id: String,
        bucket: String,
        prefix: Option<String>,
    ) -> Self {
        Self {
            client,
            store_id,
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
            },
        };
        let page = self.client.list_page(
            self.store_id.clone(),
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
                ListIterState::BeforeFirstPage | ListIterState::Open(_) => match self.refill() {
                    Ok(()) => continue,
                    Err(error) => {
                        return Some(Err(error));
                    },
                },
            }
        }
    }
}
