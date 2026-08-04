//! Connection-local paginated list handlers.

use super::StorageService;
use super::command::{CloseListCommand, ListCommand};
use super::list_session::{DEFAULT_PAGE_SIZE, ListSessionError, MAX_PAGE_SIZE};
use super::reply::{CommandOutput, ListOutput, ServiceReply};
use crate::cache::CacheIndex;
use crate::error::{StorageError, StorageResult};
use crate::protocol::WireListEntry;
use crate::session::StorageContext;

impl<I: CacheIndex> StorageService<I> {
    pub(super) async fn handle_list(
        &self,
        context: &StorageContext<I>,
        command: ListCommand,
    ) -> StorageResult<ServiceReply> {
        let backend = context.attached()?.backend();
        let cursor = match command.cursor {
            Some(cursor) => cursor,
            None => {
                let stream = backend.list(&command.bucket, command.prefix.as_deref());
                context.list_sessions.insert(stream)
            }
        };

        let drain = context
            .list_sessions
            .drain(&cursor, clamp_page_size(command.page_size) as usize)
            .await;
        let drain = match drain {
            Ok(drain) => drain,
            Err(ListSessionError::UnknownCursor) => {
                return Err(StorageError::expired_cursor(
                    "unknown or expired list cursor",
                ));
            }
            Err(ListSessionError::StreamError(error)) => return Err(error),
        };

        let entries = drain
            .entries
            .into_iter()
            .map(|entry| WireListEntry {
                key: entry.key,
                size: entry.size,
                etag: entry.etag,
                last_modified_ms: entry.last_modified_ms,
            })
            .collect();
        let next_cursor = (!drain.exhausted).then_some(cursor);
        Ok(ServiceReply::new(CommandOutput::List(ListOutput {
            entries,
            next_cursor,
        })))
    }

    pub(super) fn handle_close_list(
        &self,
        context: &StorageContext<I>,
        command: CloseListCommand,
    ) -> StorageResult<ServiceReply> {
        context.list_sessions.forget(&command.cursor);
        Ok(ServiceReply::new(CommandOutput::CloseList))
    }
}

fn clamp_page_size(page_size: u32) -> u32 {
    match page_size {
        0 => DEFAULT_PAGE_SIZE,
        value => value.min(MAX_PAGE_SIZE),
    }
}
