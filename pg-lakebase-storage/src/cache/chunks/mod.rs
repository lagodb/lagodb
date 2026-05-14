//! Process-local large-object fills backed by temporary partial files.
//!
//! # Lifecycle (ownership-driven, no external finalize contract)
//!
//! A [`LargeFillSession`] is created by
//! [`crate::cache::object_state::ObjectStateRegistry::attach_or_join_fill_session`] and returned
//! as `Arc<LargeFillSession>`. Every consumer that needs the session (open handle, in-flight
//! chunk writer) holds its own `Arc`. The per-object state stores only a
//! [`std::sync::Weak`] reference, so the session lives **exactly** as long as some consumer
//! cares about it.
//!
//! When the last `Arc<LargeFillSession>` drops:
//! * if the fill already committed (`completed == true`), there is nothing to do — promotion already moved the partial
//!   to the complete path, inserted the metadata row, and cleared the fill slot under the object lock
//! * otherwise, [`LargeFillSession::drop`](session::LargeFillSession) enqueues a [`reaper::ReapRequest`] onto an
//!   internal reaper channel. The request carries the `Arc<PerObjectState>` the session was attached to, pinning the
//!   state across the reap window. A background task consumes the channel under the per-object lock, aborts the
//!   session state (waking any still-subscribed chunk waiters), unlinks the partial payload, and clears the fill slot
//!   if the nonce still matches
//!
//! # Why a reaper and not async Drop
//!
//! Cleaning up a partial fill needs an async runtime (file unlink) and the per-object lock,
//! neither of which a synchronous [`Drop`] can hold correctly. The reaper is the async finalizer
//! that [`Drop`] cannot be. All [`Drop`] does is push a plain-old-data request into an unbounded
//! mpsc — infallible from the caller's point of view.
//!
//! # Partial progress is intentionally memory-only
//!
//! Durable metadata is written only after every chunk has landed and the partial file has been
//! renamed to the complete-file path.
//!
//! # Module layout
//!
//! - [`flight`]   — per-chunk coordination primitives (leader/follower, watch channel)
//! - [`session`]  — [`LargeFillSession`] and its state machine
//! - [`reaper`]   — channel plumbing and reaper task entry point
//! - [`ops`]      — [`crate::cache::CacheManager`] methods that drive fills through the above

mod flight;
mod ops;
mod reaper;
mod session;

#[cfg(test)]
mod tests;

pub(crate) use flight::ChunkFillClaim;
pub(crate) use reaper::{ReaperHandle, ReaperInbox, reaper_channel};
pub(crate) use session::LargeFillSession;
