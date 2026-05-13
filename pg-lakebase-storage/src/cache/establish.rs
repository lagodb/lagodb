//! Per-key residency-establishment single-flight.
//!
//! An OPEN that observes a cache miss under the per-object lock goes through one of two roles:
//!
//! * the **leader** — the first caller to claim the per-object establishment slot, responsible
//!   for issuing the backend HEAD (and, for small objects, the subsequent GET) and installing a
//!   residency via [`crate::cache::CacheManager::admit_small`] /
//!   [`crate::cache::CacheManager::admit_large`];
//! * a **follower** — every other caller that arrives while the leader is still running. The
//!   follower waits for the leader to publish an outcome; on success it retries the outer
//!   `lookup_for_open` loop (which is now guaranteed to observe a residency because the leader
//!   installs index / session state **before** signalling success), and on failure it returns
//!   a reconstructed equivalent of the leader's error to its client.
//!
//! # Cache-design invariants upheld
//!
//! 1. `size`/`etag` are observed by the leader exactly once and become the frozen fact for this
//!    cache lifecycle. Followers never run a second HEAD and never see the leader's raw
//!    `ObjectInfo`; they only observe the residency the leader produced. This is what keeps the
//!    "no reconcile, no multiple versions" rule honest in the face of concurrent opens.
//! 2. No generation: the slot stores a single [`std::sync::Weak<EstablishFlight>`] and is
//!    cleared automatically when the last leader/follower drops. A failed leader leaves no
//!    residency behind, and the next OPEN is elected as a fresh leader without any ghost state
//!    to reconcile against.
//! 3. External invalidation is untouched: establishment only runs on the miss path, never
//!    reconstructs existing residency, and never fires when any residency is visible under the
//!    object lock.
//!
//! # Outcome ordering contract (load-bearing)
//!
//! [`EstablishLeader::succeed`] must be called **after** the admit_small / admit_large call
//! that installs the residency has returned `Ok`. This ordering is what lets followers trust
//! that a `Succeeded` outcome followed by a retried `lookup_for_open` will observe a hit: by
//! the time the leader publishes `Succeeded`, either the small-KV row (insert-if-absent) or
//! the live [`crate::cache::LargeFillSession`] (attach_or_join under the fill-slot mutex) is
//! already visible to the next `lookup_for_open` on the same key.
//!
//! See [`EstablishWaiter::wait`] for the symmetric follower contract.

use std::sync::{Arc, Weak};

use tokio::sync::watch;

use crate::cache::object_state::CacheActivityGuard;
use crate::error::{StorageError, StorageErrorKind, StorageResult};
use crate::object::ObjectLocation;

/// Result published by the leader to every follower. Kept `Clone` so followers can read the
/// `watch` channel without wrapping the outcome in an extra `Arc`.
///
/// `Failed` carries the wire-level [`StorageErrorKind`] + message pair rather than a live
/// [`StorageError`] on purpose: [`StorageError`] is not `Clone` (its source chain uses
/// `Box<dyn Error + Send + Sync>`), and the follower's client only needs an equivalent error
/// to act on. The leader has already logged the full chain on its own error path, so no
/// diagnostic information is lost — only the source chain gets flattened into the display
/// message via [`StorageError::from_wire`].
#[derive(Clone, Debug)]
enum EstablishOutcome {
    Succeeded,
    Failed { kind: StorageErrorKind, message: String },
}

/// Shared coordination point between one leader and any number of followers. The `Arc` is
/// owned by the leader and by every outstanding follower; the [`PerObjectState`] slot holds
/// only a [`Weak`], so the flight lives exactly as long as someone still depends on its
/// outcome.
pub(crate) struct EstablishFlight {
    outcome: watch::Sender<Option<EstablishOutcome>>,
}

impl EstablishFlight {
    fn new() -> Self {
        let (outcome, _) = watch::channel(None);
        Self { outcome }
    }

    fn publish(&self, outcome: EstablishOutcome) {
        self.outcome.send_replace(Some(outcome));
    }

    fn subscribe(&self) -> watch::Receiver<Option<EstablishOutcome>> {
        self.outcome.subscribe()
    }
}

/// Role assigned under the per-object lock by [`PerObjectState::elect_or_join_establish`].
///
/// The per-object lock is held during election so every concurrent `lookup_for_open` on the
/// same key sees a consistent slot state: at most one caller observes an empty slot and is
/// handed the `Leader` role.
pub(crate) enum EstablishRole {
    /// Caller is the elected leader. Must drive HEAD + admit and finalize via
    /// [`EstablishLeader::succeed`] or [`EstablishLeader::fail`].
    Leader(EstablishLeader),
    /// Caller joined an in-progress establishment. Must call [`EstablishWaiter::wait`] before
    /// retrying `lookup_for_open`.
    Follower(EstablishWaiter),
}

/// The elected leader for a single miss → residency window on one key.
///
/// Absorbs the responsibilities of the former `AdmissionToken`: carries both the key under
/// establishment and the admission-kind [`CacheActivityGuard`] that keeps eviction /
/// invalidation out while HEAD + GET run between miss observation and admit. Passed by
/// reference into [`crate::cache::CacheManager::admit_small`] /
/// [`crate::cache::CacheManager::admit_large`] so the final `succeed()` / `fail()` decision
/// stays with the caller that owns the outer OPEN flow.
///
/// # Drop guard
///
/// If a leader is dropped without calling [`Self::succeed`] or [`Self::fail`] (panic, early
/// return, future cancellation), the drop handler publishes a synthetic
/// [`StorageErrorKind::Cache`] failure so followers unblock and return a well-typed error
/// instead of hanging on the watch channel. Callers that explicitly finalize the leader set
/// `finished = true` and the drop handler becomes a no-op.
pub(crate) struct EstablishLeader {
    key: ObjectLocation,
    flight: Arc<EstablishFlight>,
    /// Held purely for its [`Drop`] effect: keeps `is_active(key)` true across the
    /// miss → HEAD → admit window so eviction and invalidation stay out. Also keeps the
    /// underlying [`PerObjectState`] alive for the lifetime of this leader, so the admit
    /// paths re-acquire the same state instance through the registry lookup. Never accessed
    /// directly; `#[allow(dead_code)]` is the explicit signal that this field is
    /// load-bearing RAII state rather than an unused remnant.
    #[allow(dead_code)]
    admission: CacheActivityGuard,
    finished: bool,
}

impl EstablishLeader {
    /// The key this leader is establishing. Replaces the old `AdmissionToken.key` field.
    pub(crate) fn key(&self) -> &ObjectLocation {
        &self.key
    }

    /// Finalize the leader after the residency has been installed.
    ///
    /// Must be called only after `admit_small` / `admit_large` returned `Ok` — see the
    /// module-level outcome-ordering contract. Marks the leader finished so the drop guard
    /// does not publish a synthetic failure.
    pub(crate) fn succeed(mut self) {
        self.flight.publish(EstablishOutcome::Succeeded);
        self.finished = true;
    }

    /// Finalize the leader after HEAD / GET / admit returned an error. Publishes the error's
    /// [`StorageErrorKind`] + display message so every follower surfaces an equivalent error.
    ///
    /// # Cross-module contract with [`StorageError`]
    ///
    /// Follower-side reconstruction uses [`StorageError::from_wire`] on `(kind, message)`.
    /// The fidelity guarantee is:
    ///
    /// > For any error a leader can produce on the HEAD / GET / admit path,
    /// > `StorageError::from_wire(e.kind(), e.wire_message()).kind() == e.kind()`, and
    /// > `wire_message()` round-trips unchanged for every variant **except**
    /// > [`StorageError::Io`] — which carries a non-optional `io::Error` source field
    /// > and cannot faithfully reconstruct its `(context, source)` split from a single
    /// > flattened wire message. For `Io`, only `kind()` is preserved verbatim; the
    /// > follower's `wire_message()` may gain a reconstruction prefix.
    ///
    /// In practice the HEAD / GET / admit path produces `Backend` / `NotFound` /
    /// `Cache` errors, not `Io`, so the `Io` carve-out is not a live concern on the
    /// single-flight error path today. When adding new [`StorageError`] variants or
    /// changing existing ones, keep the `Option<Source>` pattern used by
    /// `Protocol` / `Backend` / `Cache` so `wire_message()` round-trips cleanly.
    ///
    /// The source chain is intentionally not preserved — see the module-level docs for the
    /// rationale.
    pub(crate) fn fail(mut self, error: &StorageError) {
        self.flight.publish(EstablishOutcome::Failed {
            kind: error.kind(),
            message: error.wire_message(),
        });
        self.finished = true;
    }
}

impl Drop for EstablishLeader {
    fn drop(&mut self) {
        if !self.finished {
            self.flight.publish(EstablishOutcome::Failed {
                kind: StorageErrorKind::Cache,
                message: format!("residency establishment for {} dropped without outcome", self.key),
            });
        }
    }
}

/// A follower waiting for the leader's outcome. One per concurrent OPEN that raced into an
/// already-running establishment.
pub(crate) struct EstablishWaiter {
    flight: Arc<EstablishFlight>,
}

impl EstablishWaiter {
    /// Block until the leader publishes an outcome.
    ///
    /// # Return semantics
    ///
    /// * `Ok(())` — the leader succeeded. By the outcome-ordering contract the residency
    ///   (small-KV row or live fill session) is already observable to a retried
    ///   `lookup_for_open` on the same key, so callers should re-enter the lookup loop and
    ///   expect a hit.
    /// * `Err(e)` — either the leader explicitly failed (HEAD / GET / admit error) or it was
    ///   dropped without a verdict. Either way this follower abandons its own attempt and
    ///   surfaces `e` to its client; a fresh OPEN on the same key is free to become the next
    ///   leader.
    pub(crate) async fn wait(self) -> StorageResult<()> {
        let mut rx = self.flight.subscribe();
        loop {
            if let Some(outcome) = rx.borrow_and_update().clone() {
                return match outcome {
                    EstablishOutcome::Succeeded => Ok(()),
                    EstablishOutcome::Failed { kind, message } => Err(StorageError::from_wire(kind, message)),
                };
            }
            if rx.changed().await.is_err() {
                // Sender dropped without publishing. The Drop guard on `EstablishLeader` is the
                // normal backstop, so reaching here means the flight's sender was torn down in
                // a way that bypassed the leader handle entirely (for example, a panic during
                // election before the leader was returned). Surface a cache error rather than
                // hang the follower forever.
                return Err(StorageError::cache("residency establishment flight closed without outcome"));
            }
        }
    }
}

/// Outcome of claiming or joining a flight under [`PerObjectState`]'s establishment slot.
///
/// Pure slot operation — no activity-counter side effects are performed here so the caller
/// can acquire and release the slot lock on its own schedule, then mint the admission guard
/// for a fresh leader outside the slot lock window. Keeping the slot lock scope narrow is
/// what preserves the "at most one synchronous mutex held at a time" invariant on
/// [`PerObjectState`].
pub(super) enum FlightClaim {
    /// The slot's weak reference upgraded — the caller joins as a follower on the existing
    /// flight.
    Existing(Arc<EstablishFlight>),
    /// The slot was empty; a fresh flight was installed — the caller is the leader and
    /// still needs to mint its admission guard (outside the slot lock).
    Fresh(Arc<EstablishFlight>),
}

/// Install-or-observe the flight stored behind `slot`. Separated from leader construction
/// so callers can release the slot lock before touching any other per-object state.
pub(super) fn claim_or_join(slot: &mut Weak<EstablishFlight>) -> FlightClaim {
    if let Some(flight) = slot.upgrade() {
        return FlightClaim::Existing(flight);
    }
    let flight = Arc::new(EstablishFlight::new());
    *slot = Arc::downgrade(&flight);
    FlightClaim::Fresh(flight)
}

impl EstablishLeader {
    pub(super) fn new(key: ObjectLocation, flight: Arc<EstablishFlight>, admission: CacheActivityGuard) -> Self {
        Self {
            key,
            flight,
            admission,
            finished: false,
        }
    }
}

impl EstablishWaiter {
    pub(super) fn new(flight: Arc<EstablishFlight>) -> Self {
        Self { flight }
    }
}
