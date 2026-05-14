use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::timeout;

use super::flight::ChunkFillClaim;
use super::reaper::{ReaperInbox, reaper_channel};
use super::session::LargeFillSession;
use crate::cache::meta::CachedObjectMeta;
use crate::cache::object_state::{CacheActivityKind, ObjectStateRegistry};
use crate::error::StorageError;
use crate::object::{ObjectInfo, ObjectLocation};

fn test_session(
    key: ObjectLocation,
    chunks: usize,
) -> (Arc<LargeFillSession>, ReaperInbox) {
    let (reaper, inbox) = reaper_channel();
    let (registry, _registry_inbox) = ObjectStateRegistry::new();
    let state = registry.get_or_create(&key);
    let session = Arc::new(LargeFillSession::new(
        state,
        ObjectInfo {
            size: 8,
            etag: None,
        },
        chunks,
        PathBuf::from("/tmp/session-partial"),
        1,
        reaper,
    ));
    (session, inbox)
}

#[tokio::test]
async fn abort_marks_inflight_chunk_waiters_failed() {
    let key = ObjectLocation::new("default", "bucket", "file").unwrap();
    let (session, _inbox) = test_session(key, 2);
    let leader = match session.claim_chunk(0).await.unwrap() {
        ChunkFillClaim::Leader(leader) => leader,
        ChunkFillClaim::Complete | ChunkFillClaim::Follower(_) => {
            panic!("expected first claimant to lead")
        }
    };
    let waiter = match session.claim_chunk(0).await.unwrap() {
        ChunkFillClaim::Follower(waiter) => waiter,
        ChunkFillClaim::Complete | ChunkFillClaim::Leader(_) => {
            panic!("expected second claimant to wait")
        }
    };
    let waiter_task = tokio::spawn(async move { waiter.wait().await });

    session.abort().await;

    let waiter_result = timeout(Duration::from_secs(1), waiter_task)
        .await
        .expect("waiter should wake on abort")
        .expect("waiter task should not panic")
        .unwrap();
    assert!(!waiter_result);
    assert!(matches!(
        session.claim_chunk(0).await,
        Err(StorageError::CacheFillAborted { .. })
    ));
    drop(leader);
}

/// A live fill session must remain resolvable from the per-object state as long as any Arc is
/// held. This mirrors the "Weak in the slot" lifetime rule the registry was built on.
#[tokio::test]
async fn fill_slot_stays_resolvable_while_any_arc_is_held() {
    let (registry, _inbox) = ObjectStateRegistry::new();
    let key = ObjectLocation::new("default", "bucket", "file").unwrap();
    let state = registry.get_or_create(&key);
    let session = registry
        .attach_or_join_fill_session(
            &state,
            ObjectInfo {
                size: 8,
                etag: None,
            },
            2,
            PathBuf::from("/tmp/session-partial"),
        )
        .await
        .unwrap();

    let extra = session.clone();
    drop(session);

    let current = state
        .live_fill_session()
        .expect("fill slot should still resolve while any Arc survives");
    assert!(Arc::ptr_eq(&current, &extra));
}

/// When the last `Arc` drops, the stored `Weak` must expire. The reap request enqueued on drop
/// is observed separately by `reap_request_pins_per_object_state_after_session_drop` below.
#[tokio::test]
async fn fill_slot_weak_expires_when_last_arc_drops() {
    let (registry, _inbox) = ObjectStateRegistry::new();
    let key = ObjectLocation::new("default", "bucket", "file").unwrap();
    let state = registry.get_or_create(&key);
    let session = registry
        .attach_or_join_fill_session(
            &state,
            ObjectInfo {
                size: 8,
                etag: None,
            },
            2,
            PathBuf::from("/tmp/session-partial"),
        )
        .await
        .unwrap();

    drop(session);
    assert!(
        state.live_fill_session().is_none(),
        "stale Weak in the fill slot should no longer upgrade",
    );
}

/// A completed session must **not** send a reap request — promotion already did all the
/// disk/slot work under the per-object lock, so the reaper has nothing to do.
#[tokio::test]
async fn completed_session_drop_does_not_enqueue_reap() {
    let (reaper, mut inbox) = reaper_channel();
    let (registry, _registry_inbox) = ObjectStateRegistry::new();
    let key = ObjectLocation::new("default", "bucket", "file").unwrap();
    let state = registry.get_or_create(&key);
    let info = ObjectInfo {
        size: 8,
        etag: None,
    };
    let session = Arc::new(LargeFillSession::new(
        state,
        info.clone(),
        2,
        PathBuf::from("/tmp/session-partial"),
        1,
        reaper,
    ));

    // Simulate a successful promotion: mark_complete flips the completed marker that Drop
    // observes.
    session
        .mark_complete(CachedObjectMeta::complete(key.clone(), info))
        .await
        .unwrap();

    drop(session);

    assert!(
        inbox.rx.try_recv().is_err(),
        "completed session Drop must not enqueue a reap request",
    );
}

/// After the last `Arc` drops and the slot's `Weak` expires, a subsequent
/// `attach_or_join_fill_session` for the same key must mint a **new** session (new `nonce`).
#[tokio::test]
async fn attach_mints_new_session_after_weak_expires() {
    let (registry, _inbox) = ObjectStateRegistry::new();
    let key = ObjectLocation::new("default", "bucket", "file").unwrap();
    let info = ObjectInfo {
        size: 8,
        etag: None,
    };
    let state = registry.get_or_create(&key);

    let first = registry
        .attach_or_join_fill_session(
            &state,
            info.clone(),
            2,
            PathBuf::from("/tmp/p1"),
        )
        .await
        .unwrap();
    let first_nonce = first.nonce();
    drop(first);

    let second = registry
        .attach_or_join_fill_session(&state, info, 2, PathBuf::from("/tmp/p2"))
        .await
        .unwrap();
    assert_ne!(
        second.nonce(),
        first_nonce,
        "second attach after Weak expired must mint a fresh session",
    );
    assert_eq!(second.partial_path(), PathBuf::from("/tmp/p2").as_path());
}

/// When the fill slot has been replaced (by a newer session) or cleared (by invalidate),
/// `clear_fill_slot_if_matches` called with the stale nonce must be a no-op. This is the
/// invariant that lets the reaper process stale requests without clobbering the new generation.
#[tokio::test]
async fn clear_fill_slot_if_matches_rejects_stale_nonce() {
    let (registry, _inbox) = ObjectStateRegistry::new();
    let key = ObjectLocation::new("default", "bucket", "file").unwrap();
    let info = ObjectInfo {
        size: 8,
        etag: None,
    };
    let state = registry.get_or_create(&key);

    let first = registry
        .attach_or_join_fill_session(
            &state,
            info.clone(),
            2,
            PathBuf::from("/tmp/p1"),
        )
        .await
        .unwrap();
    let stale_nonce = first.nonce();
    drop(first);

    // A new session has replaced the slot.
    let second = registry
        .attach_or_join_fill_session(&state, info, 2, PathBuf::from("/tmp/p2"))
        .await
        .unwrap();
    let live_nonce = second.nonce();

    // A reap request carrying the stale nonce must not touch the live slot.
    state.clear_fill_slot_if_matches(stale_nonce);
    assert!(
        state.fill_slot_nonce_matches(live_nonce),
        "live slot must survive stale clear_fill_slot_if_matches"
    );
    let resolved = state
        .live_fill_session()
        .expect("live session still registered");
    assert!(Arc::ptr_eq(&resolved, &second));

    // The matching nonce, of course, still removes the slot.
    state.clear_fill_slot_if_matches(live_nonce);
    assert!(state.live_fill_session().is_none());
}

/// `clear_fill_slot` (unconditional, used by invalidate) wipes the slot regardless of nonce.
/// A subsequent reap request with the old nonce then harmlessly bounces off the nonce check.
#[tokio::test]
async fn clear_fill_slot_unconditional_after_invalidate() {
    let (registry, _inbox) = ObjectStateRegistry::new();
    let key = ObjectLocation::new("default", "bucket", "file").unwrap();
    let info = ObjectInfo {
        size: 8,
        etag: None,
    };
    let state = registry.get_or_create(&key);

    let session = registry
        .attach_or_join_fill_session(&state, info, 2, PathBuf::from("/tmp/p1"))
        .await
        .unwrap();
    let nonce = session.nonce();

    // Invalidation wipes the slot while the session's Arc is still live.
    state.clear_fill_slot();
    assert!(
        state.live_fill_session().is_none(),
        "invalidate clears the slot unconditionally"
    );

    // The reaper that later fires on the surviving session's Drop must see "nonce does not
    // match" and do nothing.
    assert!(!state.fill_slot_nonce_matches(nonce));

    drop(session);
}

#[tokio::test]
async fn claim_partial_bootstrap_is_exactly_once() {
    let key = ObjectLocation::new("default", "bucket", "file").unwrap();
    let (session, _inbox) = test_session(key, 2);

    assert!(
        session.claim_partial_bootstrap(),
        "first caller must own the truncate"
    );
    assert!(
        !session.claim_partial_bootstrap(),
        "subsequent chunks must preserve prior writes"
    );
    assert!(!session.claim_partial_bootstrap());
}

/// **Load-bearing lifetime rule.**
///
/// When the last outstanding `Arc<LargeFillSession>` drops but no other lease, guard, or
/// reap request is holding the per-object state, the state must still be kept alive by the
/// reap request until the reaper has had a chance to run. Otherwise a concurrent OPEN could
/// recreate a fresh state for the same key and the reaper's nonce check would consult the wrong
/// slot, leaking the partial on disk. This test exercises the contract: before pulling the
/// request out of the inbox, there are zero other holders of the state — yet the reap request
/// we receive must still refer to the original state instance and its slot must still remember
/// the session's nonce.
#[tokio::test]
async fn reap_request_pins_per_object_state_after_session_drop() {
    // We need a reaper inbox we own so we can observe the enqueued request; the registry keeps
    // its own inbox internally. Build the state through the registry and then mint a session
    // manually to share this inbox.
    let (reaper, mut inbox) = reaper_channel();
    let (registry, _registry_inbox) = ObjectStateRegistry::new();
    let key = ObjectLocation::new("default", "bucket", "file").unwrap();
    let state = registry.get_or_create(&key);
    let state_ptr = Arc::as_ptr(&state);

    // Install the session identity into the slot the same way the registry would; only then
    // does the reaper's nonce check see "slot remembers us".
    const NONCE: u64 = 7;
    let session = Arc::new(LargeFillSession::new(
        state.clone(),
        ObjectInfo {
            size: 8,
            etag: None,
        },
        2,
        PathBuf::from("/tmp/session-partial"),
        NONCE,
        reaper,
    ));
    state.install_fill_slot_for_test(&session, NONCE);

    // Drop the only external handles to the state so the *sole* remaining strong references
    // are the ones carried by the session and the impending reap request.
    drop(state);
    drop(session);

    let req = timeout(Duration::from_secs(1), inbox.rx.recv())
        .await
        .expect("reaper inbox should receive Drop payload")
        .expect("channel still open");

    assert_eq!(
        Arc::as_ptr(&req.state),
        state_ptr,
        "reap request must pin the original state instance",
    );
    assert_eq!(req.nonce, NONCE);
    // And the slot inside the pinned state still remembers the session identity.
    assert!(req.state.fill_slot_nonce_matches(NONCE));
}

/// Activity guards and object-lock guards carry `Arc<PerObjectState>`, so the registry's
/// `Weak` must stay live while any guard exists and must expire only after every guard drops.
/// This is the object-lock-identity invariant: two concurrent lockers must observe the **same**
/// `AsyncMutex` instance.
#[tokio::test]
async fn object_lock_identity_stable_across_concurrent_lockers() {
    let (registry, _inbox) = ObjectStateRegistry::new();
    let key = ObjectLocation::new("default", "bucket", "file").unwrap();
    let state_a = registry.get_or_create(&key);

    // Concurrent locker observes the same state because the Weak is still held.
    let state_b = registry.get_or_create(&key);
    assert!(
        Arc::ptr_eq(&state_a, &state_b),
        "concurrent get_or_create must hand out the same state",
    );

    // Drop both Arcs; a fresh get_or_create is allowed to mint a new state.
    drop(state_a);
    drop(state_b);
    let state_c = registry.get_or_create(&key);
    // But as long as someone holds any guard, get_or_create must keep returning that same
    // instance — exercise the activity guard lifetime path.
    let lease = state_c.activity_guard(CacheActivityKind::OpenLease);
    let state_d = registry.get_or_create(&key);
    assert!(
        Arc::ptr_eq(&state_c, &state_d),
        "state lifetime must be extended by live activity guards",
    );
    drop(lease);
    drop(state_c);
    drop(state_d);

    // Everything dropped — a new get_or_create is free to mint fresh state.
    let _fresh = registry.get_or_create(&key);
}

/// The stale-entry sweep kicks in when the registry map crosses `CLEANUP_THRESHOLD`. We do not
/// depend on the exact threshold, only on the invariant "after a large number of one-shot
/// lookups with immediate drops, the registry eventually stops growing". This guards against a
/// latent regression where the sweep path would be removed.
#[tokio::test]
async fn registry_sweeps_stale_weak_entries_eventually() {
    let (registry, _inbox) = ObjectStateRegistry::new();
    for i in 0..10_000 {
        let key = ObjectLocation::new("default", "bucket", format!("k-{i}")).unwrap();
        let _state = registry.get_or_create(&key);
        // drop immediately — the Weak in the map becomes stale.
    }
    let after = registry.entry_count();
    assert!(
        after <= 8192,
        "registry should not grow unboundedly after many one-shot lookups (map size: {after})",
    );
}
