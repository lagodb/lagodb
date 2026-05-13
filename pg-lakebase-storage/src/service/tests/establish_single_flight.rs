//! Tests for the per-key residency-establishment single-flight.
//!
//! These tests exercise the contract documented in [`crate::cache::establish`]: at most one
//! backend HEAD is issued per cache lifecycle regardless of concurrent OPENs, followers observe
//! the leader's outcome (success → hit, failure → equivalent error), and the lookup retry loop
//! converges in at most two passes.
//!
//! # Note on scheduling assumptions
//!
//! The concurrency tests use a `yield_now` loop to let follower tasks reach
//! `lookup_for_open` before the blocked leader is released. This is a best-effort nudge,
//! not a deterministic barrier — the correctness assertions (`head_calls() == 1` and
//! residency hints) are scheduling-independent and hold regardless of when followers
//! arrive. In the extremely unlikely event that every follower arrives **after** the
//! leader publishes `Succeeded`, the followers take the `Hit` path directly. Coverage
//! of the `Waiting` path is therefore probabilistic in principle but reliable in
//! practice: the leader blocks inside `BlockingHeadBackend::head` until the test
//! explicitly releases it, so followers always win the race to `lookup_for_open` under
//! any realistic scheduler.

use std::sync::Arc;

use tokio::task::JoinSet;

use crate::backend::{MemoryObjectBackend, StoreRegistry};
use crate::error::{StorageError, StorageErrorKind};
use crate::service::StorageService;
use crate::session::handle_table::HandleTable;

use super::fixtures::{
    close, memory_cache, memory_cache_with_limits, open_file, residency_hint, BlockingHeadBackend, BUCKET,
    DEFAULT_STORE, LARGE_KEY, SMALL_KEY,
};

/// Concurrent OPENs on a missing small object converge through a single leader HEAD.
///
/// The blocking backend pins the leader's HEAD mid-call; N-1 followers race in behind it and
/// join the establishment flight. Once the leader is released, every follower retries
/// `lookup_for_open` and observes a hit without ever calling `head` itself.
#[tokio::test]
async fn concurrent_small_opens_share_a_single_head() {
    let key = super::fixtures::default_location(SMALL_KEY);
    let memory = MemoryObjectBackend::new();
    memory.insert(key.clone(), b"abc".to_vec());
    let backend = Arc::new(BlockingHeadBackend::new(memory));
    let cache = memory_cache();
    let service = Arc::new(StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, backend.clone()).unwrap(), cache.clone()));
    let handles = Arc::new(HandleTable::new());

    // Kick off the leader, then stall it inside the blocked HEAD.
    let leader_service = service.clone();
    let leader_handles = handles.clone();
    let leader = tokio::spawn(async move { open_file(&leader_service, &leader_handles, BUCKET, SMALL_KEY).await });
    backend.wait_until_first_head_starts().await;

    // Spawn followers that must all observe the single in-flight HEAD as a `Waiting` outcome.
    let mut followers = JoinSet::new();
    for _ in 0..4 {
        let service = service.clone();
        let handles = handles.clone();
        followers.spawn(async move { open_file(&service, &handles, BUCKET, SMALL_KEY).await });
    }

    // Give the followers a chance to reach `lookup_for_open` and register as waiters. We
    // cannot observe the waiter count directly, so yield a few times instead — the invariant
    // we actually care about (one HEAD call) is asserted below regardless of scheduling.
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }

    backend.release_first_head();

    let leader_open = leader.await.unwrap();
    let mut follower_handles = Vec::new();
    while let Some(open) = followers.join_next().await {
        follower_handles.push(open.unwrap());
    }

    assert_eq!(backend.head_calls(), 1, "establishment single-flight must collapse all HEADs into one");
    // Every caller must end up bound to a SmallKv residency — the leader's admit is visible to
    // followers by the time `lookup_for_open` retries.
    assert_eq!(residency_hint(&handles, leader_open.handle), Some(crate::cache::ResidencyStateHint::SmallKv),);
    for open in &follower_handles {
        assert_eq!(residency_hint(&handles, open.handle), Some(crate::cache::ResidencyStateHint::SmallKv),);
    }

    close(&service, &handles, leader_open.handle).await;
    for open in follower_handles {
        close(&service, &handles, open.handle).await;
    }
}

/// Same contract as [`concurrent_small_opens_share_a_single_head`] but on the large-fill path.
/// The leader's admit installs a [`crate::cache::LargeFillSession`]; followers observe the
/// session on retry and bind a `LargeFill` residency pointing at the same session.
#[tokio::test]
async fn concurrent_large_opens_share_a_single_head() {
    // small limit 4 so a 10-byte object takes the large path.
    let key = super::fixtures::default_location(LARGE_KEY);
    let memory = MemoryObjectBackend::new();
    memory.insert(key.clone(), b"abcdefghij".to_vec());
    let backend = Arc::new(BlockingHeadBackend::new(memory));
    let cache = memory_cache_with_limits(4, 4);
    let service = Arc::new(StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, backend.clone()).unwrap(), cache.clone()));
    let handles = Arc::new(HandleTable::new());

    let leader_service = service.clone();
    let leader_handles = handles.clone();
    let leader = tokio::spawn(async move { open_file(&leader_service, &leader_handles, BUCKET, LARGE_KEY).await });
    backend.wait_until_first_head_starts().await;

    let mut followers = JoinSet::new();
    for _ in 0..4 {
        let service = service.clone();
        let handles = handles.clone();
        followers.spawn(async move { open_file(&service, &handles, BUCKET, LARGE_KEY).await });
    }
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }

    backend.release_first_head();

    let leader_open = leader.await.unwrap();
    let mut follower_handles = Vec::new();
    while let Some(open) = followers.join_next().await {
        follower_handles.push(open.unwrap());
    }

    assert_eq!(backend.head_calls(), 1, "establishment single-flight must collapse all HEADs into one");
    assert_eq!(residency_hint(&handles, leader_open.handle), Some(crate::cache::ResidencyStateHint::LargeFill),);
    for open in &follower_handles {
        assert_eq!(residency_hint(&handles, open.handle), Some(crate::cache::ResidencyStateHint::LargeFill),);
    }

    close(&service, &handles, leader_open.handle).await;
    for open in follower_handles {
        close(&service, &handles, open.handle).await;
    }
}

/// When the leader's HEAD fails, every follower surfaces an equivalent error. The error's
/// [`StorageErrorKind`] survives the outcome channel verbatim so the client observes
/// `NotFound` (or whatever the backend returned) rather than a synthetic cache error.
#[tokio::test]
async fn follower_receives_equivalent_error_when_leader_head_fails() {
    let memory = MemoryObjectBackend::new();
    // Deliberately do not insert any key: the first HEAD will fail with NotFound after release.
    let backend = Arc::new(BlockingHeadBackend::new(memory));
    backend.fail_first_head_with_not_found();
    let cache = memory_cache();
    let service = Arc::new(StorageService::with_registry(StoreRegistry::new().with_shared_backend(DEFAULT_STORE, backend.clone()).unwrap(), cache.clone()));
    let handles = Arc::new(HandleTable::new());

    let leader_service = service.clone();
    let leader_handles = handles.clone();
    let leader = tokio::spawn(async move { service_open_result(&leader_service, &leader_handles, SMALL_KEY).await });
    backend.wait_until_first_head_starts().await;

    let mut followers = JoinSet::new();
    for _ in 0..3 {
        let service = service.clone();
        let handles = handles.clone();
        followers.spawn(async move { service_open_result(&service, &handles, SMALL_KEY).await });
    }
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }

    backend.release_first_head();

    let leader_err = leader.await.unwrap().err().expect("leader HEAD must fail");
    assert_eq!(leader_err.kind(), StorageErrorKind::NotFound);

    while let Some(follower) = followers.join_next().await {
        let err = follower.unwrap().err().expect("follower must surface leader's failure");
        assert_eq!(err.kind(), StorageErrorKind::NotFound);
    }

    assert_eq!(backend.head_calls(), 1, "followers must not issue their own HEAD after the leader fails",);
}

async fn service_open_result<I: crate::cache::CacheIndex + 'static>(
    service: &StorageService<I>,
    handles: &HandleTable,
    key: &str,
) -> Result<crate::service::tests::fixtures::OpenResult, StorageError> {
    use crate::handle::OpenFlags;
    use crate::service::command::{OpenCommand, StorageCommand};
    use crate::service::reply::CommandOutput;
    use crate::service::tests::fixtures::{OpenResult, BUCKET, DEFAULT_STORE};

    let reply = service
        .execute(
            handles,
            StorageCommand::Open(OpenCommand {
                store_id: DEFAULT_STORE.to_string(),
                bucket: BUCKET.to_string(),
                key: key.to_string(),
                flags: OpenFlags::READ_ONLY,
            }),
        )
        .await?;
    let CommandOutput::Open(output) = reply.output else {
        panic!("unexpected open output");
    };
    Ok(OpenResult {
        handle: output.handle,
        direct_io: output.direct_io,
    })
}
