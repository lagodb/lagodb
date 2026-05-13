use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

/// Tracks runtime orphan candidates, not a complete physical-cache inventory.
///
/// Startup recovery performs the full reconciliation pass and deletes historical orphans. After
/// startup, this structure only records payloads that passed through a non-atomic file/metadata
/// transition and may need a later targeted orphan check.
///
/// Cleanup must not scan all `object_meta` rows to prove ownership. It iterates these candidates
/// and performs keyed metadata checks before deleting.
///
/// Complete and partial cache files share a single candidate set: the file kind is encoded in
/// the path's suffix (see [`crate::cache::CachePathResolver`]), so the orphan-check path can
/// recover it via [`crate::cache::CachePathResolver::parse_cache_path`] without a parallel
/// collection.
///
/// Small-object payloads are **not** tracked here: both the metadata row and the small payload
/// live in the same KV store and are always written/deleted inside a single write transaction,
/// so orphaned small payloads cannot arise.
#[derive(Clone, Default)]
pub(in crate::cache) struct RuntimeOrphanCandidates {
    inner: Arc<Mutex<RuntimeOrphanCandidateState>>,
}

#[derive(Default)]
struct RuntimeOrphanCandidateState {
    file_paths: HashSet<PathBuf>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::cache) struct RuntimeOrphanCandidateSnapshot {
    pub file_paths: HashSet<PathBuf>,
}

impl RuntimeOrphanCandidates {
    pub(crate) fn clear_all(&self) {
        self.lock_inner().file_paths.clear();
    }

    pub(crate) fn snapshot(&self) -> RuntimeOrphanCandidateSnapshot {
        let inner = self.lock_inner();
        RuntimeOrphanCandidateSnapshot {
            file_paths: inner.file_paths.clone(),
        }
    }

    pub(crate) fn record_file_candidate(&self, path: PathBuf) {
        self.lock_inner().file_paths.insert(path);
    }

    pub(crate) fn clear_file_candidate(&self, path: &Path) {
        self.lock_inner().file_paths.remove(path);
    }

    /// Promotion atomically swaps the partial orphan candidate for the complete one so a
    /// subsequent cleanup pass sees exactly one path for this key.
    pub(crate) fn record_promotion(&self, partial: &Path, complete: PathBuf) {
        let mut inner = self.lock_inner();
        inner.file_paths.remove(partial);
        inner.file_paths.insert(complete);
    }

    fn lock_inner(&self) -> MutexGuard<'_, RuntimeOrphanCandidateState> {
        // Candidates are best-effort, but they are recorded around non-atomic
        // file/metadata transitions. If this state is poisoned, prefer a hard
        // failure and startup recovery over continuing with an unknown inventory.
        self.inner
            .lock()
            .expect("runtime orphan candidate mutex poisoned; candidate state is no longer trustworthy")
    }
}
