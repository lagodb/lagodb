//! Pluggable request [`RequestObserver`] and [`RequestPolicy`] hooks plus correlation metadata ([`RequestContext`]).

use std::sync::Arc;
use std::time::Duration;

use crate::error::{StorageErrorKind, StorageResult};
use crate::protocol::WireRequestPayload;
use tracing::{debug, warn};

/// Client address type shared across request contexts to avoid per-request String clones.
type ClientAddr = Arc<str>;

#[derive(Clone)]
pub struct RequestHooks {
    observer: Arc<dyn RequestObserver>,
    policy: Arc<dyn RequestPolicy>,
}

impl Default for RequestHooks {
    fn default() -> Self {
        Self {
            observer: Arc::new(NoopRequestObserver),
            policy: Arc::new(NoopRequestPolicy),
        }
    }
}

impl RequestHooks {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_observer<O>(mut self, observer: O) -> Self
    where
        O: RequestObserver,
    {
        self.observer = Arc::new(observer);
        self
    }

    pub fn with_shared_observer(
        mut self,
        observer: Arc<dyn RequestObserver>,
    ) -> Self {
        self.observer = observer;
        self
    }

    pub fn with_policy<P>(mut self, policy: P) -> Self
    where
        P: RequestPolicy,
    {
        self.policy = Arc::new(policy);
        self
    }

    pub fn with_shared_policy(mut self, policy: Arc<dyn RequestPolicy>) -> Self {
        self.policy = policy;
        self
    }

    pub(crate) fn observer(&self) -> &dyn RequestObserver {
        self.observer.as_ref()
    }

    pub(crate) fn policy(&self) -> &dyn RequestPolicy {
        self.policy.as_ref()
    }
}

/// Correlation metadata for a single in-flight request, passed to [`RequestObserver`] and [`RequestPolicy`].
///
/// The operation payload is a clone of the decoded [`WireRequestPayload`]; for READ requests
/// (the hot path) this is cheap since the variant carries only `Copy` fields.
/// Use [`Self::operation_name()`] for a human-readable label and [`Self::payload()`] for
/// pattern-matching on specific variants.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestContext {
    request_id: u64,
    client_addr: ClientAddr,
    payload: WireRequestPayload,
}

impl RequestContext {
    pub(crate) fn new(
        request_id: u64,
        client_addr: ClientAddr,
        payload: WireRequestPayload,
    ) -> Self {
        Self {
            request_id,
            client_addr,
            payload,
        }
    }

    pub fn request_id(&self) -> u64 {
        self.request_id
    }

    pub fn client_addr(&self) -> &str {
        &self.client_addr
    }

    /// Returns the wire payload for pattern-matching on specific operation variants.
    pub fn payload(&self) -> &WireRequestPayload {
        &self.payload
    }

    /// Human-readable operation name (e.g. `"open"`, `"read"`, `"close"`).
    pub fn operation_name(&self) -> &'static str {
        self.payload.operation_name()
    }

    // ---- Backward-compatible convenience: `operation()` returns a reference that supports `.name()` ----

    /// Returns a reference to the payload, which implements [`OperationMeta`].
    ///
    /// Prefer [`Self::payload()`] for pattern-matching or [`Self::operation_name()`] for the label.
    pub fn operation(&self) -> &WireRequestPayload {
        &self.payload
    }
}

/// Trait providing operation metadata from any request-like type.
///
/// Implemented on [`WireRequestPayload`] so that observers and policies can query the operation
/// name without needing a separate enum.
pub trait OperationMeta {
    /// Short human-readable name for the operation (e.g. `"open"`, `"read"`).
    fn operation_name(&self) -> &'static str;
}

impl OperationMeta for WireRequestPayload {
    fn operation_name(&self) -> &'static str {
        match self {
            Self::Open { .. } => "open",
            Self::Head { .. } => "head",
            Self::Read { .. } => "read",
            Self::Close { .. } => "close",
            Self::StageCreate { .. } => "stage_create",
            Self::Commit { .. } => "commit",
            Self::Abort { .. } => "abort",
            Self::RegisterStore { .. } => "register_store",
            Self::UnregisterStore { .. } => "unregister_store",
            Self::PurgeStoreCache { .. } => "purge_store_cache",
            Self::InvalidateObjectCache { .. } => "invalidate_object_cache",
            Self::Delete { .. } => "delete",
            Self::DeletePrefix { .. } => "delete_prefix",
            Self::List { .. } => "list",
        }
    }
}

/// Preserved as a type alias for backward compatibility with external consumers that imported
/// [`RequestOperation`]. New code should match on [`WireRequestPayload`] directly.
pub type RequestOperation = WireRequestPayload;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestOutcome {
    status: RequestStatus,
    elapsed: Duration,
}

impl RequestOutcome {
    pub(crate) fn success(elapsed: Duration) -> Self {
        Self {
            status: RequestStatus::Success,
            elapsed,
        }
    }

    pub(crate) fn error(kind: StorageErrorKind, elapsed: Duration) -> Self {
        Self {
            status: RequestStatus::Error { kind },
            elapsed,
        }
    }

    pub fn status(&self) -> RequestStatus {
        self.status
    }

    pub fn elapsed(&self) -> Duration {
        self.elapsed
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestStatus {
    Success,
    Error { kind: StorageErrorKind },
}

pub trait RequestObserver: Send + Sync + 'static {
    fn on_request_start(&self, _context: &RequestContext) {}

    fn on_request_finish(
        &self,
        _context: &RequestContext,
        _outcome: &RequestOutcome,
    ) {
    }
}

pub trait RequestPolicy: Send + Sync + 'static {
    fn before_dispatch(&self, _context: &RequestContext) -> StorageResult<()> {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopRequestObserver;

impl RequestObserver for NoopRequestObserver {}

#[derive(Clone, Copy, Debug, Default)]
pub struct TracingRequestObserver;

impl RequestObserver for TracingRequestObserver {
    fn on_request_start(&self, context: &RequestContext) {
        debug!(
            request_id = context.request_id(),
            client_addr = context.client_addr(),
            operation = context.operation_name(),
            request = ?context.payload(),
            "storage request started",
        );
    }

    fn on_request_finish(&self, context: &RequestContext, outcome: &RequestOutcome) {
        match outcome.status() {
            RequestStatus::Success => {
                debug!(
                    request_id = context.request_id(),
                    client_addr = context.client_addr(),
                    operation = context.operation_name(),
                    elapsed_us = elapsed_micros(outcome.elapsed()),
                    "storage request finished",
                );
            }
            RequestStatus::Error { kind } => {
                warn!(
                    request_id = context.request_id(),
                    client_addr = context.client_addr(),
                    operation = context.operation_name(),
                    error_kind = ?kind,
                    elapsed_us = elapsed_micros(outcome.elapsed()),
                    "storage request failed",
                );
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopRequestPolicy;

impl RequestPolicy for NoopRequestPolicy {}

fn elapsed_micros(elapsed: Duration) -> u64 {
    elapsed.as_micros().try_into().unwrap_or(u64::MAX)
}
