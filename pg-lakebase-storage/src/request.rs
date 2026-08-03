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
/// Only operation metadata is retained. In particular, configured attach
/// requests carry credentials and must not be cloned merely for logging or
/// admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestContext {
    request_id: u64,
    client_addr: ClientAddr,
    operation: RequestOperation,
}

impl RequestContext {
    pub(crate) fn new(
        request_id: u64,
        client_addr: ClientAddr,
        payload: &WireRequestPayload,
    ) -> Self {
        Self {
            request_id,
            client_addr,
            operation: RequestOperation::from(payload),
        }
    }

    pub fn request_id(&self) -> u64 {
        self.request_id
    }

    pub fn client_addr(&self) -> &str {
        &self.client_addr
    }

    /// Human-readable operation name (e.g. `"open"`, `"read"`, `"close"`).
    pub fn operation_name(&self) -> &'static str {
        self.operation.operation_name()
    }

    pub fn operation(&self) -> RequestOperation {
        self.operation
    }
}

/// Trait providing operation metadata from any request-like type.
///
/// Implemented on [`WireRequestPayload`] and [`RequestOperation`] so callers
/// can query the operation name without retaining a request payload.
pub trait OperationMeta {
    /// Short human-readable name for the operation (e.g. `"open"`, `"read"`).
    fn operation_name(&self) -> &'static str;
}

impl OperationMeta for WireRequestPayload {
    fn operation_name(&self) -> &'static str {
        match self {
            Self::AttachManaged { .. } => "attach_managed",
            Self::AttachConfigured { .. } => "attach_configured",
            Self::Open { .. } => "open",
            Self::Head { .. } => "head",
            Self::Read { .. } => "read",
            Self::Close { .. } => "close",
            Self::Upload { .. } => "upload",
            Self::ProbeStore { .. } => "probe_store",
            Self::InvalidateObjectCache { .. } => "invalidate_object_cache",
            Self::Delete { .. } => "delete",
            Self::DeletePrefix { .. } => "delete_prefix",
            Self::DeleteObjects { .. } => "delete_objects",
            Self::List { .. } => "list",
            Self::CloseList { .. } => "close_list",
        }
    }
}

/// Operation classification retained by request hooks without retaining the
/// decoded payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestOperation {
    AttachManaged,
    AttachConfigured,
    Open,
    Head,
    Read,
    Close,
    Upload,
    ProbeStore,
    InvalidateObjectCache,
    Delete,
    DeletePrefix,
    DeleteObjects,
    List,
    CloseList,
}

impl RequestOperation {
    pub const fn name(self) -> &'static str {
        match self {
            Self::AttachManaged => "attach_managed",
            Self::AttachConfigured => "attach_configured",
            Self::Open => "open",
            Self::Head => "head",
            Self::Read => "read",
            Self::Close => "close",
            Self::Upload => "upload",
            Self::ProbeStore => "probe_store",
            Self::InvalidateObjectCache => "invalidate_object_cache",
            Self::Delete => "delete",
            Self::DeletePrefix => "delete_prefix",
            Self::DeleteObjects => "delete_objects",
            Self::List => "list",
            Self::CloseList => "close_list",
        }
    }
}

impl OperationMeta for RequestOperation {
    fn operation_name(&self) -> &'static str {
        self.name()
    }
}

impl From<&WireRequestPayload> for RequestOperation {
    fn from(payload: &WireRequestPayload) -> Self {
        match payload {
            WireRequestPayload::AttachManaged { .. } => Self::AttachManaged,
            WireRequestPayload::AttachConfigured { .. } => Self::AttachConfigured,
            WireRequestPayload::Open { .. } => Self::Open,
            WireRequestPayload::Head { .. } => Self::Head,
            WireRequestPayload::Read { .. } => Self::Read,
            WireRequestPayload::Close { .. } => Self::Close,
            WireRequestPayload::Upload { .. } => Self::Upload,
            WireRequestPayload::ProbeStore { .. } => Self::ProbeStore,
            WireRequestPayload::InvalidateObjectCache { .. } => {
                Self::InvalidateObjectCache
            }
            WireRequestPayload::Delete { .. } => Self::Delete,
            WireRequestPayload::DeletePrefix { .. } => Self::DeletePrefix,
            WireRequestPayload::DeleteObjects { .. } => Self::DeleteObjects,
            WireRequestPayload::List { .. } => Self::List,
            WireRequestPayload::CloseList { .. } => Self::CloseList,
        }
    }
}

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
