//! Utility consumers that can take ownership of one PostgreSQL statement.
//!
//! Consumers are registered by an application DSO and published to the
//! runtime together with ordinary utility hooks. The runtime owns routing;
//! this module owns the typed COPY callback and the single report boundary
//! around the application implementation.

use std::cell::RefCell;
use std::ffi::c_void;

use pgrx::pg_sys;

use crate::copy::{CopyCompletion, CopyContext, CopyError};
use crate::diag::ReportableError;
use crate::runtime_api::UtilityConsumerDescriptor;
use crate::runtime_api::{
    RoutedUtilityConsumer, RoutedUtilityPredicate, UTILITY_ROUTE_CONSUMED,
    UTILITY_ROUTE_PASS_THROUGH,
};

use super::error::{HookError, UtilityHookPhase};

/// Result of the cold-path decision whether a COPY consumer owns a statement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CopyRoute {
    PassThrough,
    Consumed,
}

/// Application-owned COPY utility implementation.
// Copy consumers are registered and invoked in the backend-local utility
// process. They are not moved across threads, so imposing `Send + Sync` here
// would unnecessarily restrict provider state without adding a safety
// guarantee.
pub trait CopyConsumer: 'static {
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// Decide whether this consumer owns the COPY statement.
    ///
    /// This method must only inspect the parse node and other cold-path
    /// command metadata. It must not perform object I/O or mutate command
    /// state; the runtime uses the result to detect competing consumers before
    /// invoking any consuming implementation.
    fn route(&self, context: &CopyContext<'_>) -> Result<CopyRoute, CopyError>;

    /// Execute a statement selected by [`Self::route`].
    fn consume(
        &self,
        context: &mut CopyContext<'_>,
    ) -> Result<CopyCompletion, CopyError>;
}

type CopyConsumerEntry = Box<dyn CopyConsumer>;

struct ExternalCopyConsumerContext {
    consumer: CopyConsumerEntry,
}

thread_local! {
    static BUILDING_REGISTRY: RefCell<Vec<CopyConsumerEntry>> =
        const { RefCell::new(Vec::new()) };
}

pub(super) struct PreparedCopyConsumers {
    // The vector is allocated with its final capacity before descriptor
    // pointers are created. Moving the vector transfers its buffer without
    // moving the contexts stored in that buffer.
    contexts: Vec<ExternalCopyConsumerContext>,
    descriptors: Vec<UtilityConsumerDescriptor>,
}

impl PreparedCopyConsumers {
    pub(super) fn descriptors(&self) -> &[UtilityConsumerDescriptor] {
        &self.descriptors
    }

    pub(super) fn publish_contexts(self) {
        let Self {
            contexts,
            descriptors: _,
        } = self;
        std::mem::forget(contexts);
    }

    pub(super) fn restore(self) {
        BUILDING_REGISTRY.with_borrow_mut(|entries| {
            entries.extend(self.contexts.into_iter().map(|context| context.consumer));
        });
    }
}

#[derive(Clone, Copy)]
pub(super) struct UtilityConsumerCallbacks {
    on_match: RoutedUtilityPredicate,
    on_consume: RoutedUtilityConsumer,
}

impl UtilityConsumerCallbacks {
    pub(super) const BACKEND: Self = Self {
        on_match: copy_consumer_matches,
        on_consume: invoke_copy_consumer,
    };
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn copy_consumer_matches(
    context: *mut c_void,
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const std::ffi::c_char,
    read_only_tree: bool,
    process_context: pg_sys::ProcessUtilityContext::Type,
    params: *mut pg_sys::ParamListInfoData,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    completion_tag: *mut pg_sys::QueryCompletion,
) -> u8 {
    // SAFETY: the runtime accepted the descriptor and retains this context
    // until backend exit. Its callback ABI is tied to the current core build.
    let context = unsafe { &*context.cast::<ExternalCopyConsumerContext>() };
    // SAFETY: the runtime routes this callback only for T_CopyStmt and passes
    // the live ProcessUtility arguments for the current backend command.
    let copy = unsafe {
        CopyContext::from_raw(
            pstmt,
            query_string,
            read_only_tree,
            process_context,
            params,
            query_env,
            dest,
            completion_tag,
        )
    };
    context
        .consumer
        .route(&copy)
        .map(|route| match route {
            CopyRoute::PassThrough => UTILITY_ROUTE_PASS_THROUGH,
            CopyRoute::Consumed => UTILITY_ROUTE_CONSUMED,
        })
        .map_err(|error| {
            HookError::from_copy_error(error).with_utility_context(
                context.consumer.name(),
                UtilityHookPhase::Consume,
                pg_sys::NodeTag::T_CopyStmt,
            )
        })
        .report_unwrap()
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn invoke_copy_consumer(
    context: *mut c_void,
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const std::ffi::c_char,
    read_only_tree: bool,
    process_context: pg_sys::ProcessUtilityContext::Type,
    params: *mut pg_sys::ParamListInfoData,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    completion_tag: *mut pg_sys::QueryCompletion,
) -> u8 {
    // SAFETY: the runtime accepted the descriptor and retains this context
    // until backend exit. Its callback ABI is tied to the current core build.
    let context = unsafe { &*context.cast::<ExternalCopyConsumerContext>() };
    // SAFETY: the runtime routes this callback only for T_CopyStmt and passes
    // the live ProcessUtility arguments for the current backend command.
    let mut copy = unsafe {
        CopyContext::from_raw(
            pstmt,
            query_string,
            read_only_tree,
            process_context,
            params,
            query_env,
            dest,
            completion_tag,
        )
    };
    context
        .consumer
        .consume(&mut copy)
        .map(|completion| {
            copy.complete(completion);
            UTILITY_ROUTE_CONSUMED
        })
        .map_err(|error| {
            HookError::from_copy_error(error).with_utility_context(
                context.consumer.name(),
                UtilityHookPhase::Consume,
                pg_sys::NodeTag::T_CopyStmt,
            )
        })
        .report_unwrap()
}

pub fn register_copy_consumer(consumer: Box<dyn CopyConsumer>) {
    if super::hooks_frozen() {
        panic!("register_copy_consumer called after freeze_hooks");
    }
    let name = consumer.name();
    BUILDING_REGISTRY.with_borrow_mut(|entries| {
        if entries.iter().any(|existing| existing.name() == name) {
            return;
        }
        entries.push(consumer);
    });
}

pub(super) fn prepare_copy_consumers() -> PreparedCopyConsumers {
    let callbacks = UtilityConsumerCallbacks::BACKEND;
    let entries = BUILDING_REGISTRY.with_borrow_mut(std::mem::take);
    let mut contexts = Vec::with_capacity(entries.len());
    let mut descriptors = Vec::with_capacity(entries.len());
    for consumer in entries {
        contexts.push(ExternalCopyConsumerContext { consumer });
        let context = contexts
            .last_mut()
            .expect("a just-pushed COPY consumer context is present");
        descriptors.push(UtilityConsumerDescriptor {
            struct_size: u32::try_from(
                std::mem::size_of::<UtilityConsumerDescriptor>(),
            )
            .expect("utility consumer descriptor size exceeds u32"),
            tag: pg_sys::NodeTag::T_CopyStmt as u32,
            context: std::ptr::from_mut(context).cast(),
            on_match: Some(callbacks.on_match),
            on_consume: Some(callbacks.on_consume),
        });
    }
    PreparedCopyConsumers {
        contexts,
        descriptors,
    }
}
