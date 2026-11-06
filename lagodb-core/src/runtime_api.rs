//! Exact-build PostgreSQL rendezvous ABI for runtime-owned backend services.
//!
//! # Trust model
//!
//! This is an internal ABI between `lagodb-base` and provider DSOs built
//! from the same `lagodb-core` source tree. It is not a general C
//! plugin interface and does not attempt to make arbitrary addresses or
//! malformed foreign allocations safe to inspect.
//!
//! Raw ABI callers must provide complete, initialized, correctly aligned
//! values of the declared `#[repr(C)]` types. Pointer/count pairs must describe
//! live arrays of the exact element type, callback and C-string pointers must
//! be valid, and registered callbacks and contexts must remain live for the
//! backend lifetime. Every runtime API type is an exact-build contract: the
//! runtime and every consuming extension must be built from the same source
//! tree and installed together. The build and install flow guarantees this;
//! it is the safety precondition for every cross-DSO read, and the runtime
//! does not verify it.
//!
//! Descriptors carry no per-type layout fields. The only compatibility check
//! is the [`SdkVersion`] a provider compares once when it connects. It is a
//! diagnostic for packages from different `lagodb-core` releases, so every
//! release must bump the workspace version. Like PostgreSQL's magic block,
//! which compares only the major version, it does not detect DSOs built from
//! different revisions under one version; installing such a mix violates the
//! contract above.

mod abi_slice;
mod client;
mod error;
mod maintenance;
mod object_access;
mod planning;
mod provider;
mod registration;
mod storage_volume;
mod table_copy;
mod table_scan;
mod table_scan_predicate;
mod table_scan_worker;
mod version;

pub use abi_slice::AbiSlice;
pub use client::{
    RUNTIME_API_RENDEZVOUS, RuntimeApi, RuntimeApiError, RuntimeClient,
    STAGE_WORKER_WAKEUP_INVALID_REQUEST, STAGE_WORKER_WAKEUP_LOCATOR_NOT_FOUND,
    STAGE_WORKER_WAKEUP_OK, STAGE_WORKER_WAKEUP_RUNTIME_NOT_PRELOADED,
    StageWorkerWakeupCallback, WorkerWakeupError, rendezvous_slot,
};
pub use error::{CALLBACK_FAILED, CALLBACK_OK, CallbackErrorReport, CallbackFailed};
pub use maintenance::{
    ExecuteMaintenanceCallback, FORMAT_NAME_CAPACITY, InspectMaintenanceCallback,
    MaintenanceMode, MaintenanceReport, MaintenanceRequest, MaintenanceStats,
    RuntimeMaintenanceConfig,
};
pub use object_access::{
    OBJECT_ACCESS_DROP, OBJECT_ACCESS_EVENTS_KNOWN, OBJECT_ACCESS_FUNCTION_EXECUTE,
    OBJECT_ACCESS_NAMESPACE_SEARCH, OBJECT_ACCESS_POST_ALTER,
    OBJECT_ACCESS_POST_CREATE, OBJECT_ACCESS_TRUNCATE, ObjectAccessFilter,
    object_access_event_mask,
};
pub use planning::{
    ModifyPlannerDescriptor, RelationScanPlannerDescriptor, RoutedModifyPlannerPost,
    RoutedModifyQueryPreparation, RoutedModifyUpperPlanner,
    RoutedRelationScanPlanner,
};
pub use provider::{AccessMethodOidCallback, TableProvider};
pub use registration::{
    ObjectAccessHookDescriptor, ObjectAccessStrHookDescriptor, ProviderIdentity,
    ProviderKind, ProviderRegistration, REGISTER_DUPLICATE_ACCESS_METHOD,
    REGISTER_DUPLICATE_NAME, REGISTER_DUPLICATE_TABLE_SCAN_ROUTE,
    REGISTER_INVALID_DESCRIPTOR, REGISTER_OK, REGISTER_OUTSIDE_PROVIDER_BOOTSTRAP,
    REGISTER_PROVIDER_LIBRARY_MISMATCH, RoutedObjectAccessHook,
    RoutedObjectAccessStrHook, RoutedUtilityConsumer, RoutedUtilityPostHook,
    RoutedUtilityPreHook, RoutedUtilityPredicate, RuntimeRegistrationError,
    UTILITY_ROUTE_CONSUMED, UTILITY_ROUTE_FALLBACK, UTILITY_ROUTE_PASS_THROUGH,
    UtilityConsumerDescriptor, UtilityHookDescriptor,
};
pub use storage_volume::{
    LoadTablespaceBindingsCallback, TablespaceBindingEntry, TablespaceBindingsOutput,
};
pub use table_copy::TableCopyCallbacks;
pub use table_scan::{
    BindTableScan, BoundTableScanResult, GetBoundTableScanSchema,
    NegotiateTableScanPredicate, OpenTableScanStream, OwnsForeignServer,
    PlanTableScan, PlanTableScanTasks, PlannedTableScanResult, PlannedTableScanTasks,
    ReleaseBoundTableScan, ReleasePlannedTableScan, ReleaseTableScanPredicate,
    TABLE_SCAN_FAILED, TABLE_SCAN_PLANNED, TABLE_SCAN_UNSUPPORTED,
    TableScanBindRequest, TableScanCallbacks, TableScanDescriptor,
    TableScanPlanningRequest, TableScanPredicateResult, TableScanRoutes,
    TableScanStreamRequest, TableScanTaskMetrics, TableScanTaskPlanningRequest,
};
pub use table_scan_predicate::{
    PredicateSupport, RuntimeComparisonOperator, RuntimeIntegerWidening,
    RuntimePredicateCodecError, RuntimePredicateScalar, RuntimePredicateUpdateAction,
    RuntimePredicateValue, RuntimePruningPredicate, TableScanRuntimePredicate,
};
pub use table_scan_worker::{
    DecodeTableScanWorkerSource, OpenTableScanWorkerStream,
    PrepareTableScanWorkerSource, PreparedWorkerSourceResult,
    ReleasePreparedTableScanWorkerSource, ReleaseTableScanWorkerSource, SourceWorkId,
    TableScanWorkerDescriptor, TableScanWorkerSourceRequest,
    TableScanWorkerStreamRequest, WORKER_SOURCE_FAILED, WORKER_SOURCE_READY,
    WORKER_SOURCE_UNSUPPORTED, WorkerTableScanSourceResult,
};
pub use version::SdkVersion;
