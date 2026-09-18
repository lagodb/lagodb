//! Query-host registry of provider table-scan capabilities indexed by
//! PostgreSQL storage routes.

use std::ffi::{CStr, c_char, c_void};
use std::mem::size_of;

use lagodb_core::diag::PgReportError;
use lagodb_core::query_contract::{TableScanRoute, TableScanRouteKind};
use lagodb_core::runtime_api::{
    CallbackErrorReport, PlanTableScan, PlannedTableScanResult,
    REGISTER_DUPLICATE_TABLE_SCAN_ROUTE, REGISTER_INVALID_DESCRIPTOR,
    TableScanDescriptor, TableScanPlanningRequest,
};
use lagodb_query::datafusion::SerialTableScanCallbacks;
use pgrx::prelude::PgSqlErrorCode;

use crate::descriptor_registry::{
    DescriptorNode, DescriptorRegistry, DescriptorSnapshot,
};

thread_local! {
    static TABLE_SCANS: DescriptorRegistry<StoredTableScan> =
        const { DescriptorRegistry::new() };
}

#[derive(Clone, Copy)]
struct TableScanCallbacks {
    context: *mut c_void,
    plan_scan: PlanTableScan,
    serial: SerialTableScanCallbacks,
}

impl TableScanCallbacks {
    fn from_descriptor(descriptor: &TableScanDescriptor) -> Option<Self> {
        let expected_size = u32::try_from(size_of::<TableScanDescriptor>()).ok()?;
        if descriptor.struct_size() != expected_size {
            return None;
        }
        // SAFETY: the descriptor has the exact runtime ABI layout. Registration
        // treats its callback and context contracts as trusted unsafe input.
        let serial = unsafe {
            SerialTableScanCallbacks::from_validated_descriptor(descriptor)
        }?;
        Some(Self {
            context: descriptor.context(),
            plan_scan: descriptor.plan_scan()?,
            serial,
        })
    }
}

#[derive(Clone, Copy)]
struct StoredTableScan {
    access_method_name: *const c_char,
    foreign_data_wrapper_name: *const c_char,
    callbacks: TableScanCallbacks,
}

impl StoredTableScan {
    fn from_descriptor(descriptor: &TableScanDescriptor) -> Option<Self> {
        let callbacks = TableScanCallbacks::from_descriptor(descriptor)?;
        let access_method_name = descriptor.access_method_name();
        let foreign_data_wrapper_name = descriptor.foreign_data_wrapper_name();
        if access_method_name.is_null() && foreign_data_wrapper_name.is_null() {
            return None;
        }
        for name in [access_method_name, foreign_data_wrapper_name] {
            // SAFETY: the trusted registration ABI requires every non-null
            // route pointer to reference a backend-lifetime C string.
            if !name.is_null() && unsafe { CStr::from_ptr(name) }.is_empty() {
                return None;
            }
        }
        Some(Self {
            access_method_name,
            foreign_data_wrapper_name,
            callbacks,
        })
    }

    fn conflicts_with_registered_route(self) -> bool {
        // SAFETY: route pointers were validated by `from_descriptor` under the
        // trusted registration ABI and remain live for the backend lifetime.
        (!self.access_method_name.is_null()
            && TableScanRegistry::contains_route(TableScanRoute::access_method(
                unsafe { CStr::from_ptr(self.access_method_name) },
            )))
            || (!self.foreign_data_wrapper_name.is_null()
                && TableScanRegistry::contains_route(
                    TableScanRoute::foreign_data_wrapper(unsafe {
                        CStr::from_ptr(self.foreign_data_wrapper_name)
                    }),
                ))
    }

    fn matches(self, route: TableScanRoute<'_>) -> bool {
        let registered_name = match route.kind() {
            TableScanRouteKind::AccessMethod => self.access_method_name,
            TableScanRouteKind::ForeignDataWrapper => self.foreign_data_wrapper_name,
        };
        // SAFETY: stored names came from a validated backend-lifetime
        // descriptor; the null case is excluded before constructing `CStr`.
        !registered_name.is_null()
            && unsafe { CStr::from_ptr(registered_name) } == route.name()
    }

    fn resolved(self, kind: TableScanRouteKind) -> ResolvedTableScan {
        let route_name = match kind {
            TableScanRouteKind::AccessMethod => self.access_method_name,
            TableScanRouteKind::ForeignDataWrapper => self.foreign_data_wrapper_name,
        };
        ResolvedTableScan {
            route_kind: kind,
            route_name,
            callbacks: self.callbacks,
        }
    }
}

pub(crate) struct PendingTableScanRegistration {
    entry: Option<Box<DescriptorNode<StoredTableScan>>>,
}

impl PendingTableScanRegistration {
    pub(crate) fn prepare(
        descriptor: Option<&TableScanDescriptor>,
    ) -> Result<Self, u32> {
        let descriptor = descriptor
            .map(|descriptor| {
                StoredTableScan::from_descriptor(descriptor)
                    .ok_or(REGISTER_INVALID_DESCRIPTOR)
            })
            .transpose()?;
        if descriptor.is_some_and(StoredTableScan::conflicts_with_registered_route) {
            return Err(REGISTER_DUPLICATE_TABLE_SCAN_ROUTE);
        }
        let entry = descriptor.map(DescriptorNode::new);
        Ok(Self { entry })
    }

    pub(crate) fn commit(self) {
        TableScanRegistry::commit(self.entry);
    }
}

/// One immutable callback bundle selected by a PostgreSQL storage route.
#[derive(Clone, Copy)]
pub(super) struct ResolvedTableScan {
    route_kind: TableScanRouteKind,
    route_name: *const c_char,
    callbacks: TableScanCallbacks,
}

impl ResolvedTableScan {
    pub(super) fn route(self) -> TableScanRoute<'static> {
        // SAFETY: registration accepts only backend-lifetime route strings and
        // the selected kind always chooses a non-null registered route.
        TableScanRoute::new(self.route_kind, unsafe {
            CStr::from_ptr(self.route_name)
        })
    }

    pub(super) fn plan(
        self,
        request: &TableScanPlanningRequest,
        output: &mut PlannedTableScanResult,
        error: &mut CallbackErrorReport,
    ) -> u32 {
        // SAFETY: registration validated the callback table; all arguments
        // remain live for this synchronous FFI call.
        unsafe {
            (self.callbacks.plan_scan)(self.callbacks.context, request, output, error)
        }
    }

    fn serial_callbacks(self) -> SerialTableScanCallbacks {
        self.callbacks.serial
    }
}

/// Backend-local owner of table-scan registration and exact route lookup.
pub(super) struct TableScanRegistry;

impl TableScanRegistry {
    fn commit(entry: Option<Box<DescriptorNode<StoredTableScan>>>) {
        let _ = TABLE_SCANS.with(|registry| registry.commit(entry));
    }

    fn snapshot() -> DescriptorSnapshot<StoredTableScan> {
        TABLE_SCANS.with(|registry| registry.snapshot())
    }

    fn contains_route(route: TableScanRoute<'_>) -> bool {
        Self::resolve(route).is_some()
    }

    pub(super) fn resolve(route: TableScanRoute<'_>) -> Option<ResolvedTableScan> {
        let mut found = None;
        Self::snapshot().for_each_if(
            |descriptor| descriptor.matches(route),
            |descriptor| found = Some(descriptor.resolved(route.kind())),
        );
        found
    }

    pub(super) fn resolve_serial_callbacks(
        route: TableScanRoute<'_>,
    ) -> Result<SerialTableScanCallbacks, PgReportError> {
        Self::resolve(route)
            .map(ResolvedTableScan::serial_callbacks)
            .ok_or_else(|| {
                PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!(
                        "selected query plan references unregistered {:?} table-scan route {:?}",
                        route.kind(),
                        route.name(),
                    ),
                )
            })
    }
}
