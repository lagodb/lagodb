//! Runtime-owned registry of provider table-scan capabilities.

use std::ffi::{CStr, c_char, c_void};
use std::mem::size_of;

use lagodb_core::diag::PgReportError;
use lagodb_core::expr::RuntimeValueExpr;
use lagodb_core::expr::pushdown::PredicateFragment;
use lagodb_core::query_contract::{ProviderId, ScanEstimate, ScanId};
use lagodb_core::runtime_api::{
    CallbackErrorReport, GetPreparedTableScanSchema, OpenTableScanStream,
    PlanTableScan, PlannedTableScanResult, PrepareTableScan,
    ReleasePreparedTableScan, TABLE_SCAN_FAILED, TABLE_SCAN_NOT_OWNED,
    TABLE_SCAN_PLANNED, TABLE_SCAN_UNSUPPORTED, TableScanDescriptor,
    TableScanPlanningRequest,
};
use lagodb_query::datafusion::SerialTableScanCallbacks;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::descriptor_directory::{
    DescriptorDirectory, DescriptorNode, DescriptorSnapshot,
};
use crate::provider_bootstrap;

thread_local! {
    static TABLE_SCANS: DescriptorDirectory<StoredTableScan> =
        const { DescriptorDirectory::new() };
}

#[derive(Clone, Copy)]
pub(crate) struct StoredTableScan {
    pub(crate) provider_id: ProviderId,
    pub(crate) provider_name: *const c_char,
    pub(crate) context: *mut c_void,
    pub(crate) plan_scan: PlanTableScan,
    pub(crate) prepare_scan: PrepareTableScan,
    pub(crate) get_prepared_schema: GetPreparedTableScanSchema,
    pub(crate) open_serial_stream: OpenTableScanStream,
    pub(crate) release_prepared: ReleasePreparedTableScan,
}

#[derive(Clone, Copy)]
pub(crate) struct ValidatedTableScan {
    context: *mut c_void,
    plan_scan: PlanTableScan,
    prepare_scan: PrepareTableScan,
    get_prepared_schema: GetPreparedTableScanSchema,
    open_serial_stream: OpenTableScanStream,
    release_prepared: ReleasePreparedTableScan,
}

impl ValidatedTableScan {
    fn from_descriptor(descriptor: &TableScanDescriptor) -> Option<Self> {
        if descriptor.struct_size() != size_of::<TableScanDescriptor>() as u32 {
            return None;
        }
        Some(Self {
            context: descriptor.context(),
            plan_scan: descriptor.plan_scan()?,
            prepare_scan: descriptor.prepare_scan()?,
            get_prepared_schema: descriptor.get_prepared_schema()?,
            open_serial_stream: descriptor.open_serial_stream()?,
            release_prepared: descriptor.release_prepared()?,
        })
    }
}

pub(crate) struct PendingTableScanRegistration {
    entry: Option<Box<DescriptorNode<StoredTableScan>>>,
}

impl PendingTableScanRegistration {
    pub(crate) fn validate(
        descriptor: Option<&TableScanDescriptor>,
    ) -> Option<Option<ValidatedTableScan>> {
        match descriptor {
            Some(descriptor) => {
                Some(Some(ValidatedTableScan::from_descriptor(descriptor)?))
            }
            None => Some(None),
        }
    }

    pub(crate) fn prepare(
        provider_id: ProviderId,
        provider_name: *const c_char,
        descriptor: Option<ValidatedTableScan>,
    ) -> Self {
        let entry = descriptor.map(|descriptor| {
            DescriptorNode::new(StoredTableScan {
                provider_id,
                provider_name,
                context: descriptor.context,
                plan_scan: descriptor.plan_scan,
                prepare_scan: descriptor.prepare_scan,
                get_prepared_schema: descriptor.get_prepared_schema,
                open_serial_stream: descriptor.open_serial_stream,
                release_prepared: descriptor.release_prepared,
            })
        });
        Self { entry }
    }

    pub(crate) fn commit(self) {
        TableScanRegistry::commit(self.entry);
    }
}

pub(crate) struct PlannedScanRecord {
    pub(crate) provider_id: ProviderId,
    pub(crate) provider_name: &'static CStr,
    pub(crate) plan_data: *mut pg_sys::List,
    pub(crate) pruning: Option<PlannedScanPruning>,
    pub(crate) estimate: ScanEstimate,
}

pub(crate) struct PlannedScanPruning {
    pub(crate) bindings: Box<[RuntimeValueExpr]>,
    pub(crate) expression: *mut pg_sys::Expr,
}

enum OwnedResolution {
    Unsupported { provider_name: &'static CStr },
    Planned(PlannedScanRecord),
}

impl OwnedResolution {
    fn provider_name(&self) -> &'static CStr {
        match self {
            Self::Unsupported { provider_name, .. } => provider_name,
            Self::Planned(scan) => scan.provider_name,
        }
    }
}

struct ScanResolver {
    scan: ScanId,
    request: TableScanPlanningRequest,
    owned: Option<OwnedResolution>,
}

impl ScanResolver {
    fn new(scan: ScanId, request: TableScanPlanningRequest) -> Self {
        Self {
            scan,
            request,
            owned: None,
        }
    }

    fn visit(&mut self, descriptor: StoredTableScan) -> Result<(), PgReportError> {
        let mut output = PlannedTableScanResult::default();
        let mut error = CallbackErrorReport::default();
        let status = unsafe {
            (descriptor.plan_scan)(
                descriptor.context,
                &self.request,
                &mut output,
                &mut error,
            )
        };
        let provider_name = unsafe { CStr::from_ptr(descriptor.provider_name) };
        let resolution = match status {
            TABLE_SCAN_NOT_OWNED => return Ok(()),
            TABLE_SCAN_UNSUPPORTED => OwnedResolution::Unsupported { provider_name },
            TABLE_SCAN_PLANNED => {
                if output.struct_size != size_of::<PlannedTableScanResult>() as u32
                    || output.plan_data.is_null()
                    || unsafe { (*output.plan_data).type_ } != pg_sys::NodeTag::T_List
                    || (output.pruning_fragment.is_null()
                        != output.pruning_expression.is_null())
                    || (self.request.predicate_expression.is_null()
                        && !output.pruning_fragment.is_null())
                    || (output.pruning_fragment.is_null()
                        && !output.pruning_binding_exprs.is_null())
                    || (!output.pruning_fragment.is_null()
                        && unsafe { (*output.pruning_fragment).type_ }
                            != pg_sys::NodeTag::T_List)
                    || (!output.pruning_binding_exprs.is_null()
                        && unsafe { (*output.pruning_binding_exprs).type_ }
                            != pg_sys::NodeTag::T_List)
                {
                    return Err(PgReportError::from_message(
                        PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                        "table scan returned invalid plan data",
                    ));
                }
                let estimate = ScanEstimate::try_new(
                    output.estimated_rows,
                    output.estimated_scan_bytes,
                )
                .map_err(|error| {
                    PgReportError::from_message(
                        PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                        format!("table scan returned invalid statistics: {error}"),
                    )
                })?;
                let pruning = if output.pruning_fragment.is_null() {
                    None
                } else {
                    let fragment = unsafe {
                        PredicateFragment::decode_plan_data(output.pruning_fragment)
                    }
                    .map_err(|error| {
                        PgReportError::from_message(
                            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                            format!(
                                "table scan returned invalid pruning plan: {error}"
                            ),
                        )
                    })?;
                    let (_, layout) = fragment.into_parts();
                    let binding_count =
                        unsafe { pg_sys::list_length(output.pruning_binding_exprs) }
                            as usize;
                    if binding_count != layout.len() {
                        return Err(PgReportError::from_message(
                            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                            "table scan pruning expressions do not match their layout",
                        ));
                    }
                    let bindings = layout
                        .values()
                        .iter()
                        .enumerate()
                        .map(|(index, &metadata)| {
                            let expression: *mut pg_sys::Expr = unsafe {
                                pg_sys::list_nth(
                                    output.pruning_binding_exprs,
                                    index as i32,
                                )
                            }
                            .cast();
                            if expression.is_null() {
                                return Err(PgReportError::from_message(
                                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                                    "table scan returned a null pruning expression",
                                ));
                            }
                            Ok(RuntimeValueExpr::new(expression, metadata))
                        })
                        .collect::<Result<Vec<_>, _>>()?
                        .into_boxed_slice();
                    Some(PlannedScanPruning {
                        bindings,
                        expression: output.pruning_expression,
                    })
                };
                OwnedResolution::Planned(PlannedScanRecord {
                    provider_id: descriptor.provider_id,
                    provider_name,
                    plan_data: output.plan_data,
                    pruning,
                    estimate,
                })
            }
            TABLE_SCAN_FAILED => {
                return Err(unsafe { error.to_error("table scan planning") });
            }
            status => {
                return Err(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!("table scan planning returned unknown status {status}"),
                ));
            }
        };

        if let Some(existing) = &self.owned {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!(
                    "table scan {} is owned by both {:?} and {:?}",
                    self.scan.index(),
                    existing.provider_name(),
                    provider_name,
                ),
            ));
        }
        self.owned = Some(resolution);
        Ok(())
    }

    fn finish(self) -> Option<PlannedScanRecord> {
        match self.owned {
            None | Some(OwnedResolution::Unsupported { .. }) => None,
            Some(OwnedResolution::Planned(scan)) => Some(scan),
        }
    }
}

/// Backend-local owner of table-scan registration and resolution operations.
pub(crate) struct TableScanRegistry;

impl TableScanRegistry {
    fn commit(entry: Option<Box<DescriptorNode<StoredTableScan>>>) {
        let _ = TABLE_SCANS.with(|registry| registry.commit(entry));
    }

    fn snapshot() -> DescriptorSnapshot<StoredTableScan> {
        TABLE_SCANS.with(|registry| registry.snapshot())
    }

    /// Resolve the immutable callbacks for a provider referenced by selected
    /// plan data. Absence or duplication is a selected-path invariant
    /// violation, not a capability decline.
    pub(crate) fn callbacks(
        provider: ProviderId,
    ) -> Result<SerialTableScanCallbacks, PgReportError> {
        let mut found = None;
        Self::snapshot().try_for_each(|descriptor| {
            if descriptor.provider_id != provider {
                return Ok(());
            }
            let provider_name = unsafe { CStr::from_ptr(descriptor.provider_name) };
            if found.is_some() {
                return Err(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!(
                        "provider {provider_name:?} ({}) has multiple registered table scan descriptors",
                        provider.index(),
                    ),
                ));
            }
            found = Some(unsafe {
                SerialTableScanCallbacks::from_validated_callbacks(
                    descriptor.context,
                    descriptor.prepare_scan,
                    descriptor.get_prepared_schema,
                    descriptor.open_serial_stream,
                    descriptor.release_prepared,
                )
            });
            Ok(())
        })?;
        found.ok_or_else(|| {
            let message = match provider_bootstrap::provider_name(provider) {
                Some(provider_name) => format!(
                    "selected query plan references provider {:?} ({}) without a table scan descriptor",
                    provider_name.as_c_str(),
                    provider.index(),
                ),
                None => format!(
                    "selected query plan references unknown provider id {}",
                    provider.index(),
                ),
            };
            PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                message,
            )
        })
    }

    pub(crate) fn plan(
        scan: ScanId,
        request: TableScanPlanningRequest,
    ) -> Result<Option<PlannedScanRecord>, PgReportError> {
        let mut resolver = ScanResolver::new(scan, request);
        Self::snapshot().try_for_each(|descriptor| resolver.visit(descriptor))?;
        Ok(resolver.finish())
    }
}
