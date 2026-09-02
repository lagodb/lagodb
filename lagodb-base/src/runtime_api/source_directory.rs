//! Runtime-owned immutable directory of provider query-source descriptors.

use std::ffi::{CStr, c_char, c_void};
use std::mem::size_of;

use lagodb_core::diag::PgReportError;
use lagodb_core::query_contract::{ProviderId, SourceEstimate, SourceId};
use lagodb_core::runtime_api::{
    FfiErrorRecord, OpenQuerySourceStream, PlanQuerySource, PlannedQuerySource,
    PrepareQuerySource, QUERY_SOURCE_FAILED, QUERY_SOURCE_NOT_OWNED,
    QUERY_SOURCE_PLANNED, QUERY_SOURCE_UNSUPPORTED, QuerySourceDescriptor,
    QuerySourcePlanningRequest, ReleasePreparedQuerySource,
};
use lagodb_query::datafusion::SerialSourceCallbacks;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::descriptor_directory::{
    DescriptorDirectory, DescriptorNode, DescriptorSnapshot,
};
use crate::provider_bootstrap;

thread_local! {
    static QUERY_SOURCES: DescriptorDirectory<StoredQuerySource> =
        const { DescriptorDirectory::new() };
}

#[derive(Clone, Copy)]
pub(crate) struct StoredQuerySource {
    pub(crate) provider_id: ProviderId,
    pub(crate) provider_name: *const c_char,
    pub(crate) context: *mut c_void,
    pub(crate) plan_source: PlanQuerySource,
    pub(crate) prepare_source: PrepareQuerySource,
    pub(crate) open_serial_stream: OpenQuerySourceStream,
    pub(crate) release_prepared: ReleasePreparedQuerySource,
}

#[derive(Clone, Copy)]
pub(crate) struct ValidatedQuerySource {
    context: *mut c_void,
    plan_source: PlanQuerySource,
    prepare_source: PrepareQuerySource,
    open_serial_stream: OpenQuerySourceStream,
    release_prepared: ReleasePreparedQuerySource,
}

impl ValidatedQuerySource {
    fn from_descriptor(descriptor: &QuerySourceDescriptor) -> Option<Self> {
        if descriptor.struct_size() != size_of::<QuerySourceDescriptor>() as u32 {
            return None;
        }
        Some(Self {
            context: descriptor.context(),
            plan_source: descriptor.plan_source()?,
            prepare_source: descriptor.prepare_source()?,
            open_serial_stream: descriptor.open_serial_stream()?,
            release_prepared: descriptor.release_prepared()?,
        })
    }
}

pub(crate) struct PreparedQuerySource {
    entry: Option<Box<DescriptorNode<StoredQuerySource>>>,
}

impl PreparedQuerySource {
    pub(crate) fn validate(
        descriptor: Option<&QuerySourceDescriptor>,
    ) -> Option<Option<ValidatedQuerySource>> {
        match descriptor {
            Some(descriptor) => {
                Some(Some(ValidatedQuerySource::from_descriptor(descriptor)?))
            }
            None => Some(None),
        }
    }

    pub(crate) fn prepare(
        provider_id: ProviderId,
        provider_name: *const c_char,
        descriptor: Option<ValidatedQuerySource>,
    ) -> Self {
        let entry = descriptor.map(|descriptor| {
            DescriptorNode::new(StoredQuerySource {
                provider_id,
                provider_name,
                context: descriptor.context,
                plan_source: descriptor.plan_source,
                prepare_source: descriptor.prepare_source,
                open_serial_stream: descriptor.open_serial_stream,
                release_prepared: descriptor.release_prepared,
            })
        });
        Self { entry }
    }

    pub(crate) fn commit(self) {
        let _ = QUERY_SOURCES.with(|directory| directory.commit(self.entry));
    }
}

pub(crate) fn snapshot() -> DescriptorSnapshot<StoredQuerySource> {
    QUERY_SOURCES.with(|directory| directory.snapshot())
}

pub(crate) struct PlannedSourceRecord {
    pub(crate) provider_id: ProviderId,
    pub(crate) provider_name: &'static CStr,
    pub(crate) source: SourceId,
    pub(crate) plan_data: *mut pg_sys::List,
    pub(crate) estimate: SourceEstimate,
}

enum OwnedResolution {
    Unsupported { provider_name: &'static CStr },
    Planned(PlannedSourceRecord),
}

impl OwnedResolution {
    fn provider_name(&self) -> &'static CStr {
        match self {
            Self::Unsupported { provider_name, .. } => provider_name,
            Self::Planned(source) => source.provider_name,
        }
    }
}

struct SourceResolver {
    source: SourceId,
    request: QuerySourcePlanningRequest,
    owned: Option<OwnedResolution>,
}

impl SourceResolver {
    fn new(
        source: SourceId,
        root: *mut pg_sys::PlannerInfo,
        relation: *mut pg_sys::RelOptInfo,
        range_table_index: pg_sys::Index,
        range_table_entry: *mut pg_sys::RangeTblEntry,
    ) -> Self {
        Self {
            source,
            request: QuerySourcePlanningRequest::count_rows(
                source.index(),
                root,
                relation,
                range_table_index,
                range_table_entry,
            ),
            owned: None,
        }
    }

    fn visit(&mut self, descriptor: StoredQuerySource) -> Result<(), PgReportError> {
        let mut output = PlannedQuerySource::default();
        let mut error = FfiErrorRecord::default();
        let status = unsafe {
            (descriptor.plan_source)(
                descriptor.context,
                &self.request,
                &mut output,
                &mut error,
            )
        };
        let provider_name = unsafe { CStr::from_ptr(descriptor.provider_name) };
        let resolution = match status {
            QUERY_SOURCE_NOT_OWNED => return Ok(()),
            QUERY_SOURCE_UNSUPPORTED => {
                OwnedResolution::Unsupported { provider_name }
            }
            QUERY_SOURCE_PLANNED => {
                if output.struct_size != size_of::<PlannedQuerySource>() as u32
                    || output.plan_data.is_null()
                    || unsafe { (*output.plan_data).type_ } != pg_sys::NodeTag::T_List
                {
                    return Err(PgReportError::from_message(
                        PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                        "query source returned invalid planned source data",
                    ));
                }
                let estimate = SourceEstimate::try_new(
                    output.estimated_rows,
                    output.estimated_scan_bytes,
                )
                .map_err(|error| {
                    PgReportError::from_message(
                        PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                        format!("query source returned invalid statistics: {error}"),
                    )
                })?;
                OwnedResolution::Planned(PlannedSourceRecord {
                    provider_id: descriptor.provider_id,
                    provider_name,
                    source: self.source,
                    plan_data: output.plan_data,
                    estimate,
                })
            }
            QUERY_SOURCE_FAILED => {
                return Err(unsafe { error.to_error("query source planning") });
            }
            status => {
                return Err(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!("query source planning returned unknown status {status}"),
                ));
            }
        };

        if let Some(existing) = &self.owned {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!(
                    "query source {} is owned by both {:?} and {:?}",
                    self.source.index(),
                    existing.provider_name(),
                    provider_name,
                ),
            ));
        }
        self.owned = Some(resolution);
        Ok(())
    }

    fn finish(self) -> Option<PlannedSourceRecord> {
        match self.owned {
            None | Some(OwnedResolution::Unsupported { .. }) => None,
            Some(OwnedResolution::Planned(source)) => Some(source),
        }
    }
}

/// Resolve the immutable callbacks for a provider referenced by selected plan
/// data. Absence or duplication is a selected-path invariant violation, not a
/// capability decline.
pub(crate) fn serial_source_callbacks(
    provider: ProviderId,
) -> Result<SerialSourceCallbacks, PgReportError> {
    let mut found = None;
    snapshot().try_for_each(|descriptor| {
        if descriptor.provider_id != provider {
            return Ok(());
        }
        let provider_name = unsafe { CStr::from_ptr(descriptor.provider_name) };
        if found.is_some() {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!(
                    "provider {provider_name:?} ({}) has multiple registered query source descriptors",
                    provider.index(),
                ),
            ));
        }
        found = Some(unsafe {
            SerialSourceCallbacks::from_validated_callbacks(
                descriptor.context,
                descriptor.prepare_source,
                descriptor.open_serial_stream,
                descriptor.release_prepared,
            )
        });
        Ok(())
    })?;
    found.ok_or_else(|| {
        let message = match provider_bootstrap::provider_name(provider) {
            Some(provider_name) => format!(
                "selected query plan references provider {:?} ({}) without a query source descriptor",
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

pub(crate) fn resolve_count_rows(
    source: SourceId,
    root: *mut pg_sys::PlannerInfo,
    relation: *mut pg_sys::RelOptInfo,
    range_table_index: pg_sys::Index,
    range_table_entry: *mut pg_sys::RangeTblEntry,
) -> Result<Option<PlannedSourceRecord>, PgReportError> {
    let mut resolver = SourceResolver::new(
        source,
        root,
        relation,
        range_table_index,
        range_table_entry,
    );
    snapshot().try_for_each(|descriptor| resolver.visit(descriptor))?;
    Ok(resolver.finish())
}
