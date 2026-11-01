//! Query-local table-scan identity binding for DataFusion lowering.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{Column, Result, TableReference};
use datafusion::datasource::{empty::EmptyTable, provider_as_source};
use datafusion::logical_expr::{LogicalPlanBuilder, TableSource, UNNAMED_TABLE};
use lagodb_core::query_contract::ScanId;
use pgrx::pg_sys;

use crate::plan::QueryFragment;

use super::parallel::ParallelTableProvider;
use super::table_scan::ExternalTableProvider;

/// Projected column positions indexed by `attno - 1`, the dense layout
/// PostgreSQL uses for attribute maps.
///
/// Query planning lowers only user attributes, so every attno is `>= 1`.
#[derive(Debug, Clone)]
pub(in crate::datafusion) struct AttnoPositions(Arc<[Option<usize>]>);

impl AttnoPositions {
    /// Map each attno to its position in `attnos`.
    pub(in crate::datafusion) fn from_attnos(
        attnos: impl IntoIterator<Item = pg_sys::AttrNumber>,
    ) -> Self {
        let mut positions = Vec::new();
        for (position, attno) in attnos.into_iter().enumerate() {
            let index = Self::index(attno);
            if positions.len() <= index {
                positions.resize(index + 1, None);
            }
            positions[index] = Some(position);
        }
        Self(positions.into())
    }

    /// Position of `attno`, or `None` when this scan does not project it.
    pub(in crate::datafusion) fn position(
        &self,
        attno: pg_sys::AttrNumber,
    ) -> Option<usize> {
        self.0.get(Self::index(attno)).copied().flatten()
    }

    const fn index(attno: pg_sys::AttrNumber) -> usize {
        (attno - 1) as usize
    }
}

enum ScanProvider {
    Serial(Arc<ExternalTableProvider>),
    Parallel(Arc<ParallelTableProvider>),
    Planning {
        schema: SchemaRef,
        positions: AttnoPositions,
    },
}

impl ScanProvider {
    fn source(&self) -> Arc<dyn TableSource> {
        let provider: Arc<dyn TableProvider> = match self {
            Self::Serial(provider) => provider.clone(),
            Self::Parallel(provider) => provider.clone(),
            Self::Planning { schema, .. } => {
                Arc::new(EmptyTable::new(Arc::clone(schema)))
            }
        };
        provider_as_source(provider)
    }

    fn column_name(&self, attno: pg_sys::AttrNumber) -> Option<&str> {
        match self {
            Self::Serial(provider) => provider.column_name(attno),
            Self::Parallel(provider) => provider.column_name(attno),
            Self::Planning { schema, positions } => {
                Some(schema.field(positions.position(attno)?).name())
            }
        }
    }
}

/// One semantic `ScanId` bound to a stable DataFusion relation qualifier.
pub(super) struct ScanBinding {
    provider: ScanProvider,
    qualifier: TableReference,
}

impl ScanBinding {
    fn new(scan: ScanId, provider: ScanProvider) -> Self {
        Self {
            provider,
            qualifier: TableReference::bare(format!(
                "__lagodb_scan_{}",
                scan.index()
            )),
        }
    }

    pub(super) fn plan(&self) -> Result<LogicalPlanBuilder> {
        // Preserve SessionContext::read_table(...).alias(...) plan shape.
        LogicalPlanBuilder::scan(UNNAMED_TABLE, self.provider.source(), None)?
            .alias(self.qualifier.clone())
    }

    pub(super) fn column(&self, attno: pg_sys::AttrNumber) -> Option<Column> {
        self.provider
            .column_name(attno)
            .map(|name| Column::new(Some(self.qualifier.clone()), name))
    }
}

/// Dense query-local binding table indexed by `ScanId`.
pub(super) struct ScanBindings {
    entries: Box<[ScanBinding]>,
}

impl ScanBindings {
    pub(super) fn new(providers: &[Arc<ExternalTableProvider>]) -> Self {
        let entries = providers
            .iter()
            .enumerate()
            .map(|(index, provider)| {
                ScanBinding::new(
                    ScanId::from_index(index),
                    ScanProvider::Serial(Arc::clone(provider)),
                )
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { entries }
    }

    pub(super) fn parallel(providers: &[Arc<ParallelTableProvider>]) -> Self {
        let entries = providers
            .iter()
            .enumerate()
            .map(|(index, provider)| {
                ScanBinding::new(
                    ScanId::from_index(index),
                    ScanProvider::Parallel(Arc::clone(provider)),
                )
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { entries }
    }

    /// Schema-only sources for the same logical lowering used by execution.
    /// No provider scan is bound or opened during admission.
    pub(super) fn for_planning<'schema>(
        fragment: &QueryFragment,
        schemas: impl ExactSizeIterator<Item = &'schema SchemaRef>,
    ) -> Self {
        let entries = schemas
            .enumerate()
            .map(|(index, schema)| {
                let scan_id = ScanId::from_index(index);
                let scan = fragment
                    .scan(scan_id)
                    .expect("validated query references every planned scan");
                ScanBinding::new(
                    scan_id,
                    ScanProvider::Planning {
                        schema: Arc::clone(schema),
                        positions: AttnoPositions::from_attnos(
                            scan.columns().iter().map(|column| column.attno),
                        ),
                    },
                )
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { entries }
    }

    pub(super) fn get(&self, scan: ScanId) -> Option<&ScanBinding> {
        self.entries.get(scan.index())
    }
}
