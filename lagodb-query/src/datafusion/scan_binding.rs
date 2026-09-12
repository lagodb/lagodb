//! Query-local table-scan identity binding for DataFusion lowering.

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::common::{Column, Result, TableReference};
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::SessionContext;
use lagodb_core::query_contract::ScanId;
use pgrx::pg_sys;

use super::table_scan::ExternalTableProvider;

/// One semantic `ScanId` bound to a stable DataFusion relation qualifier.
pub(super) struct ScanBinding {
    provider: Arc<ExternalTableProvider>,
    qualifier: TableReference,
}

impl ScanBinding {
    fn new(scan: ScanId, provider: Arc<ExternalTableProvider>) -> Self {
        Self {
            provider,
            qualifier: TableReference::bare(format!(
                "__lagodb_scan_{}",
                scan.index()
            )),
        }
    }

    pub(super) fn frame(&self, session: &SessionContext) -> Result<DataFrame> {
        let provider: Arc<dyn TableProvider> = self.provider.clone();
        session.read_table(provider)?.alias(self.qualifier.table())
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
                ScanBinding::new(ScanId::from_index(index), Arc::clone(provider))
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { entries }
    }

    pub(super) fn get(&self, scan: ScanId) -> Option<&ScanBinding> {
        self.entries.get(scan.index())
    }
}
