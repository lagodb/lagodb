//! PostgreSQL 16 ANALYZE sample budget for table access methods.

use pgrx::{PgSqlErrorCode, pg_sys};

use super::RelationHandle;
use crate::catalog::search_syscache1;
use crate::diag::PgReportError;
use crate::wrapper::PgWrapper;

/// `std_typanalyze()` sets `minrows` to this multiple of the statistics target.
const ROWS_PER_STATISTICS_TARGET: u64 = 300;
/// `do_analyze_rel()` starts `targrows` here before raising it to any `minrows`.
const MIN_SAMPLE_ROWS: u64 = 100;

impl RelationHandle<'_> {
    /// Sample rows a table AM offers PostgreSQL 16's ANALYZE of this relation.
    ///
    /// PostgreSQL 16 does not pass the selected columns, `targrows`, or an
    /// inherited scan's `childtargrows` to a table AM. For a non-inherited scan,
    /// the largest relation-local target covers the built-in analyzers and
    /// extended statistics, although a column-list ANALYZE can overproduce
    /// candidates because this method also sees unselected columns.
    ///
    /// During inherited ANALYZE, PostgreSQL derives `targrows` from the parent
    /// and allocates `childtargrows` by child size. A child's relation-local
    /// budget can therefore be smaller than its assigned `childtargrows`, which
    /// under-represents that child in the parent's MCV and histogram samples.
    /// This is a known PostgreSQL 16 limitation while LagoDB does not own or
    /// fork the ANALYZE executor.
    ///
    /// A custom `typanalyze` may set a larger `minrows`. That field is a request
    /// ANALYZE need not honor, so the column then gets a smaller sample.
    pub fn analyze_sample_budget(&self) -> Result<u64, PgReportError> {
        // SAFETY: PostgreSQL's backend-local GUC value is readable here.
        let default_target = unsafe { pg_sys::default_statistics_target };
        let resolve = |target: i32| if target < 0 { default_target } else { target };

        let attributes = self.tuple_desc_handle().attributes();
        let mut max_target = attributes
            .iter()
            .filter(|attr| !attr.attisdropped)
            .map(|attr| resolve(i32::from(attr.attstattarget)))
            .max()
            .unwrap_or(0);

        for statistics_oid in
            unsafe { PgWrapper::relation_stat_ext_oids(self.as_raw())? }
        {
            // ANALYZE's ShareUpdateExclusiveLock excludes DROP STATISTICS, so
            // every OID in the relcache list has a catalog row.
            let tuple = search_syscache1(
                pg_sys::SysCacheIdentifier::STATEXTOID as i32,
                pg_sys::Datum::from(statistics_oid),
            )
            .ok_or_else(|| {
                PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!(
                        "cache lookup failed for statistics object {statistics_oid}"
                    ),
                )
            })?;
            // SAFETY: the pinned tuple is a pg_statistic_ext row, whose fixed
            // fields through stxkeys are never NULL.
            let statistics = unsafe {
                &*pg_sys::GETSTRUCT(tuple.as_raw())
                    .cast::<pg_sys::FormData_pg_statistic_ext>()
            };
            let target = if statistics.stxstattarget >= 0 {
                statistics.stxstattarget
            } else {
                // statext_compute_stattarget() takes the largest target of the
                // object's plain columns, after typanalyze resolved each -1. It
                // skips expressions, so a pure expression object gets the default.
                // SAFETY: stxkeys is an inline int2vector of dim1 elements.
                let keys = unsafe {
                    statistics
                        .stxkeys
                        .values
                        .as_slice(statistics.stxkeys.dim1 as usize)
                };
                keys.iter()
                    .map(|&key| {
                        resolve(i32::from(attributes[key as usize - 1].attstattarget))
                    })
                    .max()
                    .unwrap_or(default_target)
            };
            max_target = max_target.max(target);
        }
        Ok((ROWS_PER_STATISTICS_TARGET * max_target as u64).max(MIN_SAMPLE_ROWS))
    }
}
