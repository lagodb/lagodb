//! [`CustomScanError`]: the sole public error type for the customscan module.

use core::ffi::CStr;
use std::ffi::CString;

use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use crate::customscan::provider::LakebaseCustomScanProvider;
use crate::diag::{
    PgReportError, SqlStateError, error_source_chain_detail, join_error_details,
};

/// Executor callback phase; only trampolines attach this via [`CustomScanError::with_provider_phase`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CustomScanPhase {
    Begin,
    ReScan,
    NextSlot,
}

impl CustomScanPhase {
    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Begin => "begin",
            Self::ReScan => "rescan",
            Self::NextSlot => "next_slot",
        }
    }
}

/// Domain error for customscan framework and provider boundaries.
///
/// Variants are not public: AM code must use [`Self::provider`], [`Self::internal`], and
/// [`Self::predicate_build_at`]. Framework/trampoline code uses `pub(crate)` constructors.
#[derive(Debug)]
pub struct CustomScanError(Box<CustomScanErrorKind>);

#[derive(Debug, Error)]
enum CustomScanErrorKind {
    /// Trampoline-added context around a provider callback failure.
    #[error("customscan {:?} provider.{} failed: {source}", provider, phase.as_str())]
    Runtime {
        provider: &'static CStr,
        phase: CustomScanPhase,
        #[source]
        source: Box<CustomScanError>,
    },

    /// Provider-originated error with an explicit SQLSTATE.
    #[error("customscan provider error: {source}")]
    Provider {
        sqlerrcode: PgSqlErrorCode,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("customscan predicate construction failed")]
    PredicateBuild {
        pushed_index: Option<usize>,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("{message}: {source}")]
    Context {
        message: String,
        #[source]
        source: Box<CustomScanError>,
    },

    /// Framework/custom_private codec failure (not a provider domain error).
    #[error("customscan custom_private codec error: {source}")]
    Codec {
        #[source]
        source: crate::customscan::custom_private::DecodeError,
    },

    #[error("{message}")]
    Framework { message: String },

    #[error("customscan internal error: {source}")]
    Internal {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("{message}")]
    PgReport {
        sqlerrcode: PgSqlErrorCode,
        message: String,
        detail: Option<String>,
        hint: Option<String>,
    },
}

impl std::fmt::Display for CustomScanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&*self.0, f)
    }
}

impl std::error::Error for CustomScanError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl CustomScanError {
    fn new(kind: CustomScanErrorKind) -> Self {
        Self(Box::new(kind))
    }

    /// Wrap a domain error that implements [`SqlStateError`] for provider callbacks.
    pub fn provider<E>(err: E) -> Self
    where
        E: SqlStateError + std::error::Error + Send + Sync + 'static,
    {
        Self::new(CustomScanErrorKind::Provider {
            sqlerrcode: err.sql_error_code(),
            source: Box::new(err),
        })
    }

    /// Framework or provider invariant (always `INTERNAL_ERROR`).
    pub fn internal<E>(err: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::new(CustomScanErrorKind::Internal {
            source: Box::new(err),
        })
    }

    pub fn predicate_build_at(
        pushed_index: Option<usize>,
        err: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::new(CustomScanErrorKind::PredicateBuild {
            pushed_index,
            source: Box::new(err),
        })
    }

    /// Wrap any failure while encoding `custom_private` (provider payload or codec).
    pub(crate) fn encode_custom_private(source: impl Into<CustomScanError>) -> Self {
        Self::new(CustomScanErrorKind::Context {
            message: "customscan: failed to encode custom_private".to_owned(),
            source: Box::new(source.into()),
        })
    }

    /// Wrap a [`DecodeError`] from the custom_private codec (always `INTERNAL_ERROR`).
    pub(crate) fn private_codec(
        source: crate::customscan::custom_private::DecodeError,
    ) -> Self {
        Self::new(CustomScanErrorKind::Codec { source })
    }

    pub(crate) fn provider_private_decode<P: LakebaseCustomScanProvider>(
        source: impl Into<CustomScanError>,
    ) -> Self {
        Self::new(CustomScanErrorKind::Context {
            message: format!(
                "customscan {:?} provider failed to decode custom_private payload",
                P::NAME
            ),
            source: Box::new(source.into()),
        })
    }

    pub(crate) fn slot_not_filled(provider: &'static CStr) -> Self {
        Self::framework(format!(
            "customscan provider {provider:?} returned Ok(true) without filling \
             the scan slot (slot-non-empty invariant violated)"
        ))
    }

    pub(crate) fn slot_filled_at_eof(provider: &'static CStr) -> Self {
        Self::framework(format!(
            "customscan provider {provider:?} returned Ok(false) after filling \
             the scan slot (EOF requires an empty slot)"
        ))
    }

    pub(crate) fn scan_relation_oid_mismatch(expected: u32, opened: u32) -> Self {
        Self::framework(format!(
            "customscan BeginCustomScan: scan relation OID mismatch \
             (custom_private.relation_oid={expected}, ss_currentRelation->rd_id={opened})"
        ))
    }

    pub(crate) fn slice_null_with_nonzero_count(
        pushed_count: usize,
        recheck_count: usize,
    ) -> Self {
        Self::framework(format!(
            "customscan BeginCustomScan: custom_exprs is NULL but \
             pushed_count={pushed_count} recheck_count={recheck_count}"
        ))
    }

    pub(crate) fn slice_length_mismatch(got: usize, expected: usize) -> Self {
        Self::framework(format!(
            "customscan BeginCustomScan: custom_exprs length mismatch \
             (got {got}, expected pushed_count + recheck_count = {expected})"
        ))
    }

    pub(crate) fn multi_provider_match(relid: u32) -> Self {
        Self::framework(format!(
            "multiple LakebaseCustomScanProviders match relation {relid}"
        ))
    }

    pub(crate) fn provider_name_mismatch(expected: CString, found: CString) -> Self {
        Self::framework(format!(
            "customscan: provider name mismatch in custom_private \
             (expected {expected:?}, found {found:?}); this indicates a corrupt \
             plan tree or a stale cached plan referencing a renamed provider"
        ))
    }

    /// Attach trampoline context before reporting at the FFI boundary.
    pub(crate) fn with_provider_phase<P: LakebaseCustomScanProvider>(
        self,
        phase: CustomScanPhase,
    ) -> Self {
        Self::new(CustomScanErrorKind::Runtime {
            provider: P::NAME,
            phase,
            source: Box::new(self),
        })
    }

    /// Restore the pre-callback memory context, then raise via [`custom_scan_error_report`].
    pub(crate) fn report_after_switch(self, prior_ctx: pg_sys::MemoryContext) -> ! {
        unsafe {
            pg_sys::MemoryContextSwitchTo(prior_ctx);
        }
        self.report();
    }

    /// Raise as a PostgreSQL ERROR via [`custom_scan_error_report`].
    fn report(self) -> ! {
        pgrx::pg_sys::panic::ErrorReport::from(self)
            .report(pgrx::prelude::PgLogLevel::ERROR);
        unreachable!()
    }

    fn framework(message: String) -> Self {
        Self::new(CustomScanErrorKind::Framework { message })
    }

    #[cfg(test)]
    fn kind(&self) -> &CustomScanErrorKind {
        &self.0
    }
}

impl SqlStateError for CustomScanError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match &*self.0 {
            CustomScanErrorKind::Runtime { source, .. } => source.sql_error_code(),
            CustomScanErrorKind::Provider { sqlerrcode, .. } => *sqlerrcode,
            CustomScanErrorKind::PredicateBuild { .. }
            | CustomScanErrorKind::Context { .. }
            | CustomScanErrorKind::Codec { .. }
            | CustomScanErrorKind::Framework { .. }
            | CustomScanErrorKind::Internal { .. } => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }
            CustomScanErrorKind::PgReport { sqlerrcode, .. } => *sqlerrcode,
        }
    }
}

impl From<CustomScanError> for ErrorReport {
    fn from(err: CustomScanError) -> Self {
        custom_scan_error_report(err)
    }
}

struct CustomScanReportParts {
    sqlerrcode: PgSqlErrorCode,
    message: String,
    detail: Option<String>,
    hint: Option<String>,
}

fn custom_scan_error_report(err: CustomScanError) -> ErrorReport {
    let parts = custom_scan_error_report_parts(&err);
    let mut report = ErrorReport::new(parts.sqlerrcode, parts.message, "");
    if let Some(detail) = parts.detail {
        report = report.set_detail(detail);
    }
    if let Some(hint) = parts.hint {
        report = report.set_hint(hint);
    }
    report
}

fn custom_scan_error_report_parts(err: &CustomScanError) -> CustomScanReportParts {
    let sqlerrcode = err.sql_error_code();
    let mut detail_parts = Vec::new();
    if let Some(extra) = predicate_build_detail(err) {
        detail_parts.push(extra);
    }
    if let Some(chain) = error_source_chain_detail(err) {
        detail_parts.push(chain);
    }

    let mut nested_pg_details = Vec::new();
    let mut nested_pg_hints = Vec::new();
    collect_nested_pg_report_extras(
        err,
        &mut nested_pg_details,
        &mut nested_pg_hints,
    );
    detail_parts.extend(nested_pg_details);

    CustomScanReportParts {
        sqlerrcode,
        message: report_message(err),
        detail: join_error_details(detail_parts.into_iter().map(Some)),
        hint: join_error_details(nested_pg_hints.into_iter().map(Some)),
    }
}

fn report_message(err: &CustomScanError) -> String {
    match &*err.0 {
        CustomScanErrorKind::PgReport { message, .. } => message.clone(),
        _ => primary_message(err),
    }
}

/// Walk nested [`CustomScanError`] wrappers and collect [`CustomScanErrorKind::PgReport`] DETAIL/HINT.
fn collect_nested_pg_report_extras(
    err: &CustomScanError,
    details: &mut Vec<String>,
    hints: &mut Vec<String>,
) {
    match &*err.0 {
        CustomScanErrorKind::Runtime { source, .. }
        | CustomScanErrorKind::Context { source, .. } => {
            collect_nested_pg_report_extras(source, details, hints);
        }
        _ => {}
    }
    if let CustomScanErrorKind::PgReport { detail, hint, .. } = &*err.0 {
        if let Some(detail) = detail.clone() {
            details.push(detail);
        }
        if let Some(hint) = hint.clone() {
            hints.push(hint);
        }
    }
}

fn primary_message(err: &CustomScanError) -> String {
    match &*err.0 {
        CustomScanErrorKind::Runtime {
            provider,
            phase,
            source,
        } => format!(
            "customscan {:?} provider.{} failed: {source}",
            provider,
            phase.as_str()
        ),
        CustomScanErrorKind::PredicateBuild { .. } => {
            "customscan predicate construction failed".to_string()
        }
        _ => format!("{err}"),
    }
}

/// Structured predicate-build context only (pushed qual index). Source text lives in
/// [`error_source_chain_detail`].
fn predicate_build_detail(err: &CustomScanError) -> Option<String> {
    match &*err.0 {
        CustomScanErrorKind::PredicateBuild { pushed_index, .. } => {
            pushed_index.map(|i| {
                format!("customscan predicate construction failed at pushed qual {i}")
            })
        }
        CustomScanErrorKind::Runtime { source, .. }
        | CustomScanErrorKind::Context { source, .. } => {
            predicate_build_detail(source)
        }
        _ => None,
    }
}

impl<E> From<crate::expr::translator::BuildPredicateError<E>> for CustomScanError
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(err: crate::expr::translator::BuildPredicateError<E>) -> Self {
        Self::predicate_build_at(None, err)
    }
}

impl From<PgReportError> for CustomScanError {
    fn from(err: PgReportError) -> Self {
        let sqlerrcode = err.sql_error_code();
        let report = err.into_report();
        Self::new(CustomScanErrorKind::PgReport {
            sqlerrcode,
            message: report.message().to_string(),
            detail: report.detail().map(str::to_owned),
            hint: report.hint().map(str::to_owned),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::customscan::custom_private::DecodeError;
    use proptest::prelude::*;

    #[derive(Debug, thiserror::Error)]
    #[error("inner boom: {0}")]
    struct InnerError(&'static str);

    impl SqlStateError for InnerError {
        fn sql_error_code(&self) -> PgSqlErrorCode {
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
        }
    }

    #[derive(Debug, thiserror::Error)]
    struct SqlStateCarrier(PgSqlErrorCode, &'static str);

    impl std::fmt::Display for SqlStateCarrier {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.1)
        }
    }

    impl SqlStateError for SqlStateCarrier {
        fn sql_error_code(&self) -> PgSqlErrorCode {
            self.0
        }
    }

    #[test]
    fn provider_preserves_sqlstate() {
        let err = CustomScanError::provider(SqlStateCarrier(
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            "not supported",
        ));
        assert_eq!(
            err.sql_error_code(),
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
        );
    }

    #[test]
    fn provider_constructor_keeps_message() {
        let err = CustomScanError::provider(InnerError("disk gone"));
        match err.kind() {
            CustomScanErrorKind::Provider { source, .. } => {
                assert!(source.to_string().contains("inner boom"));
            }
            other => panic!("expected Provider variant, got {other:?}"),
        }
    }

    #[test]
    fn predicate_build_at_includes_index_in_detail() {
        let err =
            CustomScanError::predicate_build_at(Some(2), InnerError("bad qual"));
        let report = custom_scan_error_report_parts(&err);
        let detail = report.detail.expect("detail");
        assert!(detail.contains("pushed qual 2"));
        assert!(detail.contains("inner boom"));
    }

    #[test]
    fn runtime_delegates_sqlstate_to_source() {
        let inner = CustomScanError::provider(SqlStateCarrier(
            PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
            "missing",
        ));
        let err = CustomScanError::new(CustomScanErrorKind::Runtime {
            provider: c"test",
            phase: CustomScanPhase::Begin,
            source: Box::new(inner),
        });
        assert_eq!(
            err.sql_error_code(),
            PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT
        );
    }

    #[test]
    fn pg_report_variant_keeps_message() {
        let err = CustomScanError::new(CustomScanErrorKind::PgReport {
            sqlerrcode: PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            message: "report boom".to_owned(),
            detail: None,
            hint: None,
        });
        match err.kind() {
            CustomScanErrorKind::PgReport { message, .. } => {
                assert!(message.contains("report boom"));
            }
            other => panic!("expected PgReport variant, got {other:?}"),
        }
        assert!(err.to_string().contains("report boom"), "got: {err}");
    }

    #[test]
    fn framework_variants_map_to_internal_sqlstate() {
        let cases: Vec<(CustomScanError, &str)> = vec![
            (
                CustomScanError::slice_null_with_nonzero_count(1, 0),
                "customscan BeginCustomScan: custom_exprs is NULL but \
                 pushed_count=1 recheck_count=0",
            ),
            (
                CustomScanError::encode_custom_private(
                    CustomScanError::private_codec(
                        DecodeError::CountTooLargeToEncode { value: 99 },
                    ),
                ),
                "customscan: failed to encode custom_private",
            ),
        ];

        for (err, expected_prefix) in cases {
            assert_eq!(
                err.sql_error_code(),
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "variant {err:?}",
            );
            let text = format!("{err}");
            assert!(
                text.starts_with(expected_prefix),
                "expected prefix {expected_prefix:?}, got {text}"
            );
        }
    }

    #[test]
    fn decode_error_maps_to_private_codec_with_internal_sqlstate() {
        let err = CustomScanError::private_codec(DecodeError::NullPayload);
        assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_INTERNAL_ERROR);
        let report = custom_scan_error_report_parts(&err);
        assert!(
            report.message.contains("custom_private codec error"),
            "got: {}",
            report.message
        );
        assert!(report.message.contains("NULL"), "got: {}", report.message);
    }

    #[test]
    fn report_uses_custom_scan_error_report_not_domain_error_report() {
        let inner = CustomScanError::provider(SqlStateCarrier(
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            "feature missing",
        ));
        let err = CustomScanError::predicate_build_at(Some(1), InnerError("bad op"));
        let runtime = CustomScanError::new(CustomScanErrorKind::Runtime {
            provider: c"pg-test",
            phase: CustomScanPhase::Begin,
            source: Box::new(err),
        });
        let report = custom_scan_error_report_parts(&runtime);
        let detail = report.detail.expect("predicate index must be in DETAIL");
        let qual_pos = detail
            .find("pushed qual 1")
            .expect("pushed qual index line");
        let chain_pos = detail
            .find("inner boom: bad op")
            .expect("source chain line");
        assert!(
            qual_pos < chain_pos,
            "predicate context should precede source chain in DETAIL; got: {detail}"
        );
        let _ = inner;
    }

    #[test]
    fn runtime_preserves_nested_pg_report_detail_and_hint() {
        let inner = CustomScanError::new(CustomScanErrorKind::PgReport {
            sqlerrcode: PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
            message: "tuple write failed".to_string(),
            detail: Some("column 3 out of range".to_string()),
            hint: Some("check projection list".to_string()),
        });
        let err = CustomScanError::new(CustomScanErrorKind::Runtime {
            provider: c"pg-test",
            phase: CustomScanPhase::NextSlot,
            source: Box::new(inner),
        });
        assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_DATA_EXCEPTION);
        let report = custom_scan_error_report_parts(&err);
        let detail = report
            .detail
            .expect("nested PgReport DETAIL must survive Runtime");
        assert!(detail.contains("column 3 out of range"), "detail: {detail}");
        let hint = report
            .hint
            .expect("nested PgReport HINT must survive Runtime");
        assert!(hint.contains("check projection list"), "hint: {hint}");
    }

    #[test]
    fn runtime_report_includes_nested_provider_in_message() {
        let inner = CustomScanError::provider(InnerError("disk gone"));
        let runtime = CustomScanError::new(CustomScanErrorKind::Runtime {
            provider: c"pg-test",
            phase: CustomScanPhase::NextSlot,
            source: Box::new(inner),
        });
        let report = custom_scan_error_report_parts(&runtime);
        assert!(
            report.message.contains("inner boom"),
            "message: {}",
            report.message
        );
    }

    #[test]
    fn provider_private_decode_uses_internal_sqlstate_even_for_nested_provider() {
        let inner = CustomScanError::provider(SqlStateCarrier(
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            "not supported",
        ));
        let err = CustomScanError::new(CustomScanErrorKind::Context {
            message:
                "customscan \"test\" provider failed to decode custom_private payload"
                    .to_owned(),
            source: Box::new(inner),
        });
        assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_INTERNAL_ERROR);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn framework_carrier_byte_identity(
            pushed_count in any::<usize>(),
            recheck_count in any::<usize>(),
            got in any::<usize>(),
            expected_len in any::<usize>(),
            relid in any::<u32>(),
        ) {
            let e = CustomScanError::slice_null_with_nonzero_count(pushed_count, recheck_count);
            prop_assert_eq!(e.sql_error_code(), PgSqlErrorCode::ERRCODE_INTERNAL_ERROR);
            prop_assert_eq!(
                format!("{e}"),
                format!(
                    "customscan BeginCustomScan: custom_exprs is NULL but \
                     pushed_count={} recheck_count={}",
                    pushed_count, recheck_count
                )
            );

            let e = CustomScanError::slice_length_mismatch(got, expected_len);
            prop_assert_eq!(e.sql_error_code(), PgSqlErrorCode::ERRCODE_INTERNAL_ERROR);

            let e = CustomScanError::multi_provider_match(relid);
            prop_assert_eq!(
                format!("{e}"),
                format!("multiple LakebaseCustomScanProviders match relation {}", relid)
            );
        }
    }
}
