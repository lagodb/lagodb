//! [`CustomScanError`]: the sole public error type for the customscan module.

use core::ffi::CStr;
use std::ffi::CString;
use std::fmt::Display;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use crate::customscan::plan_data::EnvelopeError;
use crate::diag::{PgReportError, PgReportParts, PgReportableError, SqlStateError};
use crate::plan_data::PlanDataError;

/// Executor callback phase; only trampolines attach this via
/// [`CustomScanError::with_callback_phase`].
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
            Self::Begin => "BeginCustomScan",
            Self::ReScan => "ReScanCustomScan",
            Self::NextSlot => "ExecCustomScan access",
        }
    }
}

/// Domain error for customscan framework and provider boundaries.
///
/// Variants are not public: AM code uses [`Self::provider`] and
/// [`Self::internal`]. Framework/trampoline code uses `pub(crate)` constructors.
#[derive(Debug)]
pub struct CustomScanError(Box<CustomScanErrorKind>);

#[derive(Debug, Error)]
enum CustomScanErrorKind {
    /// Context attached exactly once by the PostgreSQL FFI trampoline.
    #[error("customscan {:?} {} callback failed: {source}", provider, phase.as_str())]
    Callback {
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

    /// Framework/custom_private codec failure (not a provider domain error).
    #[error("customscan custom_private codec error: {source}")]
    Codec {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("{message}")]
    Framework { message: String },

    #[error("customscan internal error: {source}")]
    Internal {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("{report}")]
    PgReport { report: PgReportParts },
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

    /// Wrap a custom_private codec error (always `INTERNAL_ERROR`).
    pub(crate) fn private_codec(
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::new(CustomScanErrorKind::Codec {
            source: Box::new(source),
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

    pub(crate) fn custom_exprs_missing(
        binding_count: usize,
        pushed_count: usize,
    ) -> Self {
        Self::framework(format!(
            "customscan BeginCustomScan: custom_exprs is NULL but \
             binding_count={binding_count} pushed_count={pushed_count}"
        ))
    }

    pub(crate) fn custom_exprs_length_mismatch(got: usize, expected: usize) -> Self {
        Self::framework(format!(
            "customscan BeginCustomScan: custom_exprs length mismatch \
             (got {got}, expected binding_count + pushed_count = {expected})"
        ))
    }

    pub(crate) fn multi_provider_match(relid: u32) -> Self {
        Self::framework(format!(
            "multiple LagodbCustomScanProviders match relation {relid}"
        ))
    }

    pub(crate) fn required_modify_path(provider: &CStr) -> Self {
        Self::framework(format!(
            "required Modify CustomScan provider {provider:?} emitted no path"
        ))
    }

    pub(crate) fn modify_binding(message: impl Into<String>) -> Self {
        Self::framework(message.into())
    }

    pub(crate) fn provider_name_mismatch(expected: CString, found: CString) -> Self {
        Self::framework(format!(
            "customscan: provider name mismatch in custom_private \
             (expected {expected:?}, found {found:?}); this indicates a corrupt \
             plan tree or a stale cached plan referencing a renamed provider"
        ))
    }

    /// Attach trampoline context before reporting at the FFI boundary.
    pub(crate) fn with_callback_phase(
        self,
        provider: &'static CStr,
        phase: CustomScanPhase,
    ) -> Self {
        Self::new(CustomScanErrorKind::Callback {
            provider,
            phase,
            source: Box::new(self),
        })
    }

    /// Raise as a PostgreSQL ERROR through the shared diagnostic boundary.
    pub(crate) fn report(self) -> ! {
        PgReportError::raise(ErrorReport::from(self))
    }

    /// Preserve the CustomScan-specific message, DETAIL, and HINT mapping for
    /// a runtime-routed planning callback.
    pub(crate) fn into_report_error(self) -> PgReportError {
        self.report_parts().into_pg_report_error()
    }

    pub(crate) fn framework(message: impl Display) -> Self {
        Self::new(CustomScanErrorKind::Framework {
            message: message.to_string(),
        })
    }

    #[cfg(test)]
    fn kind(&self) -> &CustomScanErrorKind {
        &self.0
    }
}

impl SqlStateError for CustomScanError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match &*self.0 {
            CustomScanErrorKind::Callback { source, .. } => source.sql_error_code(),
            CustomScanErrorKind::Provider { sqlerrcode, .. } => *sqlerrcode,
            CustomScanErrorKind::Codec { .. }
            | CustomScanErrorKind::Framework { .. }
            | CustomScanErrorKind::Internal { .. } => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }
            CustomScanErrorKind::PgReport { report } => report.sqlerrcode,
        }
    }
}

impl From<PlanDataError> for CustomScanError {
    fn from(error: PlanDataError) -> Self {
        Self::private_codec(error)
    }
}

impl From<EnvelopeError> for CustomScanError {
    fn from(error: EnvelopeError) -> Self {
        Self::private_codec(error)
    }
}

impl PgReportableError for CustomScanError {
    fn append_nested_report_extras(
        &self,
        details: &mut Vec<String>,
        hints: &mut Vec<String>,
    ) {
        self.append_nested_pg_report_extras(details, hints);
    }
}

impl From<CustomScanError> for ErrorReport {
    fn from(error: CustomScanError) -> Self {
        error.into_error_report()
    }
}

/// Walk nested [`CustomScanError`] wrappers and collect [`CustomScanErrorKind::PgReport`] DETAIL/HINT.
impl CustomScanError {
    fn append_nested_pg_report_extras(
        &self,
        details: &mut Vec<String>,
        hints: &mut Vec<String>,
    ) {
        if let CustomScanErrorKind::Callback { source, .. } = &*self.0 {
            source.append_nested_pg_report_extras(details, hints);
        }
        if let CustomScanErrorKind::PgReport { report } = &*self.0 {
            if let Some(detail) = report.detail.clone() {
                details.push(detail);
            }
            if let Some(hint) = report.hint.clone() {
                hints.push(hint);
            }
        }
    }
}

impl From<PgReportError> for CustomScanError {
    fn from(err: PgReportError) -> Self {
        Self::new(CustomScanErrorKind::PgReport {
            report: PgReportParts::from_pg_report_error(err),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn callback_context_delegates_sqlstate_to_source() {
        let inner = CustomScanError::provider(SqlStateCarrier(
            PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
            "missing",
        ));
        let err = CustomScanError::new(CustomScanErrorKind::Callback {
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
            report: PgReportParts::new(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "report boom",
                None,
                None,
            ),
        });
        match err.kind() {
            CustomScanErrorKind::PgReport { report } => {
                assert!(report.message.contains("report boom"));
            }
            other => panic!("expected PgReport variant, got {other:?}"),
        }
        assert!(err.to_string().contains("report boom"), "got: {err}");
    }

    #[test]
    fn framework_variants_map_to_internal_sqlstate() {
        let cases: Vec<(CustomScanError, &str)> = vec![(
            CustomScanError::custom_exprs_missing(1, 0),
            "customscan BeginCustomScan: custom_exprs is NULL but \
             binding_count=1 pushed_count=0",
        )];

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
    fn envelope_error_maps_to_private_codec_with_internal_sqlstate() {
        let err: CustomScanError =
            EnvelopeError::MalformedTupleLayout { reason: "test" }.into();
        assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_INTERNAL_ERROR);
        let report = err.report_parts();
        assert!(
            report.message.contains("custom_private codec error"),
            "got: {}",
            report.message
        );
        assert!(
            report.message.contains("malformed"),
            "got: {}",
            report.message
        );
        assert!(report.message.contains("test"), "got: {}", report.message);
    }

    #[test]
    fn callback_context_preserves_nested_pg_report_detail_and_hint() {
        let inner = CustomScanError::new(CustomScanErrorKind::PgReport {
            report: PgReportParts::new(
                PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
                "tuple write failed",
                Some("column 3 out of range".to_owned()),
                Some("check projection list".to_owned()),
            ),
        });
        let err = CustomScanError::new(CustomScanErrorKind::Callback {
            provider: c"pg-test",
            phase: CustomScanPhase::NextSlot,
            source: Box::new(inner),
        });
        assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_DATA_EXCEPTION);
        let report = err.report_parts();
        let detail = report
            .detail
            .expect("nested PgReport DETAIL must survive Callback");
        assert!(detail.contains("column 3 out of range"), "detail: {detail}");
        let hint = report
            .hint
            .expect("nested PgReport HINT must survive Callback");
        assert!(hint.contains("check projection list"), "hint: {hint}");
    }

    #[test]
    fn callback_report_includes_nested_provider_in_message() {
        let inner = CustomScanError::provider(InnerError("disk gone"));
        let callback = CustomScanError::new(CustomScanErrorKind::Callback {
            provider: c"pg-test",
            phase: CustomScanPhase::NextSlot,
            source: Box::new(inner),
        });
        let report = callback.report_parts();
        assert!(
            report.message.contains("inner boom"),
            "message: {}",
            report.message
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn framework_carrier_byte_identity(
            binding_count in any::<usize>(),
            pushed_count in any::<usize>(),
            got in any::<usize>(),
            expected_len in any::<usize>(),
            relid in any::<u32>(),
        ) {
            let e = CustomScanError::custom_exprs_missing(binding_count, pushed_count);
            prop_assert_eq!(e.sql_error_code(), PgSqlErrorCode::ERRCODE_INTERNAL_ERROR);
            prop_assert_eq!(
                format!("{e}"),
                format!(
                    "customscan BeginCustomScan: custom_exprs is NULL but \
                     binding_count={} pushed_count={}",
                    binding_count, pushed_count
                )
            );

            let e = CustomScanError::custom_exprs_length_mismatch(got, expected_len);
            prop_assert_eq!(e.sql_error_code(), PgSqlErrorCode::ERRCODE_INTERNAL_ERROR);

            let e = CustomScanError::multi_provider_match(relid);
            prop_assert_eq!(
                format!("{e}"),
                format!("multiple LagodbCustomScanProviders match relation {}", relid)
            );
        }
    }
}
