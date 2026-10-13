//! Selects a complete response identity independently of connection fallback.

use std::iter::once;

use super::super::routes::ObjectProvider;
use super::ProviderConfig;

const S3_IDENTITY_PROPERTIES: &[&str] = &[
    "s3.access-key-id",
    "s3.secret-access-key",
    "s3.session-token",
    "client.assume-role.arn",
    "client.assume-role.external-id",
    "client.assume-role.session-name",
];

const GCS_IDENTITY_PROPERTIES: &[&str] =
    &["gcs.credentials-json", "gcs.oauth2.token"];

const AZURE_IDENTITY_PROPERTIES: &[&str] = &[
    "adls.account-key",
    "adls.sas-token",
    "adls.bearer-token",
    "adls.client-id",
    "adls.client-secret",
    "adls.tenant-id",
];

impl<'a> ProviderConfig<'a> {
    /// Select one layer before parsing any identity fields. Scoped identities
    /// replace catalog identities, including their token and AssumeRole chain.
    /// False anonymous switches preserve credentials and disable lower anonymous
    /// modes. A credential layer stops fallback for both credentials and mode.
    /// Returns the identity and its anonymous mode, or a switch-only override.
    pub(super) fn identity(
        &self,
        provider: ObjectProvider,
    ) -> (Option<Self>, Option<bool>) {
        let (keys, switches): (&[&str], &[&str]) = match provider {
            ObjectProvider::S3 => (S3_IDENTITY_PROPERTIES, &["s3.allow-anonymous"]),
            ObjectProvider::Gcs => (
                GCS_IDENTITY_PROPERTIES,
                &["gcs.no-auth", "gcs.allow-anonymous"],
            ),
            ObjectProvider::Azure => (AZURE_IDENTITY_PROPERTIES, &[]),
        };
        let mut anonymous = None;
        for properties in self.overrides.into_iter().chain(once(self.properties)) {
            let layer = Self::with_profile(properties, None);
            if anonymous.is_none() && layer.has_any(switches) {
                anonymous = Some(switches.iter().any(|key| layer.truthy(key)));
            }
            if layer.has_any(keys) || anonymous == Some(true) {
                return (Some(layer), Some(anonymous.unwrap_or(false)));
            }
        }
        (None, anonymous)
    }

    /// Partial response identities are never completed from a local mapping.
    pub(in super::super) fn has_response_identity(
        &self,
        provider: ObjectProvider,
    ) -> bool {
        self.identity(provider).0.is_some()
    }

    /// Only an explicitly complete response bypasses local address discovery.
    pub(in super::super) fn has_connection(
        &self,
        provider: ObjectProvider,
        account_from_uri: Option<&str>,
    ) -> bool {
        match provider {
            ObjectProvider::S3 => {
                self.has("s3.endpoint")
                    && (self.has("client.region") || self.has("s3.region"))
                    && self.has("s3.path-style-access")
            }
            ObjectProvider::Gcs => self.has("gcs.service.host"),
            ObjectProvider::Azure => {
                self.has("adls.endpoint")
                    && (self.has("adls.account-name") || account_from_uri.is_some())
            }
        }
    }
}
