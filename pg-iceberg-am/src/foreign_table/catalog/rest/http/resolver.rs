use std::fmt;

use http::Uri;
use pgrx::pg_sys;
use ureq::Error;
use ureq::config::Config;
use ureq::unversioned::resolver::{DefaultResolver, ResolvedSocketAddrs, Resolver};
use ureq::unversioned::transport::NextTimeout;
use ureq::unversioned::transport::time::Duration;

/// Keeps ureq's system resolver synchronous. A finite resolver timeout makes
/// ureq spawn a helper thread, which is not permitted in a PostgreSQL backend.
///
/// Replacing ureq's deadline with `NotHappening` is intentional: system DNS is
/// allowed to block past the REST global deadline, as PostgreSQL's own
/// `pg_getaddrinfo_all()` path does. During a transaction callback that also
/// means the backend can retain transaction resources and locks until DNS
/// returns. Preserving a single-threaded backend is the chosen tradeoff.
#[derive(Default)]
pub(super) struct PostgresResolver {
    inner: DefaultResolver,
}

impl Resolver for PostgresResolver {
    fn resolve(
        &self,
        uri: &Uri,
        config: &Config,
        timeout: NextTimeout,
    ) -> Result<ResolvedSocketAddrs, Error> {
        let result = self.inner.resolve(
            uri,
            config,
            NextTimeout {
                after: Duration::NotHappening,
                reason: timeout.reason,
            },
        );
        // The system getaddrinfo call is deliberately blocking and outside the
        // REST deadline, matching PostgreSQL's pg_getaddrinfo_all(). Deliver a
        // pending cancel only after getaddrinfo returns.
        pg_sys::check_for_interrupts!();
        result
    }
}

impl fmt::Debug for PostgresResolver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresResolver")
            .finish_non_exhaustive()
    }
}
