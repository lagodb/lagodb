//! Unix listener that accepts storage peers and runs each stream through the wire connection processor (`process_connection_with_drain_timeout`).
//!
//! [`StorageServer`] also owns a shared [`CancellationToken`] for the cache subsystem's
//! background tasks (cleanup scheduler, large-fill reaper). The token is cancelled on
//! [`Drop`] or explicit shutdown so the [`CacheManager`]-owned actors can exit cleanly.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::net::UnixListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::backend::ManagedStoreRegistry;
use crate::cache::CacheIndex;
use crate::config::StorageServerConfig;
use crate::connection::{attach, process_connection_with_drain_timeout};
use crate::error::{StorageError, StorageResult};
use crate::request::RequestHooks;
use crate::service::StorageService;
use crate::session::StorageContext;
use crate::transport::bind_storage_unix_listener;

/// Bound Unix socket path plus shared [`StorageService`] and [`StorageServerConfig`] forked into each accepted connection.
pub struct StorageServer<I: CacheIndex> {
    listener: UnixListener,
    socket_path: PathBuf,
    service: Arc<StorageService<I>>,
    config: StorageServerConfig,
    request_hooks: RequestHooks,
    /// Token shared with background tasks (cleanup, etc.) — cancelled on Drop
    /// or explicit shutdown to ensure all owned tasks terminate.
    background_shutdown: CancellationToken,
}

impl<I: CacheIndex + 'static> StorageServer<I> {
    pub async fn bind(
        socket_path: impl AsRef<Path>,
        service: Arc<StorageService<I>>,
    ) -> StorageResult<Self> {
        Self::bind_with_config(socket_path, service, StorageServerConfig::default())
            .await
    }

    pub async fn bind_with_config(
        socket_path: impl AsRef<Path>,
        service: Arc<StorageService<I>>,
        config: StorageServerConfig,
    ) -> StorageResult<Self> {
        Self::bind_with_config_and_hooks(
            socket_path,
            service,
            config,
            RequestHooks::default(),
        )
        .await
    }

    pub async fn bind_with_config_and_hooks(
        socket_path: impl AsRef<Path>,
        service: Arc<StorageService<I>>,
        config: StorageServerConfig,
        request_hooks: RequestHooks,
    ) -> StorageResult<Self> {
        let config = config.normalized();
        config.validate_for_max_read_size(service.max_read_size())?;
        let socket_path = socket_path.as_ref().to_path_buf();
        let listener = bind_storage_unix_listener(&socket_path)?;
        info!("listening on storage socket {}", socket_path.display());
        Ok(Self {
            listener,
            socket_path,
            service,
            config,
            request_hooks,
            background_shutdown: CancellationToken::new(),
        })
    }

    /// Cancellation token that gates every background task owned by the cache subsystem
    /// (cleanup scheduler, large-fill reaper). Cancelled on [`Drop`] or explicit shutdown so
    /// the actors exit deterministically.
    ///
    /// The builder hands this token to [`crate::cache::CacheManager::spawn_cleanup_scheduler`]
    /// so the server's lifecycle wins over the actor's.
    pub(crate) fn background_shutdown_token(&self) -> CancellationToken {
        self.background_shutdown.clone()
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub fn managed_store_registry(&self) -> ManagedStoreRegistry {
        self.service.managed_stores().clone()
    }

    /// Accept connections until `shutdown` is cancelled, then stop accepting.
    ///
    /// Already-spawned connections are **not** awaited here; they continue running
    /// until the peer closes or the Tokio runtime shuts down.
    pub async fn serve_until(
        &self,
        shutdown: CancellationToken,
    ) -> StorageResult<()> {
        let limiter = Arc::new(Semaphore::new(self.config.max_connections.max(1)));

        loop {
            tokio::select! {
                biased;

                _ = shutdown.cancelled() => {
                    self.background_shutdown.cancel();
                    info!("storage server shutdown requested");
                    return Ok(());
                }

                accepted = self.accept_one(&limiter) => {
                    let (stream, permit) = accepted?;
                    self.spawn_connection(stream, permit);
                }
            }
        }
    }

    pub async fn serve_forever(&self) -> StorageResult<()> {
        self.serve_until(CancellationToken::new()).await
    }

    async fn accept_one(
        &self,
        limiter: &Arc<Semaphore>,
    ) -> StorageResult<(tokio::net::UnixStream, OwnedSemaphorePermit)> {
        let permit = limiter.clone().acquire_owned().await.map_err(|error| {
            StorageError::io(
                "connection limiter closed",
                std::io::Error::other(error),
            )
        })?;
        let (stream, _) = self.listener.accept().await?;
        Ok((stream, permit))
    }

    fn spawn_connection(
        &self,
        stream: tokio::net::UnixStream,
        permit: OwnedSemaphorePermit,
    ) {
        let config = self.config.clone();
        let service = self.service.clone();
        let request_hooks = self.request_hooks.clone();

        tokio::spawn(async move {
            let _permit = permit;
            let client_addr: Arc<str> = Arc::from("unix");
            let (stream, attached) =
                attach(stream, &service, &request_hooks, Arc::clone(&client_addr))
                    .await?;
            let context = StorageContext::new_attached_with_hooks_and_handle_limit(
                client_addr,
                service,
                request_hooks,
                config.max_open_handles_per_connection,
                attached,
            );
            info!(
                client_addr = &*context.client_addr,
                "accepted storage connection"
            );
            if let Err(error) =
                process_connection_with_drain_timeout(stream, context, config).await
            {
                debug!("storage connection closed: {error}");
            }
            Ok::<(), StorageError>(())
        });
    }
}

impl<I: CacheIndex> Drop for StorageServer<I> {
    fn drop(&mut self) {
        self.background_shutdown.cancel();
    }
}
