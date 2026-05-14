//! Unix listener that accepts storage peers and runs each stream through the wire connection processor (`process_connection_with_drain_timeout`).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::net::UnixListener;
use tokio::sync::Semaphore;
use tracing::{debug, info};

use crate::backend::StoreRegistry;
use crate::cache::CacheIndex;
use crate::config::StorageServerConfig;
use crate::connection::process_connection_with_drain_timeout;
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
        })
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub fn store_registry(&self) -> StoreRegistry {
        self.service.registry().clone()
    }

    pub async fn serve_forever(&self) -> StorageResult<()> {
        let connection_limiter =
            Arc::new(Semaphore::new(self.config.max_connections.max(1)));
        loop {
            let connection_permit =
                connection_limiter.clone().acquire_owned().await.map_err(
                    |error| {
                        StorageError::io(
                            "connection limiter closed",
                            std::io::Error::other(error),
                        )
                    },
                )?;
            let (stream, _) = self.listener.accept().await?;
            let config = self.config.clone();
            let context = StorageContext::new_with_hooks_and_handle_limit(
                "unix",
                self.service.clone(),
                self.request_hooks.clone(),
                config.max_open_handles_per_connection,
            );
            info!(
                client_addr = &*context.client_addr,
                "accepted storage connection"
            );
            tokio::spawn(async move {
                let _connection_permit = connection_permit;
                if let Err(error) =
                    process_connection_with_drain_timeout(stream, context, config)
                        .await
                {
                    debug!("storage connection closed: {error}");
                }
            });
        }
    }
}
