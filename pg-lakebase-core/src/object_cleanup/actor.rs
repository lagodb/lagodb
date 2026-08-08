//! Bounded storage actor pool for object cleanup execution.

use std::any::Any;
use std::panic::{self, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use pg_lakebase_storage::{StorageClient, StorageError};

use super::item::{ObjectCleanupItem, ObjectCleanupItemId};
use super::runner::{
    ObjectCleanupExecutionError, ObjectCleanupExecutionOutcome, ObjectCleanupExecutor,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ActorRuntimeConfig {
    pub(crate) page_size: usize,
    pub(crate) request_timeout: Duration,
}

enum ActorCommand {
    Execute(Arc<ObjectCleanupItem>),
    Shutdown,
}

pub(crate) struct ActorResult {
    pub(crate) actor_id: usize,
    pub(crate) item_id: ObjectCleanupItemId,
    pub(crate) outcome: ObjectCleanupExecutionOutcome,
}

struct ActorHandle {
    sender: mpsc::SyncSender<ActorCommand>,
    join: Option<JoinHandle<()>>,
    busy: bool,
}

pub(crate) struct ObjectCleanupActorPool {
    actors: Vec<ActorHandle>,
    results: mpsc::Receiver<ActorResult>,
    shutdown: Arc<AtomicBool>,
    runtime_config: Arc<RwLock<ActorRuntimeConfig>>,
}

impl ObjectCleanupActorPool {
    pub(crate) fn start(
        actor_count: usize,
        socket_path: PathBuf,
        runtime_config: ActorRuntimeConfig,
    ) -> std::io::Result<Self> {
        let shutdown = Arc::new(AtomicBool::new(false));
        let runtime_config = Arc::new(RwLock::new(runtime_config));
        let (result_sender, results) =
            mpsc::sync_channel(actor_count.saturating_mul(2).max(1));
        let mut actors = Vec::with_capacity(actor_count);
        for actor_id in 0..actor_count {
            let (sender, receiver) = mpsc::sync_channel(1);
            let actor_shutdown = Arc::clone(&shutdown);
            let actor_config = Arc::clone(&runtime_config);
            let actor_results = result_sender.clone();
            let actor_socket = socket_path.clone();
            let join = thread::Builder::new()
                .name(format!("lakebase-maintenance-{actor_id}"))
                .spawn(move || {
                    actor_loop(
                        actor_id,
                        actor_socket,
                        actor_config,
                        actor_shutdown,
                        receiver,
                        actor_results,
                    );
                })?;
            actors.push(ActorHandle {
                sender,
                join: Some(join),
                busy: false,
            });
        }
        Ok(Self {
            actors,
            results,
            shutdown,
            runtime_config,
        })
    }

    pub(crate) fn capacity(&self) -> usize {
        self.actors.iter().filter(|actor| !actor.busy).count()
    }

    pub(crate) fn dispatch(
        &mut self,
        item: Arc<ObjectCleanupItem>,
    ) -> Result<usize, Arc<ObjectCleanupItem>> {
        let Some((actor_id, actor)) = self
            .actors
            .iter_mut()
            .enumerate()
            .find(|(_, actor)| !actor.busy)
        else {
            return Err(item);
        };
        match actor.sender.send(ActorCommand::Execute(item)) {
            Ok(()) => {
                actor.busy = true;
                Ok(actor_id)
            }
            Err(error) => match error.0 {
                ActorCommand::Execute(item) => Err(item),
                ActorCommand::Shutdown => unreachable!("dispatch sends Execute"),
            },
        }
    }

    pub(crate) fn try_result(&mut self) -> Option<ActorResult> {
        let result = self.results.try_recv().ok()?;
        if let Some(actor) = self.actors.get_mut(result.actor_id) {
            actor.busy = false;
        }
        Some(result)
    }

    pub(crate) fn reload(&self, config: ActorRuntimeConfig) {
        *self
            .runtime_config
            .write()
            .expect("maintenance actor config lock poisoned") = config;
    }

    pub(crate) fn request_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        for actor in &self.actors {
            let _ = actor.sender.send(ActorCommand::Shutdown);
        }
    }

    pub(crate) fn all_finished(&self) -> bool {
        self.actors
            .iter()
            .all(|actor| actor.join.as_ref().is_none_or(|join| join.is_finished()))
    }

    pub(crate) fn has_finished_actor(&self) -> bool {
        self.actors
            .iter()
            .any(|actor| actor.join.as_ref().is_some_and(|join| join.is_finished()))
    }

    pub(crate) fn join_finished(&mut self) {
        for actor in &mut self.actors {
            if actor.join.as_ref().is_some_and(|join| join.is_finished())
                && let Some(join) = actor.join.take()
            {
                let _ = join.join();
            }
        }
    }
}

fn actor_loop(
    actor_id: usize,
    socket_path: PathBuf,
    runtime_config: Arc<RwLock<ActorRuntimeConfig>>,
    shutdown: Arc<AtomicBool>,
    commands: mpsc::Receiver<ActorCommand>,
    results: mpsc::SyncSender<ActorResult>,
) {
    let mut client: Option<(u64, Duration, StorageClient)> = None;
    while let Ok(command) = commands.recv() {
        match command {
            ActorCommand::Shutdown => break,
            ActorCommand::Execute(item) => {
                let item_id = item.id;
                let volume_id = item.volume_id();
                let config = runtime_config
                    .read()
                    .expect("maintenance actor config lock poisoned")
                    .clone();
                if client.as_ref().is_some_and(|(current_volume, timeout, _)| {
                    *current_volume != volume_id || *timeout != config.request_timeout
                }) {
                    client = None;
                }
                if client.is_none() && !shutdown.load(Ordering::Acquire) {
                    match StorageClient::connect_managed_with_timeout(
                        &socket_path,
                        volume_id,
                        config.request_timeout,
                    ) {
                        Ok(connected) => {
                            client =
                                Some((volume_id, config.request_timeout, connected));
                        }
                        Err(error) => {
                            let _ = results.send(ActorResult {
                                actor_id,
                                item_id,
                                outcome: ObjectCleanupExecutionOutcome::Retryable(
                                    ObjectCleanupExecutionError::new(error),
                                ),
                            });
                            continue;
                        }
                    }
                }

                let outcome = match client.as_ref() {
                    Some((_, _, client)) => {
                        let executor = ObjectCleanupExecutor::new(config.page_size);
                        match panic::catch_unwind(AssertUnwindSafe(|| {
                            executor.execute(client, item.as_ref(), &shutdown)
                        })) {
                            Ok(outcome) => outcome,
                            Err(payload) => {
                                let message = panic_payload_message(payload.as_ref());
                                ObjectCleanupExecutionOutcome::Permanent(
                                    ObjectCleanupExecutionError::new(
                                        StorageError::backend(format!(
                                            "maintenance actor panicked while processing item {item_id}: {message}",
                                        )),
                                    ),
                                )
                            }
                        }
                    }
                    None => ObjectCleanupExecutionOutcome::Cancelled,
                };
                if matches!(
                    &outcome,
                    ObjectCleanupExecutionOutcome::Retryable(_)
                        | ObjectCleanupExecutionOutcome::Permanent(_)
                ) {
                    client = None;
                }
                if results
                    .send(ActorResult {
                        actor_id,
                        item_id,
                        outcome,
                    })
                    .is_err()
                {
                    break;
                }
            }
        }
    }
}

fn panic_payload_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}
