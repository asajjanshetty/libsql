#![allow(dead_code)]

use std::{collections::HashMap, sync::Arc};

use crate::{connection::config::DatabaseConfig, Result};
use parking_lot::Mutex;
use tokio::sync::{
    mpsc,
    watch::{self, Receiver, Sender},
};

use super::{MakeNamespace, Namespace, NamespaceName};

static INTERNAL_NAMESPACE: &str = "_libsql_server_internal";

type ChangeMsg = (NamespaceName, Arc<DatabaseConfig>);

#[derive(Debug)]
pub struct MetaStore<T> {
    changes_tx: mpsc::Sender<ChangeMsg>,
    inner: Mutex<MetaStoreInner<T>>,
}

#[derive(Clone, Debug)]
pub struct MetaStoreHandle {
    namespace: NamespaceName,
    inner: HandleState,
}

#[derive(Debug, Clone)]
enum HandleState {
    Internal(Arc<Mutex<Arc<DatabaseConfig>>>),
    External(mpsc::Sender<ChangeMsg>, Receiver<Arc<DatabaseConfig>>),
}

#[derive(Debug)]
struct MetaStoreInner<T> {
    changes_rx: mpsc::Receiver<ChangeMsg>,
    configs: HashMap<NamespaceName, Sender<Arc<DatabaseConfig>>>,
    store: Namespace<T>,
}

impl<T> MetaStore<T> {
    pub async fn new<M>(make_namespace: &M) -> Result<Self>
    where
        M: MakeNamespace<Database = T>,
    {
        let store = make_namespace
            .create_internal(NamespaceName(INTERNAL_NAMESPACE.into()))
            .await?;

        let configs = HashMap::new();
        let (changes_tx, changes_rx) = mpsc::channel(256);

        Ok(Self {
            changes_tx,
            inner: Mutex::new(MetaStoreInner {
                store,
                configs,
                changes_rx,
            }),
        })
    }

    pub fn handle(&self, namespace: NamespaceName) -> MetaStoreHandle {
        let change_tx = self.changes_tx.clone();

        let mut lock = self.inner.lock();
        let sender = lock.configs.entry(namespace.clone()).or_insert_with(|| {
            let (tx, _) = watch::channel(Arc::new(DatabaseConfig::default()));
            tx
        });

        MetaStoreHandle {
            namespace,
            inner: HandleState::External(change_tx, sender.subscribe()),
        }
    }
}

impl MetaStoreHandle {
    #[cfg(test)]
    pub fn new_test() -> Self {
        Self::internal()
    }

    #[cfg(test)]
    pub fn load(db_path: impl AsRef<std::path::Path>) -> crate::Result<Self> {
        use std::{fs, io};

        use crate::error::Error;

        let config_path = db_path.as_ref().join("config.json");

        let config = match fs::read(&config_path) {
            Ok(data) => serde_json::from_slice(&data)?,
            Err(err) if err.kind() == io::ErrorKind::NotFound => DatabaseConfig::default(),
            Err(err) => return Err(Error::IOError(err)),
        };

        Ok(Self {
            namespace: NamespaceName(INTERNAL_NAMESPACE.into()),
            inner: HandleState::Internal(Arc::new(Mutex::new(Arc::new(config)))),
        })
    }

    pub fn internal() -> Self {
        MetaStoreHandle {
            namespace: NamespaceName(INTERNAL_NAMESPACE.into()),
            inner: HandleState::Internal(Arc::new(Mutex::new(Arc::new(DatabaseConfig::default())))),
        }
    }

    pub fn get(&self) -> Arc<DatabaseConfig> {
        match &self.inner {
            HandleState::Internal(config) => config.lock().clone(),
            HandleState::External(_, config) => config.borrow().clone(),
        }
    }

    pub async fn store(&self, new_config: impl Into<Arc<DatabaseConfig>>) -> Result<()> {
        match &self.inner {
            HandleState::Internal(config) => {
                *config.lock() = new_config.into();
            }
            HandleState::External(changes_tx, config) => {
                // TODO(lucio): Fix these unwraps, figure out the failure case
                changes_tx
                    .send((self.namespace.clone(), new_config.into()))
                    .await
                    .unwrap();

                config.clone().changed().await.unwrap();
            }
        };

        Ok(())
    }
}
