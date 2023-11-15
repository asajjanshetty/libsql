use std::sync::Arc;

use crate::connection::libsql::LibSqlConnection;
use crate::connection::write_proxy::{RpcStream, WriteProxyConnection};
use crate::connection::{Connection, MakeConnection, TrackedConnection};
use crate::namespace::replication_wal::ReplicationWal;
use crate::replication::ReplicationLogger;

pub type PrimaryConnection = TrackedConnection<LibSqlConnection<ReplicationWal>>;

pub trait Database: Sync + Send + 'static {
    /// The connection type of the database
    type Connection: Connection;

    fn connection_maker(&self) -> Arc<dyn MakeConnection<Connection = Self::Connection>>;
    fn shutdown(&self);
}

pub struct ReplicaDatabase {
    pub connection_maker:
        Arc<dyn MakeConnection<Connection = TrackedConnection<WriteProxyConnection<RpcStream>>>>,
}

impl Database for ReplicaDatabase {
    type Connection = TrackedConnection<WriteProxyConnection<RpcStream>>;

    fn connection_maker(&self) -> Arc<dyn MakeConnection<Connection = Self::Connection>> {
        self.connection_maker.clone()
    }

    fn shutdown(&self) {}
}

pub struct PrimaryDatabase {
    pub logger: Arc<ReplicationLogger>,
    pub connection_maker: Arc<dyn MakeConnection<Connection = PrimaryConnection>>,
}

impl Database for PrimaryDatabase {
    type Connection = PrimaryConnection;

    fn connection_maker(&self) -> Arc<dyn MakeConnection<Connection = Self::Connection>> {
        self.connection_maker.clone()
    }

    fn shutdown(&self) {
        self.logger.closed_signal.send_replace(true);
    }
}
