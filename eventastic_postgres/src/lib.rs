mod repository;
mod transaction;
use async_trait::async_trait;
use eventastic::{
    aggregate::{Aggregate, Context, SideEffect},
    repository::RepositoryError,
};
use sqlx::{Postgres, Transaction};
pub use repository::PostgresRepository;
pub use transaction::PostgresTransaction;
use serde::{Serialize, de::DeserializeOwned};
use sqlx::types::Uuid;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("DB Error {0}")]
    DbError(sqlx::Error),
    #[error("Serialization Error {0}")]
    SerializationError(#[from] serde_json::Error),
    #[error("Invalid Version Number")]
    InvalidVersionNumber,
    #[error("Invalid Snapshot Version number")]
    InvalidSnapshotVersion,
    #[error("Optimistic Concurrency Error")]
    OptimisticConcurrencyError,
}

impl From<sqlx::Error> for DbError {
    fn from(e: sqlx::Error) -> Self {
        if let Some(db_error) = e.as_database_error() {
            if let Some(code) = db_error.code() {
                if code == "23505" && db_error.message().contains("aggregate_version") {
                    return DbError::OptimisticConcurrencyError;
                }
            }
        }
        DbError::DbError(e)
    }
}

#[async_trait]
pub trait TransactionalOutbox: Send + Sync {
    async fn store_side_effects(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        items: Vec<(Uuid, serde_json::Value)>,
    ) -> Result<(), DbError>;
}

#[async_trait]
pub trait RootExt<S, T, O>
where
    S: SideEffect<Id = Uuid> + Serialize + Send + Sync + 'static,
    T: Aggregate<AggregateId = Uuid, DomainEventId = Uuid, SideEffect = S>
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
    <T as Aggregate>::DomainEvent: Serialize + DeserializeOwned + Send + Sync,
    O: TransactionalOutbox + Send + Sync,
{
    async fn load(
        transaction: &mut PostgresTransaction<'_, O>,
        aggregate_id: Uuid,
    ) -> Result<Context<T>, RepositoryError<T::ApplyError, T::DomainEventId, DbError>> {
        Context::load(transaction, &aggregate_id).await
    }
}

impl<S, T, O> RootExt<S, T, O> for T
where
    S: SideEffect<Id = Uuid> + Serialize + Send + Sync + 'static,
    T: Aggregate<AggregateId = Uuid, DomainEventId = Uuid, SideEffect = S>
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
    <T as Aggregate>::DomainEvent: Serialize + DeserializeOwned + Send + Sync,
    O: TransactionalOutbox + Send + Sync,
{
}
