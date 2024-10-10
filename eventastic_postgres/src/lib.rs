mod outbox;
mod repository;
mod transaction;
use async_trait::async_trait;
use eventastic::{
    aggregate::{Aggregate, Context, SideEffect},
    repository::RepositoryError,
};
pub use repository::PostgresRepository;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::types::Uuid;
use thiserror::Error;
pub use transaction::PostgresTransaction;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("DB Error {0}")]
    DbError(sqlx::Error),
    #[error("Serialization Error {0}")]
    SerializationError(#[from] serde_json::Error),
    #[error("Invalid Version Number")]
    InvalidVersionNumber,
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
pub trait RootExt<
    S: SideEffect<Id = Uuid> + Send + Sync + 'static,
    T: Aggregate<AggregateId = Uuid, DomainEventId = Uuid, SideEffect = S>
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
> where
    <T as Aggregate>::DomainEvent: Serialize + DeserializeOwned + Send + Sync,
{
    async fn load(
        transaction: &mut PostgresTransaction<'_>,
        aggregate_id: Uuid,
    ) -> Result<Context<T>, RepositoryError<T::ApplyError, T::DomainEventId, DbError>> {
        Context::load(transaction, &aggregate_id.clone()).await
    }
}

impl<
        S: SideEffect<Id = Uuid> + Send + Sync + 'static,
        T: Aggregate<AggregateId = Uuid, DomainEventId = Uuid, SideEffect = S>
            + Serialize
            + DeserializeOwned
            + Send
            + Sync
            + 'static,
    > RootExt<S, T> for T
where
    <T as Aggregate>::DomainEvent: Serialize + DeserializeOwned + Send + Sync,
{
}
