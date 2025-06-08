mod repository;
mod side_effect;
mod transaction;
use async_trait::async_trait;
use eventastic::{
    aggregate::{Aggregate, Context, SideEffect},
    event::DomainEvent,
    repository::RepositoryError,
};
pub use repository::PostgresRepository;
use serde::{Serialize, de::DeserializeOwned};
pub use side_effect::SideEffectStorage;
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
pub trait RootExt<T, O>
where
    T: Aggregate<AggregateId = Uuid> + Serialize + DeserializeOwned + Send + Sync + 'static,
    <T as Aggregate>::DomainEvent:
        DomainEvent<EventId = Uuid> + Serialize + DeserializeOwned + Send + Sync,
    <T as Aggregate>::SideEffect: SideEffect<SideEffectId = Uuid> + Serialize + Send + Sync,
    O: SideEffectStorage + Send + Sync,
{
    async fn load(
        transaction: &mut PostgresTransaction<'_, O>,
        aggregate_id: Uuid,
    ) -> Result<
        Context<T>,
        RepositoryError<
            T::ApplyError,
            <<T as Aggregate>::DomainEvent as DomainEvent>::EventId,
            DbError,
        >,
    > {
        Context::load(transaction, &aggregate_id).await
    }
}

impl<T, O> RootExt<T, O> for T
where
    T: Aggregate<AggregateId = Uuid> + Serialize + DeserializeOwned + Send + Sync + 'static,
    <T as Aggregate>::DomainEvent:
        DomainEvent<EventId = Uuid> + Serialize + DeserializeOwned + Send + Sync,
    <T as Aggregate>::SideEffect: SideEffect<SideEffectId = Uuid> + Serialize + Send + Sync,
    O: SideEffectStorage + Send + Sync,
{
}
