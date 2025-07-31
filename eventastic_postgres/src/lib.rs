mod common;
mod encryption;
mod pickle;
mod reader_impl;
mod repository;
mod side_effect;
mod table_registry;
mod transaction;

pub use encryption::{EncryptionProvider, NoEncryption, NoEncryptionError};
pub use pickle::Pickle;
pub use repository::PostgresRepository;
pub use side_effect::SideEffectStorage;
pub use table_registry::{TableConfig, TableRegistry, TableRegistryBuilder};
pub use transaction::PostgresTransaction;

use async_trait::async_trait;
use eventastic::{
    aggregate::{Aggregate, Context, SideEffect},
    event::DomainEvent,
    repository::{Repository, RepositoryError},
};
use sqlx::types::Uuid;
use thiserror::Error;

/// Errors that can occur during PostgreSQL operations.
#[derive(Error, Debug)]
pub enum DbError<E> {
    /// A database operation failed.
    #[error("DB Error {0}")]
    DbError(sqlx::Error),
    /// Failed to pickle data.
    #[error("Pickling Error {0}")]
    PicklingError(anyhow::Error),
    /// An invalid version number was encountered (e.g., negative value where positive expected).
    #[error("Invalid Version Number")]
    InvalidVersionNumber,
    /// An invalid snapshot version number was encountered.
    #[error("Invalid Snapshot Version Number")]
    InvalidSnapshotVersion,
    /// A concurrent modification was detected (optimistic locking failure).
    #[error("Optimistic Concurrency Error")]
    OptimisticConcurrencyError,
    /// An aggregate type was not registered in the table registry.
    #[error("Aggregate type not registered in table registry")]
    UnregisteredAggregate,
    /// Failed to encrypt or decrypt data.
    #[error("Encryption Error {0}")]
    Encryption(E),
    /// Failed to encrypt or decrypt data.
    #[error("Encryption provider returned wrong number of items")]
    EncrypytionProviderReturnedWrongNumberOfItems,
}

impl<E> From<sqlx::Error> for DbError<E> {
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

/// Extension trait for loading aggregates from PostgreSQL storage.
///
/// This trait provides PostgreSQL-specific methods for working with aggregates
/// that have UUID-based identifiers and can be serialized to JSON.
#[async_trait]
pub trait RootExt<T, O, E>
where
    T: Aggregate<AggregateId = Uuid> + Pickle + Send + Sync + 'static,
    <T as Aggregate>::DomainEvent: DomainEvent<EventId = Uuid> + Pickle + Send + Sync,
    <T as Aggregate>::SideEffect: SideEffect<SideEffectId = Uuid> + Pickle + Send + Sync,
    <T as Aggregate>::ApplyError: Send + Sync,
    O: SideEffectStorage<E::Error> + Send + Sync,
    E: EncryptionProvider + Clone + Send + Sync,
{
    /// Loads an aggregate from PostgreSQL storage by its UUID using an existing transaction.
    ///
    /// This method replays the event stream for the given aggregate ID,
    /// starting from any available snapshot and applying subsequent events.
    async fn load_with_transaction(
        transaction: &mut PostgresTransaction<'_, O, E>,
        aggregate_id: Uuid,
    ) -> Result<
        Context<T>,
        RepositoryError<
            T::ApplyError,
            <<T as Aggregate>::DomainEvent as DomainEvent>::EventId,
            DbError<E::Error>,
        >,
    > {
        Context::load(transaction, &aggregate_id).await
    }

    /// Loads an aggregate from PostgreSQL storage by its UUID without a transaction.
    ///
    /// This method is more efficient for read-only operations as it uses a
    /// connection directly from the pool without starting a transaction.
    async fn load(
        repository: &PostgresRepository<O, E>,
        aggregate_id: Uuid,
    ) -> Result<
        Context<T>,
        RepositoryError<
            T::ApplyError,
            <<T as Aggregate>::DomainEvent as DomainEvent>::EventId,
            DbError<E::Error>,
        >,
    >
    where
        O: Clone,
    {
        repository.load(&aggregate_id).await
    }
}

impl<T, O, E> RootExt<T, O, E> for T
where
    T: Aggregate<AggregateId = Uuid> + Pickle + Send + Sync + 'static,
    <T as Aggregate>::DomainEvent: DomainEvent<EventId = Uuid> + Pickle + Send + Sync,
    <T as Aggregate>::SideEffect: SideEffect<SideEffectId = Uuid> + Pickle + Send + Sync,
    <T as Aggregate>::ApplyError: Send + Sync,
    O: SideEffectStorage<E::Error> + Send + Sync,
    E: EncryptionProvider + Clone + Send + Sync,
{
}
