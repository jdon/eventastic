mod common;
mod encryption;
mod error;
mod pickle;
mod reader_impl;
mod repository;
mod side_effect;
mod table_config;
mod transaction;

pub use encryption::{EncryptionProvider, NoEncryption, NoEncryptionError};
pub use error::{DbError, SideEffectDbError};
pub use pickle::Pickle;
pub use repository::PostgresRepository;
pub use side_effect::SideEffectStorage;
pub use table_config::TableConfig;
pub use transaction::PostgresTransaction;

use crate::error::EventSourcingDbError;

use async_trait::async_trait;
use eventastic::{
    aggregate::{Aggregate, Context, SideEffect},
    event::DomainEvent,
    repository::{Repository, RepositoryError},
};
use sqlx::types::Uuid;

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
    O: SideEffectStorage<E::Error, T::SideEffect> + Send + Sync,
    E: EncryptionProvider + Clone + Send + Sync,
{
    /// Loads an aggregate from PostgreSQL storage by its UUID using an existing transaction.
    ///
    /// This method replays the event stream for the given aggregate ID,
    /// starting from any available snapshot and applying subsequent events.
    async fn load_with_transaction(
        transaction: &mut PostgresTransaction<'_, T, O, E>,
        aggregate_id: Uuid,
    ) -> Result<
        Context<T>,
        RepositoryError<
            T::ApplyError,
            <<T as Aggregate>::DomainEvent as DomainEvent>::EventId,
            EventSourcingDbError<E, T>,
        >,
    > {
        Context::load(transaction, &aggregate_id).await
    }

    /// Loads an aggregate from PostgreSQL storage by its UUID without a transaction.
    ///
    /// This method is more efficient for read-only operations as it uses a
    /// connection directly from the pool without starting a transaction.
    async fn load(
        repository: &PostgresRepository<T, O, E>,
        aggregate_id: Uuid,
    ) -> Result<
        Context<T>,
        RepositoryError<
            T::ApplyError,
            <<T as Aggregate>::DomainEvent as DomainEvent>::EventId,
            EventSourcingDbError<E, T>,
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
    O: SideEffectStorage<E::Error, T::SideEffect> + Send + Sync,
    E: EncryptionProvider + Clone + Send + Sync,
{
}
