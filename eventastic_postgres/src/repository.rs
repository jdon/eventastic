use std::marker::PhantomData;

use crate::{
    DbError, PostgresTransaction, SideEffectStorage, TableRegistry, encryption::EncryptionProvider,
    pickle::Pickle, reader_impl,
};
use async_trait::async_trait;
use eventastic::{
    aggregate::{Aggregate, Context, SideEffect},
    event::{DomainEvent, EventStoreEvent},
    repository::{Repository, RepositoryError, RepositoryReader, Snapshot},
};
use futures::StreamExt;
use sqlx::{
    Pool, Postgres,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::Uuid,
};

/// PostgreSQL-based repository implementation for event sourcing.
///
/// This repository provides persistent storage for aggregates, events, and snapshots
/// using PostgreSQL as the backing store. It integrates with a configurable side effect
/// storage mechanism for handling the outbox pattern.
#[derive(Clone)]
pub struct PostgresRepository<T, O, E>
where
    T: Clone,
    O: Clone,
    E: Clone,
{
    pub(crate) inner: Pool<Postgres>,
    pub(crate) outbox: O,
    pub(crate) tables: TableRegistry,
    encryption_provider: E,
    phantom_aggregate: std::marker::PhantomData<T>,
}

impl<T, O, E> PostgresRepository<T, O, E>
where
    T: Aggregate + Clone,
    T::SideEffect: SideEffect<SideEffectId = Uuid> + Pickle + Send + Sync,
    O: SideEffectStorage<E::Error, T::SideEffect> + Clone,
    E: EncryptionProvider + Clone,
{
    /// Creates a new PostgreSQL repository with the specified connection and pool options.
    ///
    /// # Parameters
    ///
    /// - `connect_options` - PostgreSQL connection configuration
    /// - `pool_options` - Connection pool configuration  
    /// - `outbox` - Side effect storage implementation for the outbox pattern
    /// - `tables` - Registry of table configurations for different aggregates
    pub async fn new(
        connect_options: PgConnectOptions,
        pool_options: PgPoolOptions,
        outbox: O,
        tables: TableRegistry,
        encryption_provider: E,
    ) -> Result<Self, sqlx::Error> {
        let pool = pool_options.connect_with(connect_options).await?;

        Ok(Self {
            inner: pool,
            outbox,
            tables,
            encryption_provider,
            phantom_aggregate: PhantomData,
        })
    }

    /// Start a new database transaction using the default isolation level.
    ///
    /// The returned transaction can be used to perform multiple operations
    /// atomically and provides access to the repository methods.
    pub async fn begin_transaction(&self) -> Result<PostgresTransaction<'_, T, O, E>, sqlx::Error> {
        Ok(PostgresTransaction {
            inner: self.inner.begin().await?,
            outbox: &self.outbox,
            tables: &self.tables,
            encryption_provider: &self.encryption_provider,
            phantom_aggregate: PhantomData,
        })
    }

    /// Run database migrations to set up the required tables and schema.
    ///
    /// This method should be called once during application startup to ensure
    /// the database schema is up to date with the required tables for events,
    /// snapshots, and outbox storage.
    pub async fn run_migrations(&self) -> Result<(), sqlx::Error> {
        sqlx::migrate!("./migrations").run(&self.inner).await?;

        Ok(())
    }
}

#[async_trait]
impl<T, O, E> RepositoryReader<T> for PostgresRepository<T, O, E>
where
    T: Aggregate<AggregateId = Uuid> + Pickle + Send + Sync + 'static,
    T::DomainEvent: DomainEvent<EventId = Uuid> + Pickle + Send + Sync,
    T::SideEffect: SideEffect<SideEffectId = Uuid> + Pickle + Send + Sync,
    T::ApplyError: Send + Sync,
    O: SideEffectStorage<E::Error, T::SideEffect> + Clone + Send + Sync,
    E: EncryptionProvider + Clone + Send + Sync,
{
    type DbError = DbError<E::Error>;

    /// Returns a stream of domain events.
    fn stream_from(
        &mut self,
        id: &T::AggregateId,
        version: u64,
    ) -> impl futures::Stream<
        Item = std::result::Result<
            eventastic::event::EventStoreEvent<
                <T as eventastic::aggregate::Aggregate>::DomainEvent,
            >,
            Self::DbError,
        >,
    > {
        let query = match self.tables.stream_events_query::<T>() {
            Some(query) => query,
            None => {
                return futures::stream::iter(vec![Err(DbError::UnregisteredAggregate)]).boxed();
            }
        };
        Box::pin(reader_impl::stream_from::<_, T, E>(
            &self.inner,
            id,
            version,
            query,
            &self.encryption_provider,
        ))
    }

    /// Returns a specific domain event from the database.
    async fn get_event(
        &mut self,
        aggregate_id: &T::AggregateId,
        event_id: &<<T as Aggregate>::DomainEvent as DomainEvent>::EventId,
    ) -> Result<Option<EventStoreEvent<<T as Aggregate>::DomainEvent>>, Self::DbError> {
        let query = self
            .tables
            .get_event_query::<T>()
            .ok_or(DbError::UnregisteredAggregate)?;
        reader_impl::get_event::<_, T, E>(
            &self.inner,
            aggregate_id,
            event_id,
            query,
            &self.encryption_provider,
        )
        .await
    }

    /// Returns a snapshot of the aggregate in the database
    async fn get_snapshot(
        &mut self,
        id: &T::AggregateId,
    ) -> Result<Option<Snapshot<T>>, Self::DbError> {
        let query = self
            .tables
            .get_snapshot_query::<T>()
            .ok_or(DbError::UnregisteredAggregate)?;
        reader_impl::get_snapshot::<_, T, E>(&self.inner, id, query, &self.encryption_provider)
            .await
    }
}

#[async_trait]
impl<T, O, E> Repository<T> for PostgresRepository<T, O, E>
where
    T: Aggregate<AggregateId = Uuid> + Pickle + Send + Sync + 'static,
    T::DomainEvent: DomainEvent<EventId = Uuid> + Pickle + Send + Sync,
    T::SideEffect: eventastic::aggregate::SideEffect<SideEffectId = Uuid> + Pickle + Send + Sync,
    T::ApplyError: Send + Sync,
    O: SideEffectStorage<E::Error, T::SideEffect> + Clone + Send + Sync,
    E: EncryptionProvider + Clone + Send + Sync,
{
    type Error = RepositoryError<
        T::ApplyError,
        <<T as Aggregate>::DomainEvent as DomainEvent>::EventId,
        DbError<E::Error>,
    >;

    /// Loads an aggregate from the repository by its ID.
    ///
    /// This method performs a non-transactional read directly from the pool,
    /// avoiding the overhead of starting a transaction. It will load the
    /// latest state of the aggregate by replaying its event stream.
    /// If a snapshot is available, it will be used to optimize the loading process.
    async fn load(&self, aggregate_id: &T::AggregateId) -> Result<Context<T>, Self::Error> {
        // Create a mutable reference to self to satisfy the RepositoryReader trait
        let mut repo_ref = self.clone();
        Context::load(&mut repo_ref, aggregate_id).await
    }
}
