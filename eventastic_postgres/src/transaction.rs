use crate::common::utils;
use crate::pickle::Pickle;
use crate::{DbError, EncryptionProvider, SideEffectStorage, TableRegistry, reader_impl};
use anyhow::Context as _;
use anyhow::anyhow;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use eventastic::aggregate::SaveError;
use eventastic::aggregate::SideEffect;
use eventastic::aggregate::{Aggregate, Context};
use eventastic::event::DomainEvent;
use eventastic::event::EventStoreEvent;
use eventastic::repository::Snapshot;
use eventastic::repository::{RepositoryError, RepositoryReader, RepositoryWriter};
use futures::StreamExt;
use sqlx::Row;
use sqlx::types::Uuid;
use sqlx::{Postgres, Transaction};

/// PostgreSQL transaction wrapper that implements the [`RepositoryWriter`] and [`RepositoryReader`] traits.
///
/// This struct provides transactional access to PostgreSQL storage for event sourcing
/// operations. It manages database transactions and integrates with side effect storage.
pub struct PostgresTransaction<'a, O, E>
where
    O: SideEffectStorage,
{
    pub(crate) inner: Transaction<'a, Postgres>,
    pub(crate) outbox: &'a O,
    pub(crate) tables: &'a TableRegistry,
    pub(crate) encryption_provider: &'a E,
}

impl<'a, O, E> PostgresTransaction<'a, O, E>
where
    O: SideEffectStorage,
    E: EncryptionProvider + Send + Sync + 'static,
{
    /// Commit the transaction to the database.
    ///
    /// This finalizes all operations performed within this transaction,
    /// making them permanently visible to other database connections.
    pub async fn commit(self) -> Result<(), DbError> {
        Ok(self.inner.commit().await?)
    }

    /// Rollback the transaction, discarding all changes.
    ///
    /// This undoes all operations performed within this transaction,
    /// returning the database to its state before the transaction began.
    pub async fn rollback(self) -> Result<(), DbError> {
        Ok(self.inner.rollback().await?)
    }

    /// Get the inner postgres transaction
    pub fn into_inner(self) -> Transaction<'a, Postgres> {
        self.inner
    }

    /// Returns a mutable reference to the underlying [`sqlx::Transaction`].
    pub fn inner_mut(&mut self) -> &mut Transaction<'a, Postgres> {
        &mut self.inner
    }

    /// Get an aggregate by ID using the table registry.
    pub async fn get<T>(
        &mut self,
        id: &Uuid,
    ) -> Result<Context<T>, RepositoryError<T::ApplyError, Uuid, DbError>>
    where
        T: Aggregate<AggregateId = Uuid> + 'static + Send + Sync + Pickle,
        T::DomainEvent: DomainEvent<EventId = Uuid> + Pickle + Send + Sync,
        T::SideEffect: SideEffect<SideEffectId = Uuid> + Pickle + Send + Sync,
        T::ApplyError: Send + Sync,
    {
        Context::load(self, id).await
    }

    /// Store an aggregate using the table registry.
    pub async fn store<T>(
        &mut self,
        aggregate: &mut Context<T>,
    ) -> Result<(), SaveError<T, DbError>>
    where
        T: Aggregate<AggregateId = Uuid> + 'static + Send + Sync + Pickle,
        T::DomainEvent: DomainEvent<EventId = Uuid> + Pickle + Send + Sync,
        T::SideEffect: SideEffect<SideEffectId = Uuid> + Pickle + Send + Sync,
        T::ApplyError: Send + Sync,
    {
        aggregate.save(self).await
    }

    pub fn encryption_provider(&self) -> &E {
        &self.encryption_provider
    }
}

#[async_trait]
impl<O, T, E> RepositoryReader<T> for PostgresTransaction<'_, O, E>
where
    T: Aggregate<AggregateId = Uuid> + 'static + Pickle + Send + Sync,
    T::SideEffect: SideEffect<SideEffectId = Uuid> + Pickle + Send + Sync,
    T::DomainEvent: DomainEvent<EventId = Uuid> + Pickle + Send + Sync,
    T::ApplyError: Send + Sync,
    O: SideEffectStorage,
    E: EncryptionProvider + Send + Sync,
{
    type DbError = DbError;

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
            &mut *self.inner,
            id,
            version,
            query,
            self.encryption_provider,
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
            &mut *self.inner,
            aggregate_id,
            event_id,
            query,
            self.encryption_provider,
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
        reader_impl::get_snapshot::<_, T, E>(&mut *self.inner, id, query, self.encryption_provider)
            .await
    }
}

#[async_trait]
impl<O, T, E> RepositoryWriter<T> for PostgresTransaction<'_, O, E>
where
    T: Aggregate<AggregateId = Uuid> + 'static + Pickle + Send + Sync,
    T::SideEffect: SideEffect<SideEffectId = Uuid> + Pickle + Send + Sync,
    T::DomainEvent: DomainEvent<EventId = Uuid> + Pickle + Send + Sync,
    T::ApplyError: Send + Sync,
    O: SideEffectStorage,
    E: EncryptionProvider + Send + Sync,
{
    /// Stores new domain events to the database
    async fn store_events(
        &mut self,
        id: &T::AggregateId,
        events: Vec<EventStoreEvent<T::DomainEvent>>,
    ) -> Result<Vec<<<T as Aggregate>::DomainEvent as DomainEvent>::EventId>, Self::DbError> {
        let mut event_ids_to_insert: Vec<<<T as Aggregate>::DomainEvent as DomainEvent>::EventId> =
            Vec::with_capacity(events.len());
        let mut versions_to_insert: Vec<i64> = Vec::with_capacity(events.len());
        let mut aggregate_ids_to_insert: Vec<T::AggregateId> = Vec::with_capacity(events.len());
        let mut events_to_insert: Vec<Vec<u8>> = Vec::with_capacity(events.len());
        let mut created_ats_to_insert: Vec<DateTime<Utc>> = Vec::with_capacity(events.len());

        for events in events.chunks(self.encryption_provider.max_batch_size()) {
            let mut plain = Vec::with_capacity(events.len());
            for event in events {
                let event_id = *event.id();
                let version = event.version;

                let version = utils::version_to_i64(version)?;

                let serialised_event = event
                    .event
                    .pickle()
                    .context("Failed to pickle event")
                    .map_err(DbError::PicklingError)?;

                event_ids_to_insert.push(event_id);
                versions_to_insert.push(version);
                aggregate_ids_to_insert.push(*id);
                plain.push(serialised_event);
                created_ats_to_insert.push(Utc::now());
            }
            let number_of_items = plain.len();
            let mut cipher = self
                .encryption_provider
                .encrypt(plain)
                .await
                .context("Failed to encrypt events")
                .map_err(DbError::Encryption)?;
            if cipher.len() != number_of_items {
                return Err(DbError::Encryption(anyhow!(
                    "Encrypting events returned wrong number of items"
                )));
            }
            events_to_insert.append(&mut cipher);
        }

        let insert_query = self
            .tables
            .insert_events_query::<T>()
            .ok_or(DbError::UnregisteredAggregate)?;

        let inserted_ids: Result<Vec<Uuid>, sqlx::Error> = sqlx::query(insert_query)
            .bind(&event_ids_to_insert[..])
            .bind(&versions_to_insert[..])
            .bind(&aggregate_ids_to_insert[..])
            .bind(&events_to_insert[..])
            .bind(&created_ats_to_insert[..])
            .fetch_all(&mut *self.inner)
            .await?
            .into_iter()
            .map(|row| row.try_get(0))
            .collect();

        Ok(inserted_ids?)
    }

    /// Stores a snapshot of the aggregate in the database
    async fn store_snapshot(&mut self, snapshot: Snapshot<T>) -> Result<(), Self::DbError> {
        let aggregated_id = *snapshot.aggregate.aggregate_id();
        let aggregate = snapshot
            .aggregate
            .pickle()
            .context("Failed to pickle aggregate")
            .map_err(DbError::PicklingError)?;
        let cipher = self
            .encryption_provider
            .encrypt(vec![aggregate])
            .await
            .context("Failed to encrypt snapshot")
            .map_err(DbError::Encryption)?;
        if cipher.len() != 1 {
            return Err(DbError::Encryption(anyhow!(
                "Encrypting snapshot returned wrong number of items"
            )));
        }
        let aggregate = cipher
            .into_iter()
            .next()
            .expect("Must have encrypted snapshot");

        let upsert_query = self
            .tables
            .upsert_snapshot_query::<T>()
            .ok_or(DbError::UnregisteredAggregate)?;

        sqlx::query(upsert_query)
            .bind(aggregated_id)
            .bind(aggregate)
            .bind(utils::version_to_i64(snapshot.version)?)
            .bind(utils::snapshot_version_to_i64(snapshot.snapshot_version)?)
            .bind(Utc::now())
            .execute(&mut *self.inner)
            .await?;

        Ok(())
    }

    /// Stores side effects into the database
    #[doc(hidden)]
    async fn store_side_effects(
        &mut self,
        outbox_item: Vec<T::SideEffect>,
    ) -> Result<(), Self::DbError> {
        self.outbox
            .store_side_effects(&mut self.inner, outbox_item)
            .await
    }
}
