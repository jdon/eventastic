use std::fmt::Debug;

use crate::{DbError, SideEffectStorage};
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use eventastic::aggregate::Aggregate;
use eventastic::aggregate::SideEffect;
use eventastic::event::DomainEvent;
use eventastic::event::EventStoreEvent;
use eventastic::repository::RepositoryTransaction;
use eventastic::repository::Snapshot;
use futures::stream;
use futures_util::stream::StreamExt;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::Row;
use sqlx::query;
use sqlx::query_as;
use sqlx::types::JsonValue;
use sqlx::types::Uuid;
use sqlx::{Postgres, Transaction};
pub struct PostgresTransaction<'a, O>
where
    O: SideEffectStorage,
{
    pub(crate) inner: Transaction<'a, Postgres>,
    pub(crate) outbox: &'a O,
}

impl<'a, O> PostgresTransaction<'a, O>
where
    O: SideEffectStorage,
{
    /// Commit the transaction to the db.
    pub async fn commit(self) -> Result<(), DbError> {
        Ok(self.inner.commit().await?)
    }

    /// Rollback the transaction
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
}

#[derive(sqlx::FromRow)]
struct PartialSnapShotRow {
    aggregate: serde_json::Value,
    snapshot_version: i64,
    version: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct PartialEventRow {
    event_id: Uuid,
    version: i64,
    event: JsonValue,
}

impl PartialEventRow {
    fn to_event<Evt>(
        row: PartialEventRow,
    ) -> Result<eventastic::event::EventStoreEvent<Evt>, DbError>
    where
        Evt: DomainEvent<EventId = Uuid> + DeserializeOwned,
    {
        let row_version = u64::try_from(row.version).map_err(|_| DbError::InvalidVersionNumber)?;

        serde_json::from_value::<Evt>(row.event)
            .map(|e| EventStoreEvent {
                id: row.event_id,
                event: e,
                version: row_version,
            })
            .map_err(DbError::SerializationError)
    }
}

#[async_trait]
impl<'a, O, T> RepositoryTransaction<T> for PostgresTransaction<'a, O>
where
    T: Aggregate<AggregateId = Uuid> + 'a + DeserializeOwned + Serialize + Send + Sync,
    T::SideEffect: SideEffect<SideEffectId = Uuid> + Serialize + Send + Sync,
    T::DomainEvent: DomainEvent<EventId = Uuid> + Serialize + DeserializeOwned + Send + Sync,
    O: SideEffectStorage,
{
    /// The type of error that is returned from the database.
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
            <Self as eventastic::repository::RepositoryTransaction<T>>::DbError,
        >,
    > {
        let Ok(version) = i64::try_from(version) else {
            return stream::iter(vec![Err(DbError::InvalidVersionNumber)]).boxed();
        };

        let res = query_as::<_, PartialEventRow>(
            "
                SELECT event, event_id, version
                FROM events 
                where aggregate_id = $1 AND version >= $2 ORDER BY version ASC",
        )
        .bind(*id)
        .bind(version)
        .fetch(&mut *self.inner);

        res.map(|row| match row {
            Ok(row) => PartialEventRow::to_event(row),
            Err(e) => Err(DbError::DbError(e)),
        })
        .boxed()
    }

    /// Returns a specific domain event from the database.
    async fn get_event(
        &mut self,
        aggregate_id: &T::AggregateId,
        event_id: &<<T as Aggregate>::DomainEvent as DomainEvent>::EventId,
    ) -> Result<Option<EventStoreEvent<<T as Aggregate>::DomainEvent>>, Self::DbError> {
        query_as::<_, PartialEventRow>(
            "SELECT event, event_id, version FROM events where aggregate_id = $1 AND event_id = $2",
        )
        .bind(aggregate_id)
        .bind(event_id)
        .fetch_optional(&mut *self.inner)
        .await?
        .map(PartialEventRow::to_event)
        .transpose()
    }

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
        let mut events_to_insert: Vec<serde_json::Value> = Vec::with_capacity(events.len());
        let mut created_ats_to_insert: Vec<DateTime<Utc>> = Vec::with_capacity(events.len());

        for event in events {
            let event_id = *event.id();
            let version = event.version;

            let version = i64::try_from(version).map_err(|_| DbError::InvalidVersionNumber)?;

            let serialised_event =
                serde_json::to_value(event.event).map_err(DbError::SerializationError)?;

            event_ids_to_insert.push(event_id);
            versions_to_insert.push(version);
            aggregate_ids_to_insert.push(*id);
            events_to_insert.push(serialised_event);
            created_ats_to_insert.push(Utc::now());
        }

        let inserted_ids:Result<Vec<Uuid>, sqlx::Error> = sqlx::query(
            "INSERT INTO events(event_id, version, aggregate_id, event, created_at) 
            SELECT * FROM UNNEST($1::uuid[], $2::bigint[], $3::uuid[], $4::jsonb[], $5::timestamptz[])
            ON CONFLICT DO NOTHING returning event_id",
        ).bind(&event_ids_to_insert[..]).bind(&versions_to_insert[..]).bind(&aggregate_ids_to_insert[..]).bind(&events_to_insert[..]).bind(&created_ats_to_insert[..])
        .fetch_all(&mut *self.inner).await?.into_iter().map(|row|row.try_get(0)).collect();

        Ok(inserted_ids?)
    }

    /// Returns a snapshot of the aggregate in the database
    async fn get_snapshot(
        &mut self,
        id: &T::AggregateId,
    ) -> Result<Option<Snapshot<T>>, Self::DbError> {
        let row = query_as::<_, PartialSnapShotRow>(
            "SELECT aggregate, version, snapshot_version from snapshots where aggregate_id = $1 AND snapshot_version = $2",
        )
        .bind(id)
        .bind(i64::try_from(T::SNAPSHOT_VERSION).map_err(|_| DbError::InvalidSnapshotVersion)?)
        .fetch_optional(&mut *self.inner)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let version = u64::try_from(row.version).map_err(|_| DbError::InvalidVersionNumber)?;
        let snapshot_version =
            u64::try_from(row.snapshot_version).map_err(|_| DbError::InvalidSnapshotVersion)?;
        let aggregate: T =
            serde_json::from_value(row.aggregate).map_err(DbError::SerializationError)?;

        Ok(Some(Snapshot {
            aggregate,
            version,
            snapshot_version,
        }))
    }

    /// Stores a snapshot of the aggregate in the database
    async fn store_snapshot(&mut self, snapshot: Snapshot<T>) -> Result<(), Self::DbError> {
        let aggregated_id = *snapshot.aggregate.aggregate_id();
        let aggregate =
            serde_json::to_value(snapshot.aggregate).map_err(DbError::SerializationError)?;
        query("INSERT INTO snapshots(aggregate_id, aggregate, version, snapshot_version, created_at) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (aggregate_id) DO UPDATE SET aggregate = $2, version = $3, snapshot_version = $4, created_at = $5")
            .bind(aggregated_id)
            .bind(aggregate)
            .bind(i64::try_from(snapshot.version).map_err(|_| DbError::InvalidVersionNumber)?)
            .bind(i64::try_from(snapshot.snapshot_version).map_err(|_| DbError::InvalidSnapshotVersion)?)
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
