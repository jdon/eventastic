use std::fmt::Debug;

use crate::outbox::OutBoxMessage;
use crate::DbError;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use eventastic::aggregate::Aggregate;
use eventastic::aggregate::Context;
use eventastic::aggregate::SideEffect;
use eventastic::event::Event;
use eventastic::event::EventStoreEvent;
use eventastic::event::Stream;
use eventastic::repository::RepositoryError;
use eventastic::repository::RepositoryTransaction;
use eventastic::repository::Snapshot;
use futures::stream;
use futures::TryStreamExt;
use futures_util::stream::StreamExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sqlx::query;
use sqlx::query_as;
use sqlx::types::JsonValue;
use sqlx::types::Uuid;
use sqlx::QueryBuilder;
use sqlx::{Postgres, Transaction};

pub struct PostgresTransaction<'a> {
    pub(crate) inner: Transaction<'a, Postgres>,
}

impl<'a> PostgresTransaction<'a> {
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

    /// Returns a batch of 10 outbox items
    pub async fn get_outbox_batch<T>(&mut self) -> Result<Vec<OutBoxMessage<T>>, DbError>
    where
        T: DeserializeOwned,
        T: SideEffect,
    {
        let messages = query_as::<_, OutBoxRow>(
            "SELECT * from outbox WHERE requeue = true ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 10 "
        )
        .fetch_all(&mut *self.inner)
        .await?;

        messages
            .into_iter()
            .map(|m| {
                let message =
                    serde_json::from_value(m.message).map_err(DbError::SerializationError)?;
                Ok(OutBoxMessage::new(message, m.retries as _, m.requeue))
            })
            .collect::<Result<Vec<_>, _>>()
    }

    /// Deletes an item from the outbox.
    #[doc(hidden)]
    pub async fn delete_outbox_item<T>(&mut self, outbox_id: T) -> Result<(), DbError>
    where
        for<'sql> T: sqlx::Decode<'sql, Postgres>
            + sqlx::Type<Postgres>
            + sqlx::Encode<'sql, Postgres>
            + Unpin,
    {
        let _ = query("DELETE FROM outbox where id = $1")
            .bind(outbox_id)
            .execute(&mut *self.inner)
            .await?;
        Ok(())
    }

    /// Update the [`OutBoxMessage::retries`] and [`OutBoxMessage:requeue`] for a specific [`OutBoxMessage`]
    #[doc(hidden)]
    pub async fn update_outbox_item<T>(
        &mut self,
        outbox_item: OutBoxMessage<T>,
    ) -> Result<(), DbError>
    where
        T: SideEffect + DeserializeOwned,
        for<'sql> T::Id: sqlx::Decode<'sql, Postgres>
            + sqlx::Type<Postgres>
            + sqlx::Encode<'sql, Postgres>
            + Unpin,
    {
        let _ = query("UPDATE outbox set retries = $2, requeue = $3 where id = $1")
            .bind(outbox_item.message.id())
            .bind(i32::from(outbox_item.retries))
            .bind(outbox_item.requeue)
            .execute(&mut *self.inner)
            .await?;
        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct PartialSnapShotRow {
    snapshot: serde_json::Value,
}

#[derive(Debug, sqlx::FromRow)]
struct PartialEventRow<EId>
where
    EId: Unpin,
{
    event_id: EId,
    version: i64,
    event: JsonValue,
}

impl<EId> PartialEventRow<EId>
where
    EId: Debug + Send + Unpin,
{
    fn to_event<Evt>(
        row: PartialEventRow<EId>,
    ) -> Result<eventastic::event::EventStoreEvent<EId, Evt>, DbError>
    where
        Evt: Send + Clone + Eq + DeserializeOwned,
    {
        let row_version = u64::try_from(row.version).map_err(|_| DbError::InvalidVersionNumber)?;
        match serde_json::from_value::<Evt>(row.event) {
            Ok(e) => Ok(EventStoreEvent {
                id: row.event_id,
                event: e,
                version: row_version,
            }),
            Err(e) => Err(DbError::SerializationError(e)),
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct OutBoxRow {
    message: JsonValue,
    retries: i32,
    requeue: bool,
    #[allow(dead_code)]
    created_at: DateTime<Utc>,
}

#[async_trait]
impl<
        S: SideEffect<Id = Uuid>,
        T: Aggregate<DomainEventId = Uuid, AggregateId = Uuid, SideEffect = S>,
        'a,
    > RepositoryTransaction<T> for PostgresTransaction<'a>
where
    T: Aggregate + 'a + DeserializeOwned + Serialize + Send + Sync,
    S: 'a,
    <T as Aggregate>::DomainEvent: Serialize + DeserializeOwned + Send + Sync,
    <T as Aggregate>::SideEffect: Send + Sync,
{
    /// The type of error that is returned from the database.
    type DbError = DbError;

    /// Returns a stream of domain events.
    fn stream(
        &mut self,
        id: &T::AggregateId,
    ) -> Stream<T::DomainEventId, T::DomainEvent, Self::DbError> {
        let res = query_as::<_, PartialEventRow<T::DomainEventId>>(
            "
            SELECT event, event_id, version
            FROM events 
            where aggregate_id = $1 ORDER BY version ASC",
        )
        .bind(*id)
        .fetch(&mut *self.inner);

        res.map(|row| match row {
            Ok(row) => PartialEventRow::to_event(row),
            Err(e) => Err(DbError::DbError(e)),
        })
        .boxed()
    }

    /// Returns a stream of domain events.
    fn stream_from(
        &mut self,
        id: &T::AggregateId,
        version: u64,
    ) -> Stream<T::DomainEventId, T::DomainEvent, Self::DbError> {
        let Ok(version) = i64::try_from(version) else {
            return stream::iter(vec![Err(DbError::InvalidVersionNumber)]).boxed();
        };

        let res = query_as::<_, PartialEventRow<T::DomainEventId>>(
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
        event_id: &T::DomainEventId,
    ) -> Result<
        Option<EventStoreEvent<<T as Aggregate>::DomainEventId, <T as Aggregate>::DomainEvent>>,
        Self::DbError,
    > {
        let res = query_as::<_, PartialEventRow<T::DomainEventId>>(
            "SELECT event, event_id, version FROM events where aggregate_id = $1 AND event_id = $2",
        )
        .bind(aggregate_id)
        .bind(event_id)
        .fetch_optional(&mut *self.inner)
        .await;

        match res {
            Ok(Some(row)) => match PartialEventRow::to_event(row) {
                Ok(e) => Ok(Some(e)),
                Err(e) => Err(e),
            },
            Ok(None) => Ok(None),
            Err(e) => Err(DbError::DbError(e)),
        }
    }

    /// Adds new domain events to the database
    async fn append(
        &mut self,
        id: &T::AggregateId,
        events: Vec<EventStoreEvent<T::DomainEventId, T::DomainEvent>>,
    ) -> Result<Vec<T::DomainEventId>, Self::DbError> {
        if events.is_empty() {
            return Ok(Vec::new());
        }

        let mut event_ids_to_insert: Vec<T::DomainEventId> = Vec::with_capacity(events.len());
        let mut versions_to_insert: Vec<i64> = Vec::with_capacity(events.len());
        let mut aggregate_ids_to_insert: Vec<T::AggregateId> = Vec::with_capacity(events.len());
        let mut events_to_insert: Vec<serde_json::Value> = Vec::with_capacity(events.len());
        let mut created_ats_to_insert: Vec<DateTime<Utc>> = Vec::with_capacity(events.len());

        for event in events {
            let event_id = *event.id();
            let version = event.version;

            let version = i64::try_from(version).map_err(|_| DbError::InvalidVersionNumber)?;

            match serde_json::to_value(event.event).map_err(DbError::SerializationError) {
                Ok(s) => {
                    event_ids_to_insert.push(event_id);
                    versions_to_insert.push(version);
                    aggregate_ids_to_insert.push(*id);
                    events_to_insert.push(s);
                    created_ats_to_insert.push(Utc::now());
                }
                Err(e) => return Err(e),
            }
        }

        let inserted_ids = sqlx::query!(
            "INSERT INTO events(event_id, version, aggregate_id, event, created_at) 
            SELECT * FROM UNNEST($1::uuid[], $2::bigint[], $3::uuid[], $4::jsonb[], $5::timestamptz[])
            ON CONFLICT DO NOTHING returning event_id",
            &event_ids_to_insert[..],
            &versions_to_insert[..],
            &aggregate_ids_to_insert[..],
            &events_to_insert[..],
            &created_ats_to_insert[..]
        ).fetch_all(&mut *self.inner).await?.into_iter().map(|row| row.event_id).collect();

        Ok(inserted_ids)
    }

    /// Returns a snapshot of the aggregate in the database
    async fn get_snapshot(&mut self, id: &T::AggregateId) -> Option<Snapshot<T>> {
        let json_value = query_as::<_, PartialSnapShotRow>(
            "SELECT snapshot from snapshots where aggregate_id = $1",
        )
        .bind(id)
        .fetch_one(&mut *self.inner)
        .await
        .ok()?;

        serde_json::from_value(json_value.snapshot).ok()
    }

    /// Stores a snapshot of the aggregate in the database
    async fn store_snapshot(&mut self, snapshot: Snapshot<T>) -> Result<(), Self::DbError>
    where
        T: Serialize,
    {
        let aggregated_id = *snapshot.aggregate.aggregate_id();
        let json_value = serde_json::to_value(snapshot).map_err(DbError::SerializationError)?;
        query("INSERT INTO snapshots(aggregate_id, snapshot, created_at) VALUES ($1, $2, $3) ON CONFLICT (aggregate_id) DO UPDATE SET snapshot = $2, created_at = $3")
            .bind(aggregated_id)
            .bind(json_value)
            .bind(Utc::now())
            .execute(&mut *self.inner)
            .await?;

        Ok(())
    }

    /// Insert side effects into the database
    #[doc(hidden)]
    async fn insert_side_effects(
        &mut self,
        outbox_item: Vec<T::SideEffect>,
    ) -> Result<(), Self::DbError>
    where
        T::SideEffect: Serialize,
    {
        if outbox_item.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<Postgres> =
            QueryBuilder::new("INSERT INTO outbox(id, message, retries, requeue, created_at) ");

        let outbox_item = outbox_item
            .into_iter()
            .map(|item| {
                Ok((
                    *item.id(),
                    serde_json::to_value(item).map_err(DbError::SerializationError)?,
                ))
            })
            .collect::<Result<Vec<_>, DbError>>()?;

        query_builder.push_values(outbox_item, |mut b, item| {
            b.push_bind(item.0)
                .push_bind(item.1)
                .push_bind(0)
                .push_bind(true)
                .push_bind(Utc::now());
        });

        let query = query_builder.build();

        query.execute(&mut *self.inner).await?;
        Ok(())
    }

    /// Commit the transaction to the db.
    async fn commit(self) -> Result<(), Self::DbError> {
        Ok(self.commit().await?)
    }

    /// Loads an Aggregate Root instance from the data store,
    /// referenced by its unique identifier.
    async fn get(
        &mut self,
        id: &T::AggregateId,
    ) -> Result<Context<T>, RepositoryError<T::ApplyError, T::DomainEventId, Self::DbError>>
    where
        T: DeserializeOwned,
    {
        let snapshot = self.get_snapshot(id).await;

        let (context, version) = if let Some(snapshot) = snapshot {
            if snapshot.snapshot_version == T::SNAPSHOT_VERSION {
                // Snapshot is valid so return it
                let context: Context<T> = snapshot.into();
                // We want to get the next event in the stream
                let version = context.version() + 1;
                (Some(context), version)
            } else {
                (None, 0)
            }
        } else {
            (None, 0)
        };

        let ctx = <Self as RepositoryTransaction<T>>::stream_from(self, id, version)
            .map_err(RepositoryError::Repository)
            .try_fold(context, |ctx: Option<Context<T>>, event| async move {
                let new_ctx_result = match ctx {
                    None => Context::rehydrate_from(&event),
                    Some(ctx) => ctx.apply_rehydrated_event(&event),
                };

                let new_ctx = new_ctx_result.map_err(|e| RepositoryError::Apply(event.id, e))?;

                Ok(Some(new_ctx))
            })
            .await?;

        ctx.ok_or(RepositoryError::AggregateNotFound)
    }
}
