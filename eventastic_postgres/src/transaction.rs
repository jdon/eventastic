use std::fmt::Debug;

use crate::outbox::OutBoxMessage;
use crate::DbError;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use eventastic::aggregate::Aggregate;
use eventastic::aggregate::SideEffect;
use eventastic::event::Event;
use eventastic::event::EventStoreEvent;
use eventastic::event::Stream;
use eventastic::repository::RepositoryTransaction;
use eventastic::repository::Snapshot;
use futures_util::stream::StreamExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sqlx::query;
use sqlx::query_as;
use sqlx::types::JsonValue;
use sqlx::QueryBuilder;
use sqlx::{Postgres, Transaction};

pub struct PostgresTransaction<'a> {
    pub(crate) inner: Transaction<'a, Postgres>,
}

impl<'a> PostgresTransaction<'a> {
    /// Commit the transaction to the db.
    pub async fn commit(self) -> Result<(), sqlx::Error> {
        self.inner.commit().await
    }

    /// Returns a batch of 10 outbox items
    pub async fn get_outbox_batch<T: SideEffect>(
        &mut self,
    ) -> Result<Vec<OutBoxMessage<T>>, DbError>
    where
        T: DeserializeOwned,
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
            + Unpin
            + Send
            + Sync,
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
            .bind(outbox_item.retries as i32)
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

struct FullEventRow<AId, EId>
where
    EId: Unpin,
    AId: Unpin,
{
    event_id: EId,
    version: i64,
    aggregate_id: AId,
    event: JsonValue,
    created_at: DateTime<Utc>,
}

impl<EId> PartialEventRow<EId>
where
    EId: Debug + Send + Sync + Unpin,
{
    fn to_event<Evt>(
        row: PartialEventRow<EId>,
    ) -> Result<eventastic::event::EventStoreEvent<EId, Evt>, DbError>
    where
        Evt: Send + Sync + Clone + Eq,
        for<'de> Evt: serde::Deserialize<'de>,
    {
        let version: u32 = {
            match row.version.try_into() {
                Ok(i) => i,
                Err(e) => {
                    return Err(DbError::DbError(sqlx::Error::Decode(Box::new(e))));
                }
            }
        };

        match serde_json::from_value::<Evt>(row.event) {
            Ok(e) => Ok(EventStoreEvent {
                id: row.event_id,
                event: e,
                version,
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
impl<'a, T> RepositoryTransaction<'a, T> for PostgresTransaction<'a>
where
    T: Aggregate + 'a + DeserializeOwned + Serialize,
    <T as Aggregate>::DomainEvent: Serialize,
    for<'sql> T::DomainEventId:
        sqlx::Decode<'sql, Postgres> + sqlx::Type<Postgres> + sqlx::Encode<'sql, Postgres> + Unpin,
    for<'sql> T::AggregateId:
        sqlx::Decode<'sql, Postgres> + sqlx::Type<Postgres> + sqlx::Encode<'sql, Postgres> + Unpin,
    for<'sql> <<T as eventastic::aggregate::Aggregate>::SideEffect as SideEffect>::Id:
        sqlx::Decode<'sql, Postgres> + sqlx::Type<Postgres> + sqlx::Encode<'sql, Postgres> + Unpin,
    for<'de> <T as Aggregate>::DomainEvent: serde::Deserialize<'de>,
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
            where aggregate_id = $1",
        )
        .bind(id.clone())
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
    ) -> Result<(), Self::DbError> {
        let events = events
            .into_iter()
            .map(|event| {
                let event_id = event.id().clone();
                let event_version = event.version;
                match serde_json::to_value(event.event).map_err(DbError::SerializationError) {
                    Ok(s) => Ok(FullEventRow {
                        event_id,
                        version: i64::from(event_version),
                        aggregate_id: id.clone(),
                        event: s,
                        created_at: Utc::now(),
                    }),
                    Err(e) => Err(e),
                }
            })
            .collect::<Result<Vec<FullEventRow<T::AggregateId, T::DomainEventId>>, Self::DbError>>(
            )?;

        let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO events(event_id, version, aggregate_id, event, created_at) ",
        );

        query_builder.push_values(events, |mut b, event| {
            b.push_bind(event.event_id)
                .push_bind(event.version)
                .push_bind(event.aggregate_id)
                .push_bind(event.event)
                .push_bind(event.created_at);
        });

        let query = query_builder.build();

        query.execute(&mut *self.inner).await?;

        Ok(())
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
        let aggregated_id = snapshot.aggregate.aggregate_id().clone();
        let json_value = serde_json::to_value(snapshot).map_err(DbError::SerializationError)?;
        query("INSERT INTO snapshots(aggregate_id, snapshot, created_at) VALUES ($1, $2,$3)")
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
        T::SideEffect: Serialize + 'async_trait,
    {
        let mut query_builder: QueryBuilder<Postgres> =
            QueryBuilder::new("INSERT INTO outbox(id, message, retries, requeue, created_at) ");

        // let items:  = vec![];

        let outbox_item = outbox_item
            .into_iter()
            .map(|item| {
                Ok((
                    item.id().clone(),
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
}
