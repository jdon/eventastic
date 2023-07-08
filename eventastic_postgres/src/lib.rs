use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use eventastic::aggregate::Aggregate;
use eventastic::aggregate::OutBoxHandler;
use eventastic::aggregate::OutBoxMessage;
use eventastic::aggregate::Repository;
use eventastic::aggregate::RepositoryTransaction;
use eventastic::aggregate::Snapshot;
use eventastic::event::Event;
use eventastic::event::EventStoreEvent;
use eventastic::event::Stream;
use futures_util::stream::StreamExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sqlx::query_as;
use sqlx::types::JsonValue;
use sqlx::QueryBuilder;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Pool, Postgres, Transaction,
};
use thiserror::Error;
use uuid::Uuid;

pub struct PostgresRepository {
    inner: Pool<Postgres>,
}

impl PostgresRepository {
    pub async fn new(
        connect_options: PgConnectOptions,
        pool_options: PgPoolOptions,
    ) -> Result<Self, sqlx::Error> {
        let pool = pool_options.connect_with(connect_options).await?;

        Ok(Self { inner: pool })
    }

    pub async fn transaction(&self) -> Result<PostgresTransaction<'_>, sqlx::Error> {
        Ok(PostgresTransaction {
            inner: self.inner.begin().await?,
        })
    }
}

#[async_trait]
impl<'a, T> Repository<'a, T, PostgresTransaction<'a>> for PostgresRepository
where
    T: Aggregate<AggregateId = Uuid, DomainEventId = Uuid, SideEffectId = Uuid> + 'a,
    <T as Aggregate>::DomainEvent: Serialize,
    T::DomainEvent: DeserializeOwned + Serialize,
    T::SideEffect: DeserializeOwned + Serialize,
{
    type DbError = DbError;

    async fn begin_transaction(&'a self) -> Result<PostgresTransaction<'a>, Self::DbError> {
        Ok(self.transaction().await?)
    }
}

pub struct PostgresTransaction<'a> {
    inner: Transaction<'a, Postgres>,
}

impl<'a> PostgresTransaction<'a> {
    pub async fn commit(self) -> Result<(), sqlx::Error> {
        self.inner.commit().await
    }
}

#[derive(Debug, sqlx::FromRow)]
struct PartialEventRow {
    event_id: Uuid,
    version: i64,
    event: JsonValue,
}

struct FullEventRow {
    event_id: Uuid,
    version: i64,
    aggregate_id: Uuid,
    event: JsonValue,
    created_at: DateTime<Utc>,
}

#[derive(Debug, sqlx::FromRow)]
struct OutBoxRow {
    id: Uuid,
    message: JsonValue,
    retries: i64,
    requeue: bool,
    #[allow(dead_code)]
    created_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct PartialSnapShotRow {
    snapshot: serde_json::Value,
}

impl PartialEventRow {
    fn to_event<Evt>(
        row: PartialEventRow,
    ) -> Result<eventastic::event::EventStoreEvent<Uuid, Evt>, DbError>
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

#[derive(Error, Debug)]
pub enum DbError {
    #[error("DB Error {0}")]
    DbError(#[from] sqlx::Error),
    #[error("Serialization Error {0}")]
    SerializationError(#[source] serde_json::Error),
}

#[async_trait]
impl<'a, T> OutBoxHandler<Uuid, T> for PostgresTransaction<'a>
where
    T: Send,
{
    type OutBoxDbError = DbError;

    async fn get_outbox_items(&mut self) -> Result<Vec<OutBoxMessage<Uuid, T>>, Self::OutBoxDbError>
    where
        T: DeserializeOwned,
    {
        let messages = sqlx::query_as!(
            OutBoxRow,
            "SELECT * from outbox WHERE requeue = true ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 10 "
        )
        .fetch_all(&mut *self.inner)
        .await?;

        messages
            .into_iter()
            .map(|m| {
                let message =
                    serde_json::from_value(m.message).map_err(DbError::SerializationError)?;
                Ok(OutBoxMessage::new(m.id, message, m.retries as _, m.requeue))
            })
            .collect::<Result<Vec<_>, _>>()
    }

    /// Delete an item from the outbox.
    #[doc(hidden)]
    async fn delete_outbox_item(&mut self, outbox_id: Uuid) -> Result<(), Self::OutBoxDbError> {
        let _ = sqlx::query!("DELETE FROM outbox where id = $1", outbox_id)
            .execute(&mut *self.inner)
            .await?;
        Ok(())
    }

    /// Insert or update an outbox item into the repository.
    #[doc(hidden)]
    async fn upsert_outbox_item(
        &mut self,
        outbox_item: OutBoxMessage<Uuid, T>,
    ) -> Result<(), Self::OutBoxDbError>
    where
        T: Serialize + 'async_trait,
    {
        let retires = outbox_item.retries() as i32;
        let json_value =
            serde_json::to_value(outbox_item.message).map_err(DbError::SerializationError)?;
        sqlx::query!(
            "INSERT INTO outbox(id, message, retries, requeue, created_at) VALUES ($1, $2, $3, $4, $5) ON CONFLICT ON CONSTRAINT outbox_pkey DO UPDATE set retries = $3, requeue = $4",
            outbox_item.id,
            json_value,
            retires,
            outbox_item.requeue,
            Utc::now()
        )
        .execute(&mut *self.inner)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl<'a, T> RepositoryTransaction<'a, T> for PostgresTransaction<'a>
where
    T: Aggregate<AggregateId = Uuid, DomainEventId = Uuid, SideEffectId = Uuid> + 'a,
    <T as Aggregate>::DomainEvent: Serialize,
    for<'de> <T as Aggregate>::DomainEvent: serde::Deserialize<'de>,
{
    type DbError = DbError;

    fn stream(
        &mut self,
        id: &T::AggregateId,
    ) -> Stream<T::AggregateId, T::DomainEvent, Self::DbError> {
        let res = query_as!(
            PartialEventRow,
            "
            SELECT event, event_id, version
            FROM events 
            where aggregate_id = $1 ",
            id
        )
        .fetch(&mut *self.inner);

        res.map(|row| match row {
            Ok(row) => PartialEventRow::to_event(row),
            Err(e) => Err(DbError::DbError(e)),
        })
        .boxed()
    }

    async fn get_event(
        &mut self,
        aggregate_id: &T::AggregateId,
        event_id: &T::DomainEventId,
    ) -> Result<
        Option<EventStoreEvent<<T as Aggregate>::DomainEventId, <T as Aggregate>::DomainEvent>>,
        Self::DbError,
    > {
        let res = query_as!(
            PartialEventRow,
            "
            SELECT event, event_id, version
            FROM events 
            where aggregate_id = $1 AND event_id = $2",
            aggregate_id,
            event_id
        )
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

    async fn append(
        &mut self,
        id: &T::AggregateId,
        events: Vec<EventStoreEvent<T::DomainEventId, T::DomainEvent>>,
    ) -> Result<(), Self::DbError> {
        let events = events
            .into_iter()
            .map(|event| {
                let event_id = *event.id();
                let event_version = event.version;
                match serde_json::to_value(event.event).map_err(DbError::SerializationError) {
                    Ok(s) => Ok(FullEventRow {
                        event_id,
                        version: i64::from(event_version),
                        aggregate_id: *id,
                        event: s,
                        created_at: Utc::now(),
                    }),
                    Err(e) => Err(e),
                }
            })
            .collect::<Result<Vec<FullEventRow>, Self::DbError>>()?;

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

    async fn get_snapshot(&mut self, id: &T::AggregateId) -> Option<Snapshot<T>>
    where
        T: DeserializeOwned,
    {
        let json_value = sqlx::query_as!(
            PartialSnapShotRow,
            "SELECT snapshot from snapshots where aggregate_id = $1",
            id
        )
        .fetch_one(&mut *self.inner)
        .await
        .ok()?;

        serde_json::from_value(json_value.snapshot).ok()
    }

    async fn store_snapshot(&mut self, snapshot: Snapshot<T>) -> Result<(), Self::DbError>
    where
        T: Serialize,
    {
        let aggregated_id = *snapshot.aggregate.aggregate_id();
        let json_value = serde_json::to_value(snapshot).map_err(DbError::SerializationError)?;
        sqlx::query!(
            "INSERT INTO snapshots(aggregate_id, snapshot, created_at) VALUES ($1, $2,$3)",
            aggregated_id,
            json_value,
            Utc::now()
        )
        .execute(&mut *self.inner)
        .await?;

        Ok(())
    }

    async fn commit(self) -> Result<(), Self::DbError> {
        Ok(self.commit().await?)
    }
}
