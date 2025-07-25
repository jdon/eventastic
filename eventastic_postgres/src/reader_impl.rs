//! Generic implementations for [`RepositoryReader`] operations.
//!
//! This module contains shared implementation logic for reading operations
//! that can be used by both [`PostgresTransaction`] and [`PostgresConnection`].
//! All operations use dynamic table names provided via the [`TableConfig`].

use crate::DbError;
use crate::common::{PartialEventRow, PartialSnapshotRow, utils};
use crate::pickle::Pickle;
use eventastic::aggregate::Aggregate;
use eventastic::event::DomainEvent;
use eventastic::event::EventStoreEvent;
use eventastic::repository::Snapshot;
use futures::stream;
use futures_util::stream::StreamExt;
use sqlx::types::Uuid;
use sqlx::{Executor, query_as};

/// Generic implementation for streaming events from configured table.
pub fn stream_from<'e, 'c: 'e, E, T>(
    executor: E,
    id: &T::AggregateId,
    version: u64,
    query: String,
) -> impl futures::Stream<Item = std::result::Result<EventStoreEvent<T::DomainEvent>, DbError>> + 'e
where
    E: Executor<'c, Database = sqlx::Postgres> + 'e,
    T: Aggregate<AggregateId = Uuid>,
    T::DomainEvent: DomainEvent<EventId = Uuid> + Pickle + Send + 'e,
{
    let Ok(version) = utils::version_to_i64(version) else {
        return stream::iter(vec![Err(DbError::InvalidVersionNumber)]).boxed();
    };

    let id = *id;

    stream::once(async move {
        query_as::<_, PartialEventRow>(&query)
            .bind(id)
            .bind(version)
            .fetch_all(executor)
            .await
    })
    .map(|result| match result {
        Ok(rows) => stream::iter(rows.into_iter().map(PartialEventRow::to_event)).boxed(),
        Err(e) => stream::iter(vec![Err(DbError::DbError(e))]).boxed(),
    })
    .flatten()
    .boxed()
}

/// Generic implementation for getting an event by ID from configured table.
pub async fn get_event<'c, E, T>(
    executor: E,
    aggregate_id: &T::AggregateId,
    event_id: &<<T as Aggregate>::DomainEvent as DomainEvent>::EventId,
    query: &str,
) -> Result<Option<EventStoreEvent<<T as Aggregate>::DomainEvent>>, DbError>
where
    E: Executor<'c, Database = sqlx::Postgres>,
    T: Aggregate<AggregateId = Uuid>,
    T::DomainEvent: DomainEvent<EventId = Uuid> + Pickle + Send,
{
    query_as::<_, PartialEventRow>(query)
        .bind(aggregate_id)
        .bind(event_id)
        .fetch_optional(executor)
        .await?
        .map(PartialEventRow::to_event)
        .transpose()
}

/// Generic implementation for getting a snapshot from configured table.
pub async fn get_snapshot<'c, E, T>(
    executor: E,
    id: &T::AggregateId,
    query: &str,
) -> Result<Option<Snapshot<T>>, DbError>
where
    E: Executor<'c, Database = sqlx::Postgres>,
    T: Aggregate<AggregateId = Uuid> + Pickle,
{
    let row = query_as::<_, PartialSnapshotRow>(query)
        .bind(id)
        .bind(utils::snapshot_version_to_i64(T::SNAPSHOT_VERSION)?)
        .fetch_optional(executor)
        .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    Ok(Some(PartialSnapshotRow::to_snapshot(row)?))
}
