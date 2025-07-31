//! Generic implementations for [`RepositoryReader`] operations.
//!
//! This module contains shared implementation logic for reading operations
//! that can be used by both [`PostgresTransaction`] and [`PostgresConnection`].
//! All operations use dynamic table names provided via the [`TableConfig`].

use std::sync::Arc;

use crate::common::{PartialEventRow, PartialSnapshotRow, utils};
use crate::pickle::Pickle;
use crate::{DbError, EncryptionProvider};
use eventastic::aggregate::Aggregate;
use eventastic::event::DomainEvent;
use eventastic::event::EventStoreEvent;
use eventastic::repository::Snapshot;
use futures_util::stream::StreamExt;
use sqlx::types::Uuid;
use sqlx::{Executor, query_as};

/// Generic implementation for streaming events from configured table.
pub fn stream_from<'e, 'c: 'e, E, T, EP>(
    executor: E,
    id: &T::AggregateId,
    version: u64,
    query: Arc<str>,
    encryption_provider: &'e EP,
) -> impl futures::Stream<
    Item = std::result::Result<EventStoreEvent<T::DomainEvent>, DbError<EP::Error>>,
> + 'e
where
    E: Executor<'c, Database = sqlx::Postgres> + 'e,
    T: Aggregate<AggregateId = Uuid>,
    T::DomainEvent: DomainEvent<EventId = Uuid> + Pickle + Send + 'e,
    EP: EncryptionProvider + Sync + Send + 'e,
{
    let id = *id;

    async_stream::stream! {
        let version = utils::version_to_i64(version)?;

        let chunks = query_as::<_, PartialEventRow>(&query)
            .bind(id)
            .bind(version)
            .fetch(executor)
            .chunks(encryption_provider.max_batch_size());

        for await chunk in chunks {
            let chunk = chunk.into_iter().collect::<Result<Vec<_>, _>>()?;
            // TODO: We could have the query return a vector of events rather than doing this here.
            let cipher: Vec<_> = chunk.iter().map(|row| row.event.clone()).collect();
            let number_of_items = cipher.len();
            let plain = encryption_provider
                .decrypt(cipher)
                .await
                .map_err(DbError::Encryption)?;
            if plain.len() != number_of_items {
                Err(DbError::EncrypytionProviderReturnedWrongNumberOfItems)?;
            }
            for (mut row, plain) in chunk.into_iter().zip(plain.into_iter()) {
                row.event = plain;
                yield PartialEventRow::to_event(row);
            }
        }
    }
}

/// Generic implementation for getting an event by ID from configured table.
pub async fn get_event<'c, E, T, EP>(
    executor: E,
    aggregate_id: &T::AggregateId,
    event_id: &<<T as Aggregate>::DomainEvent as DomainEvent>::EventId,
    query: &str,
    encryption_provider: &EP,
) -> Result<Option<EventStoreEvent<<T as Aggregate>::DomainEvent>>, DbError<EP::Error>>
where
    E: Executor<'c, Database = sqlx::Postgres>,
    T: Aggregate<AggregateId = Uuid>,
    T::DomainEvent: DomainEvent<EventId = Uuid> + Pickle + Send,
    EP: EncryptionProvider,
{
    let Some(mut row) = query_as::<_, PartialEventRow>(query)
        .bind(aggregate_id)
        .bind(event_id)
        .fetch_optional(executor)
        .await?
    else {
        return Ok(None);
    };
    let mut plain = encryption_provider
        .decrypt(vec![row.event])
        .await
        .map_err(DbError::Encryption)?
        .into_iter();
    let Some(event) = plain.next() else {
        return Err(DbError::EncrypytionProviderReturnedWrongNumberOfItems);
    };
    if plain.next().is_some() {
        return Err(DbError::EncrypytionProviderReturnedWrongNumberOfItems);
    }
    row.event = event;
    Ok(Some(PartialEventRow::to_event(row)?))
}

/// Generic implementation for getting a snapshot from configured table.
pub async fn get_snapshot<'c, E, T, EP>(
    executor: E,
    id: &T::AggregateId,
    query: &str,
    encryption_provider: &EP,
) -> Result<Option<Snapshot<T>>, DbError<EP::Error>>
where
    E: Executor<'c, Database = sqlx::Postgres>,
    T: Aggregate<AggregateId = Uuid> + Pickle,
    EP: EncryptionProvider,
{
    let row = query_as::<_, PartialSnapshotRow>(query)
        .bind(id)
        .bind(utils::snapshot_version_to_i64(T::SNAPSHOT_VERSION)?)
        .fetch_optional(executor)
        .await?;

    let Some(mut row) = row else {
        return Ok(None);
    };

    let plain = encryption_provider
        .decrypt(vec![row.aggregate.clone()])
        .await
        .map_err(DbError::Encryption)?;
    if plain.len() != 1 {
        Err(DbError::EncrypytionProviderReturnedWrongNumberOfItems)?;
    }
    row.aggregate = plain
        .into_iter()
        .next()
        .expect("Decrypt must return 1 item for snapshot");

    Ok(Some(PartialSnapshotRow::to_snapshot(row)?))
}
