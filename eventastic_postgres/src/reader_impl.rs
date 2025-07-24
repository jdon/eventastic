//! Generic implementations for [`RepositoryReader`] operations.
//!
//! This module contains shared implementation logic for reading operations
//! that can be used by both [`PostgresTransaction`] and [`PostgresConnection`].
//! All operations use dynamic table names provided via the [`TableConfig`].

use std::sync::Arc;

use crate::common::{PartialEventRow, PartialSnapshotRow, utils};
use crate::pickle::Pickle;
use crate::{DbError, EncryptionProvider};
use anyhow::{Context, anyhow};
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
) -> impl futures::Stream<Item = std::result::Result<EventStoreEvent<T::DomainEvent>, DbError>> + 'e
where
    E: Executor<'c, Database = sqlx::Postgres> + 'e,
    T: Aggregate<AggregateId = Uuid>,
    T::DomainEvent: DomainEvent<EventId = Uuid> + Pickle + Send + 'e,
    EP: EncryptionProvider + Sync + Send + 'e,
{
    let id = *id;

    Box::pin(async_stream::stream! {
        let version = utils::version_to_i64(version)?;

        let chunks = query_as::<_, PartialEventRow>(&query)
            .bind(id)
            .bind(version)
            .fetch(executor)
            .chunks(encryption_provider.max_batch_size());

        for await chunk in chunks {
            let chunk = chunk.into_iter().collect::<Result<Vec<_>, _>>()?;
            let cipher: Vec<_> = chunk.iter().map(|row| row.event.clone()).collect();
            let number_of_items = cipher.len();
            let plain = encryption_provider
                .decrypt(cipher)
                .await
                .context("Decryption error")
                .map_err(DbError::Encryption)?;
            if plain.len() != number_of_items {
                Err(DbError::Encryption(anyhow!(
                    "Decrypting events returned wrong number of items"
                )))?;
            }
            for (mut row, plain) in chunk.into_iter().zip(plain.into_iter()) {
                row.event = plain;
                yield PartialEventRow::to_event(row);
            }
        }
    })
}

/// Generic implementation for getting an event by ID from configured table.
pub async fn get_event<'c, E, T, EP>(
    executor: E,
    aggregate_id: &T::AggregateId,
    event_id: &<<T as Aggregate>::DomainEvent as DomainEvent>::EventId,
    query: &str,
    encryption_provider: &EP,
) -> Result<Option<EventStoreEvent<<T as Aggregate>::DomainEvent>>, DbError>
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
    let plain = encryption_provider
        .decrypt(vec![row.event])
        .await
        .context("Failed to decrypt event")
        .map_err(DbError::Encryption)?;
    if plain.len() != 1 {
        Err(DbError::Encryption(anyhow!(
            "Decrypting event returned wrong number of items"
        )))?;
    }
    row.event = plain
        .into_iter()
        .next()
        .expect("Decrypt must return 1 item for event");
    return Ok(Some(PartialEventRow::to_event(row)?));
}

/// Generic implementation for getting a snapshot from configured table.
pub async fn get_snapshot<'c, E, T, EP>(
    executor: E,
    id: &T::AggregateId,
    query: &str,
    encryption_provider: &EP,
) -> Result<Option<Snapshot<T>>, DbError>
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
        .context("Failed to decrypt snapshot")
        .map_err(DbError::Encryption)?;
    if plain.len() != 1 {
        Err(DbError::Encryption(anyhow!(
            "Decrypting snapshot returned wrong number of items"
        )))?;
    }
    row.aggregate = plain
        .into_iter()
        .next()
        .expect("Decrypt must return 1 item for snapshot");

    Ok(Some(PartialSnapshotRow::to_snapshot(row)?))
}
