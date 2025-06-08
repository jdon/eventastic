use async_trait::async_trait;
use chrono::{DateTime, Utc};
use eventastic::aggregate::SideEffect;
use eventastic_postgres::{DbError, PostgresRepository, PostgresTransaction, SideEffectStorage};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::types::Uuid;
use sqlx::{Postgres, Transaction};
use std::sync::Arc;

use crate::OutboxMessage;

/// Default implementation of [`SideEffectStorage`] that stores messages in an `outbox` table.
#[derive(Clone, Copy, Default)]
pub struct TableOutbox;

#[async_trait]
impl SideEffectStorage for TableOutbox {
    async fn store_side_effects<T: SideEffect<SideEffectId = Uuid> + Serialize + Send + Sync>(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        items: Vec<T>,
    ) -> Result<(), DbError> {
        let mut ids: Vec<Uuid> = Vec::with_capacity(items.len());
        let mut messages: Vec<serde_json::Value> = Vec::with_capacity(items.len());
        let mut retries: Vec<i32> = Vec::with_capacity(items.len());
        let mut requeues: Vec<bool> = Vec::with_capacity(items.len());
        let mut created_ats: Vec<DateTime<Utc>> = Vec::with_capacity(items.len());

        for side_effect in items {
            let id = side_effect.id().clone();
            let msg = serde_json::to_value(side_effect).map_err(DbError::SerializationError)?;
            ids.push(id);
            messages.push(msg);
            retries.push(0);
            requeues.push(true);
            created_ats.push(Utc::now());
        }

        sqlx::query(
            "INSERT INTO outbox(id, message, retries, requeue, created_at)
             SELECT * FROM UNNEST($1::uuid[], $2::jsonb[], $3::int[], $4::boolean[], $5::timestamptz[])
             ON CONFLICT (id) DO UPDATE SET
                message = excluded.message,
                retries = excluded.retries,
                requeue = excluded.requeue,
                created_at = excluded.created_at",
        )
        .bind(&ids)
        .bind(&messages)
        .bind(&retries)
        .bind(&requeues)
        .bind(&created_ats)
        .execute(transaction.as_mut())
        .await?;

        Ok(())
    }
}

#[async_trait]
pub trait TransactionOutboxExt<T>
where
    T: SideEffect + DeserializeOwned + Send + 'static,
    T::SideEffectId: Clone + Send + 'static,
    for<'sql> T::SideEffectId:
        sqlx::Decode<'sql, Postgres> + sqlx::Type<Postgres> + sqlx::Encode<'sql, Postgres> + Unpin,
{
    async fn get_outbox_batch(&mut self) -> Result<Vec<OutboxMessage<T>>, DbError>;

    async fn delete_outbox_item(&mut self, id: T::SideEffectId) -> Result<(), DbError>;

    async fn update_outbox_item(&mut self, item: OutboxMessage<T>) -> Result<(), DbError>;
}

#[async_trait]
impl<'a, T> TransactionOutboxExt<T> for PostgresTransaction<'a, TableOutbox>
where
    T: SideEffect + DeserializeOwned + Send + 'static,
    T::SideEffectId: Clone + Send + 'static,
    for<'sql> T::SideEffectId:
        sqlx::Decode<'sql, Postgres> + sqlx::Type<Postgres> + sqlx::Encode<'sql, Postgres> + Unpin,
{
    async fn get_outbox_batch(&mut self) -> Result<Vec<OutboxMessage<T>>, DbError> {
        #[derive(sqlx::FromRow)]
        struct OutboxRow {
            message: serde_json::Value,
            retries: i32,
            requeue: bool,
        }

        let rows = sqlx::query_as::<_, OutboxRow>(
            "SELECT message, retries, requeue FROM outbox \
             WHERE requeue = true ORDER BY created_at \
             FOR UPDATE SKIP LOCKED LIMIT 10",
        )
        .fetch_all(self.inner_mut().as_mut())
        .await?;

        rows.into_iter()
            .map(|row| {
                let msg = serde_json::from_value(row.message)?;
                Ok(OutboxMessage::new(msg, row.retries as u16, row.requeue))
            })
            .collect::<Result<Vec<_>, serde_json::Error>>()
            .map_err(DbError::SerializationError)
    }

    async fn delete_outbox_item(&mut self, id: T::SideEffectId) -> Result<(), DbError> {
        sqlx::query("DELETE FROM outbox WHERE id = $1")
            .bind(id)
            .execute(self.inner_mut().as_mut())
            .await?;
        Ok(())
    }

    async fn update_outbox_item(&mut self, item: OutboxMessage<T>) -> Result<(), DbError> {
        sqlx::query("UPDATE outbox SET retries = $2, requeue = $3 WHERE id = $1")
            .bind(item.message.id())
            .bind(i32::from(item.retries))
            .bind(item.requeue)
            .execute(self.inner_mut().as_mut())
            .await?;

        Ok(())
    }
}

/// Trait used to handle side effects pulled from the outbox.
#[async_trait]
pub trait SideEffectHandler {
    type SideEffect: SideEffect;
    type Error: Send;

    /// Handle a side effect message.
    ///
    /// Returning `Ok(())` deletes the message from the outbox. Returning
    /// `Err((true, E))` requeues the message. Returning `Err((false, E))`
    /// leaves the message without requeuing.
    async fn handle(&self, msg: &Self::SideEffect, retries: u16)
    -> Result<(), (bool, Self::Error)>;
}

/// Extension trait for running the outbox worker using a [`TableOutbox`].
#[async_trait]
pub trait RepositoryOutboxExt {
    async fn start_outbox<T, H>(
        &self,
        handler: H,
        poll_interval: std::time::Duration,
    ) -> Result<(), DbError>
    where
        T: SideEffect + DeserializeOwned + Send + Sync + 'static,
        T::SideEffectId: Clone + Send + 'static,
        H: SideEffectHandler<SideEffect = T> + Send + Sync,
        for<'sql> T::SideEffectId: sqlx::Decode<'sql, Postgres>
            + sqlx::Type<Postgres>
            + sqlx::Encode<'sql, Postgres>
            + Unpin;
}

#[async_trait]
impl RepositoryOutboxExt for PostgresRepository<TableOutbox> {
    async fn start_outbox<T, H>(
        &self,
        handler: H,
        poll_interval: std::time::Duration,
    ) -> Result<(), DbError>
    where
        T: SideEffect + DeserializeOwned + Send + Sync + 'static,
        T::SideEffectId: Clone + Send + 'static,
        H: SideEffectHandler<SideEffect = T> + Send + Sync,
        for<'sql> T::SideEffectId: sqlx::Decode<'sql, Postgres>
            + sqlx::Type<Postgres>
            + sqlx::Encode<'sql, Postgres>
            + Unpin,
    {
        let handler = Arc::new(handler);
        loop {
            let deadline = std::time::Instant::now() + poll_interval;
            let _ = process_outbox_batch::<T, H>(self, handler.clone()).await;
            tokio::time::sleep_until(deadline.into()).await;
        }
    }
}

async fn process_outbox_batch<T, H>(
    repo: &PostgresRepository<TableOutbox>,
    handler: Arc<H>,
) -> Result<(), DbError>
where
    T: SideEffect + DeserializeOwned + Send + Sync + 'static,
    T::SideEffectId: Clone + Send + 'static,
    H: SideEffectHandler<SideEffect = T> + Send + Sync,
    for<'a> PostgresTransaction<'a, TableOutbox>: TransactionOutboxExt<T>,
    for<'sql> T::SideEffectId:
        sqlx::Decode<'sql, Postgres> + sqlx::Type<Postgres> + sqlx::Encode<'sql, Postgres> + Unpin,
{
    let mut tx = repo.begin_transaction().await?;

    let outbox_items: Vec<OutboxMessage<T>> = tx.get_outbox_batch().await?;

    for mut item in outbox_items {
        let id: T::SideEffectId = item.message.id().clone();

        match handler.handle(&item.message, item.retries).await {
            Ok(()) => {
                tx.delete_outbox_item(id).await?;
            }
            Err((requeue, _)) => {
                item.retries += 1;
                item.requeue = requeue;
                tx.update_outbox_item(item).await?;
            }
        }
    }

    tx.commit().await
}
