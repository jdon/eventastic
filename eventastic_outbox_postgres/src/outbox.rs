use anyhow::{Context, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use eventastic::aggregate::SideEffect;
use eventastic_postgres::{
    DbError, EncryptionProvider, Pickle, PostgresRepository, PostgresTransaction, SideEffectStorage,
};
use sqlx::types::Uuid;
use sqlx::{Postgres, Transaction};
use std::sync::Arc;

use crate::OutboxMessage;

/// Default implementation of [`SideEffectStorage`] that stores messages in an `outbox` table.
#[derive(Clone, Copy, Default)]
pub struct TableOutbox<E> {
    encryption_provider: E,
}

impl<E> TableOutbox<E> {
    pub fn new(encryption_provider: E) -> Self {
        Self {
            encryption_provider,
        }
    }
}

#[async_trait]
impl<E: EncryptionProvider + Send + Sync + 'static> SideEffectStorage for TableOutbox<E> {
    async fn store_side_effects<T: SideEffect<SideEffectId = Uuid> + Pickle + Send + Sync>(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        items: Vec<T>,
    ) -> Result<(), DbError> {
        let mut ids: Vec<Uuid> = Vec::with_capacity(items.len());
        let mut messages: Vec<Vec<u8>> = Vec::with_capacity(items.len());
        let mut retries: Vec<i32> = Vec::with_capacity(items.len());
        let mut requeues: Vec<bool> = Vec::with_capacity(items.len());
        let mut created_ats: Vec<DateTime<Utc>> = Vec::with_capacity(items.len());

        for chunk in items.chunks(self.encryption_provider.max_batch_size()) {
            let mut plain = Vec::with_capacity(chunk.len());
            for side_effect in chunk {
                let id = *side_effect.id();
                let msg = side_effect
                    .pickle()
                    .context("Failed to pickle side effect")
                    .map_err(DbError::PicklingError)?;
                ids.push(id);
                plain.push(msg);
                retries.push(0);
                requeues.push(true);
                created_ats.push(Utc::now());
            }
            let number_of_items = plain.len();
            let mut cipher = self
                .encryption_provider
                .encrypt(plain)
                .await
                .context("Failed to encrypt side effects")
                .map_err(DbError::Encryption)?;
            if number_of_items != cipher.len() {
                return Err(DbError::Encryption(anyhow!(
                    "Encrypting side effects returned wrong number of items"
                )));
            }
            messages.append(&mut cipher);
        }

        sqlx::query(
            "INSERT INTO outbox(id, message, retries, requeue, created_at)
             SELECT * FROM UNNEST($1::uuid[], $2::bytea[], $3::int[], $4::boolean[], $5::timestamptz[])
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
    T: SideEffect + Pickle + Send + 'static,
    T::SideEffectId: Clone + Send + 'static,
    for<'sql> T::SideEffectId:
        sqlx::Decode<'sql, Postgres> + sqlx::Type<Postgres> + sqlx::Encode<'sql, Postgres> + Unpin,
{
    async fn get_outbox_batch(&mut self) -> Result<Vec<OutboxMessage<T>>, DbError>;

    async fn delete_outbox_item(&mut self, id: T::SideEffectId) -> Result<(), DbError>;

    async fn update_outbox_item(&mut self, item: OutboxMessage<T>) -> Result<(), DbError>;
}

#[async_trait]
impl<T, E> TransactionOutboxExt<T> for PostgresTransaction<'_, TableOutbox<E>, E>
where
    T: SideEffect + Pickle + Send + 'static,
    T::SideEffectId: Clone + Send + 'static,
    for<'sql> T::SideEffectId:
        sqlx::Decode<'sql, Postgres> + sqlx::Type<Postgres> + sqlx::Encode<'sql, Postgres> + Unpin,
    E: EncryptionProvider + Send + Sync + 'static,
{
    async fn get_outbox_batch(&mut self) -> Result<Vec<OutboxMessage<T>>, DbError> {
        #[derive(sqlx::FromRow)]
        struct OutboxRow {
            message: Vec<u8>,
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

        let mut messages = Vec::with_capacity(rows.len());
        for chunk in rows.chunks(self.encryption_provider().max_batch_size()) {
            let cipher: Vec<_> = chunk.into_iter().map(|row| row.message.clone()).collect();
            let number_of_items = cipher.len();
            let mut plain = self
                .encryption_provider()
                .decrypt(cipher)
                .await
                .context("Failed to decrypt side effects")
                .map_err(DbError::Encryption)?;
            if plain.len() != number_of_items {
                return Err(DbError::Encryption(anyhow!(
                    "Decrypting side effects returned wrong number of items"
                )));
            }
            messages.append(&mut plain);
        }

        rows.into_iter()
            .zip(messages.into_iter())
            .map(|(row, message)| {
                let msg = T::unpickle(&message).context("Failed to unpickle side effect")?;
                Ok(OutboxMessage::new(msg, row.retries as u16, row.requeue))
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()
            .map_err(DbError::PicklingError)
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

/// Trait for handling side effects pulled from the outbox.
///
/// Implementors define how to process side effects that have been stored
/// in the transactional outbox. The handler controls retry behavior through
/// its return values.
///
/// # Return Values
///
/// - `Ok(())` - Side effect processed successfully, message will be deleted
/// - `Err((true, E))` - Processing failed, message will be requeued for retry
/// - `Err((false, E))` - Processing failed, message will be marked as non-retryable
#[async_trait]
pub trait SideEffectHandler {
    type SideEffect: SideEffect;
    type Error: Send;

    /// Handle a side effect message.
    ///
    /// This method is called for each side effect retrieved from the outbox.
    /// The implementation should process the side effect and return appropriate
    /// results to control retry behavior.
    ///
    /// # Parameters
    ///
    /// - `msg` - The side effect to process
    /// - `retries` - Number of times this message has been retried
    ///
    /// # Returns
    ///
    /// - `Ok(())` - Processing successful, message will be deleted from outbox
    /// - `Err((true, E))` - Processing failed, message will be requeued for retry  
    /// - `Err((false, E))` - Processing failed, message will not be retried
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
        T: SideEffect + Pickle + Send + Sync + 'static,
        T::SideEffectId: Clone + Send + 'static,
        H: SideEffectHandler<SideEffect = T> + Send + Sync,
        for<'sql> T::SideEffectId: sqlx::Decode<'sql, Postgres>
            + sqlx::Type<Postgres>
            + sqlx::Encode<'sql, Postgres>
            + Unpin;
}

#[async_trait]
impl<E> RepositoryOutboxExt for PostgresRepository<TableOutbox<E>, E>
where
    E: EncryptionProvider + Clone + Send + Sync + 'static,
{
    async fn start_outbox<T, H>(
        &self,
        handler: H,
        poll_interval: std::time::Duration,
    ) -> Result<(), DbError>
    where
        T: SideEffect + Pickle + Send + Sync + 'static,
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
            let _ = process_outbox_batch::<T, H, E>(self, handler.clone()).await;
            tokio::time::sleep_until(deadline.into()).await;
        }
    }
}

async fn process_outbox_batch<T, H, E>(
    repo: &PostgresRepository<TableOutbox<E>, E>,
    handler: Arc<H>,
) -> Result<(), DbError>
where
    T: SideEffect + Pickle + Send + Sync + 'static,
    T::SideEffectId: Clone + Send + 'static,
    H: SideEffectHandler<SideEffect = T> + Send + Sync,
    E: EncryptionProvider + Clone + Send + Sync + 'static,
    for<'a> PostgresTransaction<'a, TableOutbox<E>, E>: TransactionOutboxExt<T>,
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
