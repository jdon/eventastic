use crate::{DbError, PostgresRepository};
use eventastic::aggregate::{SideEffect, SideEffectHandler};
use serde::de::DeserializeOwned;
use sqlx::Postgres;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct OutBoxMessage<T>
where
    T: SideEffect,
{
    /// The contents of this outbox message
    /// This is usually an enum
    pub message: T,

    /// The amount of times this message has been retried
    pub(crate) retries: u16,

    /// Whether or not to requeue this item
    /// If set to false this item won't be requeued but will remain in the repository.
    pub requeue: bool,
}

impl<T> OutBoxMessage<T>
where
    T: SideEffect,
{
    pub fn new(message: T, retries: u16, requeue: bool) -> OutBoxMessage<T> {
        OutBoxMessage {
            message,
            retries,
            requeue,
        }
    }
}

impl<T> OutBoxMessage<T>
where
    T: SideEffect,
{
    /// The amount of times this message has been retried
    pub fn retries(&self) -> u16 {
        self.retries
    }
}

impl PostgresRepository {
    /// Start the outbox.
    /// This function will run forever, so should generally be spawned as a background task
    pub async fn start_outbox<T, H>(
        &self,
        handler: H,
        poll_interval: std::time::Duration,
    ) -> Result<(), DbError>
    where
        T: SideEffect + DeserializeOwned,
        H: SideEffectHandler<SideEffect = T>,
        for<'sql> <T as SideEffect>::Id: sqlx::Decode<'sql, Postgres>
            + sqlx::Type<Postgres>
            + sqlx::Encode<'sql, Postgres>
            + Unpin,
    {
        let handler = Arc::new(handler);
        loop {
            let deadline = std::time::Instant::now() + poll_interval;

            // Errors are ignored in the default implementation as they are added to the dead box.
            let _ = self.process_outbox_batch::<T, H>(handler.clone()).await;
            tokio::time::sleep_until(deadline.into()).await;
        }
    }

    /// Process a batch of outbox items.
    #[doc(hidden)]
    async fn process_outbox_batch<T, H>(&self, handler: Arc<H>) -> Result<(), DbError>
    where
        T: SideEffect + DeserializeOwned,
        H: SideEffectHandler<SideEffect = T>,
        for<'sql> <T as SideEffect>::Id: sqlx::Decode<'sql, Postgres>
            + sqlx::Type<Postgres>
            + sqlx::Encode<'sql, Postgres>
            + Unpin,
    {
        let mut tx = self.begin_transaction().await?;

        let outbox_items = tx.get_outbox_batch::<T>().await?;

        for mut item in outbox_items {
            let item_id = item.message.id().clone();

            match handler.handle(&item.message, item.retries).await {
                Ok(_) => {
                    tx.delete_outbox_item(item_id).await?;
                }
                Err((requeue, _)) => {
                    item.retries += 1;
                    item.requeue = requeue;
                    tx.update_outbox_item(item).await?;
                }
            };
        }

        tx.commit().await?;
        Ok(())
    }
}
