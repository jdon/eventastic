use async_trait::async_trait;
use chrono::{DateTime, Utc};
use eventastic_postgres::{DbError, TransactionalOutbox};
use sqlx::{Postgres, Transaction};
use sqlx::types::Uuid;

/// Default implementation of [`TransactionalOutbox`] that stores messages in an `outbox` table.
#[derive(Clone, Copy, Default)]
pub struct TableOutbox;

#[async_trait]
impl TransactionalOutbox for TableOutbox {
    async fn store_side_effects(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        items: Vec<(Uuid, serde_json::Value)>,
    ) -> Result<(), DbError> {
        let mut ids: Vec<Uuid> = Vec::with_capacity(items.len());
        let mut messages: Vec<serde_json::Value> = Vec::with_capacity(items.len());
        let mut retries: Vec<i32> = Vec::with_capacity(items.len());
        let mut requeues: Vec<bool> = Vec::with_capacity(items.len());
        let mut created_ats: Vec<DateTime<Utc>> = Vec::with_capacity(items.len());

        for (id, msg) in items {
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
