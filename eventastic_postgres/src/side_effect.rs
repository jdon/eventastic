use crate::DbError;
use async_trait::async_trait;
use eventastic::aggregate::SideEffect;
use serde::Serialize;
use sqlx::types::Uuid;
use sqlx::{Postgres, Transaction};

#[async_trait]
pub trait SideEffectStorage: Send + Sync {
    async fn store_side_effects<T: SideEffect<SideEffectId = Uuid> + Serialize + Send + Sync>(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        items: Vec<T>,
    ) -> Result<(), DbError>;
}
