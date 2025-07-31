use crate::DbError;
use crate::pickle::Pickle;
use async_trait::async_trait;
use eventastic::aggregate::SideEffect;
use sqlx::types::Uuid;
use sqlx::{Postgres, Transaction};

/// Trait for storing side effects in a PostgreSQL database.
///
/// This trait abstracts the storage mechanism for side effects, allowing
/// different implementations such as direct table storage or outbox patterns.
/// Implementors define how side effects are persisted within a database transaction.
#[async_trait]
pub trait SideEffectStorage<E, T>: Send + Sync
where
    T: SideEffect<SideEffectId = Uuid> + Pickle + Send + Sync,
{
    /// Store a collection of side effects within the given database transaction.
    ///
    /// This method is called as part of the aggregate save process to ensure
    /// side effects are stored atomically with domain events.
    ///
    /// # Parameters
    ///
    /// - `transaction` - The database transaction to use for storage
    /// - `items` - Collection of side effects to store
    ///
    /// # Errors
    ///
    /// Returns [`DbError`] if the storage operation fails.
    async fn store_side_effects(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        items: Vec<T>,
    ) -> Result<(), DbError<E>>;
}
