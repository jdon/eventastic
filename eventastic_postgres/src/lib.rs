//! # Eventastic PostgreSQL Implementation
//!
//! This crate provides a PostgreSQL-based implementation of the eventastic
//! event sourcing framework. It includes:
//!
//! - [`PostgresRepository`] - PostgreSQL repository implementation
//! - [`PostgresTransaction`] - Transaction management for PostgreSQL
//! - Error handling specific to PostgreSQL operations
//! - Extensions for loading aggregates from PostgreSQL storage
//!
//! ## Features
//!
//! - Event streaming from PostgreSQL
//! - Snapshot storage and retrieval
//! - Optimistic concurrency control
//! - Side effect storage integration
//!
//! ## Example
//!
//! ```rust,ignore
//! use eventastic_postgres::{PostgresRepository, PostgresTransaction};
//! use sqlx::postgres::PgConnectOptions;
//!
//! let connect_options = PgConnectOptions::new()
//!     .host("localhost")
//!     .database("eventstore");
//!     
//! let repository = PostgresRepository::new(
//!     connect_options,
//!     sqlx::pool::PoolOptions::new(),
//!     outbox_storage,
//! ).await?;
//! ```

mod repository;
mod side_effect;
mod transaction;
use async_trait::async_trait;
use eventastic::{
    aggregate::{Aggregate, Context, SideEffect},
    event::DomainEvent,
    repository::RepositoryError,
};
pub use repository::PostgresRepository;
use serde::{Serialize, de::DeserializeOwned};
pub use side_effect::SideEffectStorage;
use sqlx::types::Uuid;

use thiserror::Error;
pub use transaction::PostgresTransaction;

/// Errors that can occur during PostgreSQL operations.
#[derive(Error, Debug)]
pub enum DbError {
    /// A database operation failed.
    #[error("DB Error {0}")]
    DbError(sqlx::Error),
    /// Failed to serialize or deserialize data to/from JSON.
    #[error("Serialization Error {0}")]
    SerializationError(#[from] serde_json::Error),
    /// An invalid version number was encountered (e.g., negative value where positive expected).
    #[error("Invalid Version Number")]
    InvalidVersionNumber,
    /// An invalid snapshot version number was encountered.
    #[error("Invalid Snapshot Version number")]
    InvalidSnapshotVersion,
    /// A concurrent modification was detected (optimistic locking failure).
    #[error("Optimistic Concurrency Error")]
    OptimisticConcurrencyError,
}

impl From<sqlx::Error> for DbError {
    fn from(e: sqlx::Error) -> Self {
        if let Some(db_error) = e.as_database_error() {
            if let Some(code) = db_error.code() {
                if code == "23505" && db_error.message().contains("aggregate_version") {
                    return DbError::OptimisticConcurrencyError;
                }
            }
        }
        DbError::DbError(e)
    }
}

/// Extension trait for loading aggregates from PostgreSQL storage.
///
/// This trait provides PostgreSQL-specific methods for working with aggregates
/// that have UUID-based identifiers and can be serialized to JSON.
#[async_trait]
pub trait RootExt<T, O>
where
    T: Aggregate<AggregateId = Uuid> + Serialize + DeserializeOwned + Send + Sync + 'static,
    <T as Aggregate>::DomainEvent:
        DomainEvent<EventId = Uuid> + Serialize + DeserializeOwned + Send + Sync,
    <T as Aggregate>::SideEffect: SideEffect<SideEffectId = Uuid> + Serialize + Send + Sync,
    O: SideEffectStorage + Send + Sync,
{
    /// Loads an aggregate from PostgreSQL storage by its UUID.
    ///
    /// This method replays the event stream for the given aggregate ID,
    /// starting from any available snapshot and applying subsequent events.
    async fn load(
        transaction: &mut PostgresTransaction<'_, O>,
        aggregate_id: Uuid,
    ) -> Result<
        Context<T>,
        RepositoryError<
            T::ApplyError,
            <<T as Aggregate>::DomainEvent as DomainEvent>::EventId,
            DbError,
        >,
    > {
        Context::load(transaction, &aggregate_id).await
    }
}

impl<T, O> RootExt<T, O> for T
where
    T: Aggregate<AggregateId = Uuid> + Serialize + DeserializeOwned + Send + Sync + 'static,
    <T as Aggregate>::DomainEvent:
        DomainEvent<EventId = Uuid> + Serialize + DeserializeOwned + Send + Sync,
    <T as Aggregate>::SideEffect: SideEffect<SideEffectId = Uuid> + Serialize + Send + Sync,
    O: SideEffectStorage + Send + Sync,
{
}
