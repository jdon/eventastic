//! Repository abstractions for event sourcing persistence.
//!
//! This module defines the core persistence traits that concrete implementations
//! (like `eventastic_postgres`) must implement to support event sourcing operations.
//!
//! ## Repository Traits
//!
//! ### [`RepositoryReader`]
//! Provides read-only access to event streams and snapshots. Use for queries,
//! reporting, or loading aggregates without modification.
//!
//! ### [`RepositoryWriter`]  
//! Extends [`RepositoryReader`] with write operations within a transaction boundary.
//! Required for any operation that modifies aggregate state or produces side effects.
//!
//! ### [`Repository`]
//! High-level abstraction for simple aggregate loading without explicit
//! transaction management.
//!
//! ## Usage Pattern
//!
//! ```rust,ignore
//! // Begin transaction for write operations
//! let mut transaction = repository.begin_transaction().await?;
//! let mut context = transaction.get(&aggregate_id).await?;
//! context.record_that(event)?;
//! transaction.store(&mut context).await?;
//! transaction.commit().await?;
//! ```
//!
//! For the complete event sourcing workflow, see [`crate::event`] and [`crate::aggregate`].
//!
use async_trait::async_trait;
use futures::Stream;
use std::fmt::Debug;

use crate::{
    aggregate::{Aggregate, Context},
    event::{DomainEvent, EventStoreEvent},
};

/// List of possible errors that can be returned by the [`RepositoryWriter`] trait.
///
/// Each error type represents a specific failure scenario that can occur during
/// repository operations. Understanding these errors is crucial for implementing
/// proper error handling and recovery strategies.
#[derive(Debug, thiserror::Error)]
pub enum RepositoryError<E, EventId, DE> {
    /// This error is returned by [`RepositoryWriter`] methods when the
    /// desired [`Aggregate`] could not be found in the data store.
    #[error("Aggregate was not found")]
    AggregateNotFound,

    /// This error is returned by [`RepositoryWriter`] methods when
    /// the desired [`Aggregate`] returns an error while applying a Domain Event.
    ///
    /// ## When this occurs:
    /// - Event contains corrupted or invalid data
    /// - Event violates business rules or invariants
    /// - Schema evolution issues where old events can't be applied to new aggregates
    /// - Serialization/deserialization failures
    #[error("Failed to apply events to aggregate from event stream. Event Id: {0} caused: {1}")]
    Apply(EventId, #[source] E),

    /// This error is returned when [`RepositoryWriter`] methods return
    /// an unexpected error while streaming back the Aggregate's Event Stream.
    ///
    /// ## When this occurs:
    /// - Database connection failures
    /// - Network connectivity issues
    /// - Serialization/deserialization errors
    /// - Database query failures
    /// - Transaction isolation issues
    #[error("Event store failed while streaming events: {0}")]
    Repository(#[from] DE),
}

/// A snapshot of the [`Aggregate`] that is persisted in the db.
#[derive(Debug, Clone)]
pub struct Snapshot<T>
where
    T: Aggregate,
{
    pub aggregate: T,
    pub version: u64,
    pub snapshot_version: u64,
}

/// A RepositoryReader provides read-only access to aggregate data.
///
/// This trait defines the interface for reading events and snapshots from the event store.
/// It can be implemented by both transactional and non-transactional repository
/// implementations to enable efficient read operations without requiring write access.
#[async_trait]
pub trait RepositoryReader<T: Aggregate> {
    /// The error type returned by the Store during repository operations.
    type DbError;

    /// Opens an Event Stream, effectively streaming all Domain Events
    /// of an Event Stream back in the application from a specific version.
    #[doc(hidden)]
    fn stream_from(
        &mut self,
        id: &T::AggregateId,
        version: u64,
    ) -> impl Stream<Item = Result<EventStoreEvent<T::DomainEvent>, Self::DbError>>;

    /// Get a specific event from the event store by its ID.
    #[doc(hidden)]
    async fn get_event(
        &mut self,
        aggregate_id: &T::AggregateId,
        event_id: &<<T as Aggregate>::DomainEvent as DomainEvent>::EventId,
    ) -> Result<Option<EventStoreEvent<T::DomainEvent>>, Self::DbError>;

    /// Retrieves the latest snapshot of the Aggregate from the Event Store.
    /// This method must check that the snapshot version matches the expected
    /// [`Aggregate::SNAPSHOT_VERSION`] to ensure compatibility.
    #[doc(hidden)]
    async fn get_snapshot(
        &mut self,
        id: &T::AggregateId,
    ) -> Result<Option<Snapshot<T>>, Self::DbError>;
}

/// A RepositoryTransaction provides transactional access to aggregate persistence.
///
/// This trait extends [`RepositoryReader`] to provide write operations within a transaction
/// boundary. All write operations in event sourcing must be performed within a transaction
/// to ensure consistency between events, snapshots, and side effects.
#[async_trait]
pub trait RepositoryWriter<T: Aggregate>: RepositoryReader<T> {
    /// Appends new Domain Events to the specified Event Stream.
    ///
    /// Returns a list of the Domain Event Ids that were successfully stored.
    #[doc(hidden)]
    async fn store_events(
        &mut self,
        id: &T::AggregateId,
        events: Vec<EventStoreEvent<T::DomainEvent>>,
    ) -> Result<Vec<<<T as Aggregate>::DomainEvent as DomainEvent>::EventId>, Self::DbError>;

    /// Stores a snapshot of the aggregate state to optimize future loading.
    #[doc(hidden)]
    async fn store_snapshot(&mut self, snapshot: Snapshot<T>) -> Result<(), Self::DbError>;

    /// Insert side effects into the repository
    #[doc(hidden)]
    async fn store_side_effects(
        &mut self,
        side_effects: Vec<T::SideEffect>,
    ) -> Result<(), Self::DbError>;
}

/// A Repository provides high-level operations for loading
/// [`Aggregate`] instances without requiring explicit transaction management.
///
/// This trait is intended for simpler use cases where automatic transaction
/// handling is preferred over manual transaction control.
#[async_trait]
pub trait Repository<T: Aggregate> {
    /// The error type returned by the Repository during operations.
    type Error;

    /// Loads an aggregate from the repository by its ID.
    ///
    /// This method automatically handles transaction management and will
    /// load the latest state of the aggregate by replaying its event stream.
    /// If a snapshot is available, it will be used to optimize the loading process.
    ///
    /// # Errors
    ///
    /// Returns repository-specific errors which may include:
    /// - Aggregate not found errors
    /// - Database connection errors  
    /// - Event application errors
    async fn load(&self, aggregate_id: &T::AggregateId) -> Result<Context<T>, Self::Error>;
}
