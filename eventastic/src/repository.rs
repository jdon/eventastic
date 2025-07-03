use async_trait::async_trait;
use futures::Stream;
use std::fmt::Debug;

use crate::{
    aggregate::{Aggregate, Context},
    event::{DomainEvent, EventStoreEvent},
};

/// List of possible errors that can be returned by the [`RepositoryTransaction`] trait.
#[derive(Debug, thiserror::Error)]
pub enum RepositoryError<E, EventId, DE> {
    /// This error is returned by [`RepositoryTransaction`] methods when the
    /// desired Aggregate could not be found in the data store.
    #[error("Aggregate was not found")]
    AggregateNotFound,

    /// This error is returned by [`RepositoryTransaction`] methods when
    /// the desired [`Aggregate`] returns an error while applying a Domain Event
    ///
    /// This usually implies the Event contains corrupted or invalid data.
    #[error("Failed to apply events to aggregate from event stream. Event Id: {0} caused: {1}")]
    Apply(EventId, #[source] E),

    /// This error is returned when [`RepositoryTransaction`] methods return
    /// an unexpected error while streaming back the Aggregate's Event Stream.
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
/// This trait can be implemented by both transactional and non-transactional
/// repository implementations to enable efficient read operations.
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

/// A RepositoryTransaction is an object that allows to load and save
/// an [`Aggregate`] from and to a persistent data store
#[async_trait]
pub trait RepositoryTransaction<T: Aggregate>: RepositoryReader<T> {
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
