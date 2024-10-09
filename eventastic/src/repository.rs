use async_trait::async_trait;
use futures::Stream;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fmt::Debug;

use crate::{aggregate::Aggregate, event::EventStoreEvent};

/// List of possible errors that can be returned by the [`RepositoryTransaction`] trait.
#[derive(Debug, thiserror::Error)]
pub enum RepositoryError<E, EventId, DE> {
    /// This error is returned by [`RepositoryTransaction::get`] when the
    /// desired Aggregate could not be found in the data store.
    #[error("Aggregate was not found")]
    AggregateNotFound,

    /// This error is returned by [`RepositoryTransaction::get`] when
    /// the desired [Aggregate] returns an error while applying a Domain Event
    ///
    /// This usually implies the Event contains corrupted or invalid data.
    #[error("Failed to apply events to aggregate from event stream. Event Id: {0} caused: {1}")]
    Apply(EventId, #[source] E),

    /// This error is returned when the [`RepositoryTransaction::get`] returns
    /// an unexpected error while streaming back the Aggregate's Event Stream.
    #[error("Event store failed while streaming events: {0}")]
    Repository(#[from] DE),
}

/// A snap of the [`Aggregate`] that is persisted in the db.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Snapshot<T>
where
    T: Aggregate,
{
    pub aggregate: T,
    pub version: u64,
    pub snapshot_version: u64,
}

impl<T> Snapshot<T>
where
    T: Aggregate,
{
    pub fn id(&self) -> &T::AggregateId {
        self.aggregate.aggregate_id()
    }
}

/// A RepositoryTransaction is an object that allows to load and save
/// an [`Aggregate`] from and to a persistent data store
#[async_trait]
pub trait RepositoryTransaction<T>
where
    T: Aggregate,
    T::AggregateId: Clone,
    T::ApplyError: Debug,
    Self: Sized,
{
    /// The error type returned by the Store during a [`RepositoryTransaction::stream`] and [`RepositoryTransaction::append`] call.
    type DbError;

    /// Opens an Event Stream, effectively streaming all Domain Events
    /// of an Event Stream back in the application.
    #[doc(hidden)]
    fn stream(
        &mut self,
        id: &T::AggregateId,
    ) -> impl Stream<
        Item = Result<
            EventStoreEvent<T::DomainEventId, <T as Aggregate>::DomainEvent>,
            Self::DbError,
        >,
    >;

    /// Opens an Event Stream, effectively streaming all Domain Events
    /// of an Event Stream back in the application from a specific version.
    #[doc(hidden)]
    fn stream_from(
        &mut self,
        id: &T::AggregateId,
        version: u64,
    ) -> impl Stream<
        Item = Result<
            EventStoreEvent<T::DomainEventId, <T as Aggregate>::DomainEvent>,
            Self::DbError,
        >,
    >;

    // Get a specific event from the event store.
    #[doc(hidden)]
    async fn get_event(
        &mut self,
        aggregate_id: &T::AggregateId,
        event_id: &T::DomainEventId,
    ) -> Result<
        Option<EventStoreEvent<T::DomainEventId, <T as Aggregate>::DomainEvent>>,
        Self::DbError,
    >;

    /// Appends new Domain Events to the specified Event Stream.
    ///
    /// Returns a list of the Domain Event Ids that were successfully appended.
    /// If
    #[doc(hidden)]
    async fn append(
        &mut self,
        id: &T::AggregateId,
        events: Vec<EventStoreEvent<T::DomainEventId, T::DomainEvent>>,
    ) -> Result<Vec<T::DomainEventId>, Self::DbError>;

    #[doc(hidden)]
    async fn get_snapshot(&mut self, id: &T::AggregateId) -> Option<Snapshot<T>>
    where
        T: DeserializeOwned;

    #[doc(hidden)]
    async fn store_snapshot(&mut self, snapshot: Snapshot<T>) -> Result<(), Self::DbError>
    where
        T: Serialize;

    /// Insert side effects in to the repository
    #[doc(hidden)]
    async fn insert_side_effects(
        &mut self,
        outbox_item: Vec<T::SideEffect>,
    ) -> Result<(), Self::DbError>
    where
        T::SideEffect: Serialize;

    async fn commit(self) -> Result<(), Self::DbError>;
}
