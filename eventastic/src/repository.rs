use async_trait::async_trait;
use futures::TryStreamExt;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fmt::Debug;

use crate::{
    aggregate::{Aggregate, Context},
    event::{EventStoreEvent, Stream},
};

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
    T::AggregateId: Clone + Send + Sync,
    T::ApplyError: Debug,
    Self: Sized + Send + Sync,
{
    /// The error type returned by the Store during a [`RepositoryTransaction::stream`] and [`RepositoryTransaction::append`] call.
    type DbError: Send + Sync;

    /// Opens an Event Stream, effectively streaming all Domain Events
    /// of an Event Stream back in the application.
    #[doc(hidden)]
    fn stream(
        &mut self,
        id: &T::AggregateId,
    ) -> Stream<T::DomainEventId, T::DomainEvent, Self::DbError>;

    /// Opens an Event Stream, effectively streaming all Domain Events
    /// of an Event Stream back in the application from a specific version.
    #[doc(hidden)]
    fn stream_from(
        &mut self,
        id: &T::AggregateId,
        version: u64,
    ) -> Stream<T::DomainEventId, T::DomainEvent, Self::DbError>;

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

    /// Appends a new Domain Events to the specified Event Stream.
    ///
    /// The result of this operation is the new [Version] of the Event Stream
    /// with the specified Domain Events added to it.
    #[doc(hidden)]
    async fn append(
        &mut self,
        id: &T::AggregateId,
        events: Vec<EventStoreEvent<T::DomainEventId, T::DomainEvent>>,
    ) -> Result<(), Self::DbError>;

    #[doc(hidden)]
    async fn get_snapshot(&mut self, id: &T::AggregateId) -> Option<Snapshot<T>>
    where
        T: DeserializeOwned;

    #[doc(hidden)]
    async fn store_snapshot(&mut self, snapshot: Snapshot<T>) -> Result<(), Self::DbError>
    where
        T: Serialize;

    /// Loads an Aggregate Root instance from the data store,
    /// referenced by its unique identifier.
    async fn get(
        &mut self,
        id: &T::AggregateId,
    ) -> Result<Context<T>, RepositoryError<T::ApplyError, T::DomainEventId, Self::DbError>>
    where
        T: DeserializeOwned,
    {
        let snapshot = self.get_snapshot(id).await;

        let (context, version) = if let Some(snapshot) = snapshot {
            if snapshot.snapshot_version == T::SNAPSHOT_VERSION {
                // Snapshot is valid so return it
                let context: Context<T> = snapshot.into();
                // We want to get the next event in the stream
                let version = context.version() + 1;
                (Some(context), version)
            } else {
                (None, 0)
            }
        } else {
            (None, 0)
        };

        let ctx = self
            .stream_from(id, version)
            .map_err(RepositoryError::Repository)
            .try_fold(context, |ctx: Option<Context<T>>, event| async move {
                let new_ctx_result = match ctx {
                    None => Context::rehydrate_from(&event),
                    Some(ctx) => ctx.apply_rehydrated_event(&event),
                };

                let new_ctx = new_ctx_result.map_err(|e| RepositoryError::Apply(event.id, e))?;

                Ok(Some(new_ctx))
            })
            .await?;

        ctx.ok_or(RepositoryError::AggregateNotFound)
    }

    /// Stores a new version of an Aggregate Root instance to the data store.
    async fn store(
        &mut self,
        root: &mut Context<T>,
    ) -> Result<(), RepositoryError<T::ApplyError, T::DomainEventId, Self::DbError>>
    where
        T: Serialize,
        T::SideEffect: Serialize,
    {
        let events_to_commit = root.take_uncommitted_events();

        if events_to_commit.is_empty() {
            return Ok(());
        }

        let side_effects_to_commit = root.take_uncommitted_side_effects();

        let aggregate_id = root.aggregate_id();

        let snapshot_version = root.snapshot_version();
        let snapshot_to_store = root.state();

        let snapshot = Snapshot {
            snapshot_version,
            aggregate: snapshot_to_store.clone(),
            version: root.version(),
        };

        self.append(aggregate_id, events_to_commit)
            .await
            .map_err(RepositoryError::Repository)?;

        self.store_snapshot(snapshot)
            .await
            .map_err(RepositoryError::Repository)?;

        self.insert_side_effects(side_effects_to_commit).await?;

        Ok(())
    }

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
