use std::fmt::Debug;

use async_trait::async_trait;
use futures::TryStreamExt;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    aggregate::{Aggregate, Context},
    event::{EventStoreEvent, Stream},
    SideEffectHandler,
};

/// List of possible errors that can be returned by the [`Repository`] / [`RepositoryTransaction`] trait.
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
    pub version: u32,
    pub snapshot_version: u32,
}

impl<T> Snapshot<T>
where
    T: Aggregate,
{
    pub fn id(&self) -> &T::AggregateId {
        self.aggregate.aggregate_id()
    }
}

#[derive(Debug, Clone)]
pub struct OutBoxMessage<Id, T> {
    /// The id of the outbox message
    /// This can be used an idempotency id
    pub id: Id,
    /// The contents of this outbox message
    /// This is usually an enum
    pub message: T,

    retries: u32,

    /// Whether or not to requeue this item
    /// If set to false this item won't be requeued but will remain in the repository.
    pub requeue: bool,
}

impl<Id, T> OutBoxMessage<Id, T> {
    pub fn new(id: Id, message: T, retries: u32, requeue: bool) -> OutBoxMessage<Id, T> {
        OutBoxMessage {
            id,
            message,
            retries,
            requeue,
        }
    }
}

impl<Id, T> OutBoxMessage<Id, T> {
    /// The amount of times this message has been retried
    pub fn retries(&self) -> u32 {
        self.retries
    }
}

#[async_trait]
pub trait OutBoxHandler<Id, SideEffect>
where
    SideEffect: Send,
{
    /// The error type returned by the Store during a [`RepositoryTransaction::stream`] and [`RepositoryTransaction::append`] call.
    type OutBoxDbError: Send + Sync;

    /// Delete an item from the outbox.
    #[doc(hidden)]
    async fn delete_outbox_item(&mut self, outbox_id: Id) -> Result<(), Self::OutBoxDbError>;

    /// Insert or update an outbox item into the repository.
    #[doc(hidden)]
    async fn get_outbox_items(
        &mut self,
    ) -> Result<Vec<OutBoxMessage<Id, SideEffect>>, Self::OutBoxDbError>
    where
        SideEffect: DeserializeOwned;

    /// Insert or update an outbox item into the repository.
    #[doc(hidden)]
    async fn upsert_outbox_item(
        &mut self,
        outbox_item: OutBoxMessage<Id, SideEffect>,
    ) -> Result<(), Self::OutBoxDbError>
    where
        SideEffect: Serialize + 'async_trait;
}

/// A RepositoryTransaction is an object that allows to load and save
/// an [`Aggregate`] from and to a persistent data store
#[async_trait]
pub trait RepositoryTransaction<T, 'a>: Send + Sync
where
    T: Aggregate,
    T::AggregateId: Clone + Send + Sync,
    T::ApplyError: Debug,
    Self: Sized,
    Self: OutBoxHandler<T::SideEffectId, T::SideEffect, OutBoxDbError = Self::DbError>,
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

    // /// Get outbox a list of outbox items from the repository
    // /// This should generally use `FOR UPDATE SKIP LOCKED`
    // #[doc(hidden)]
    // async fn get_outbox_items(
    //     &mut self,
    // ) -> Result<Vec<OutBoxMessage<T::SideEffectId, T::SideEffect>>, Self::DbError>
    // where
    //     <T as Aggregate>::SideEffect: DeserializeOwned;

    // /// Delete an item from the outbox.
    // #[doc(hidden)]
    // async fn delete_outbox_item(&mut self, outbox_id: T::SideEffectId)
    //     -> Result<(), Self::DbError>;

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

        if let Some(snapshot) = snapshot {
            if snapshot.snapshot_version == T::SNAPSHOT_VERSION {
                // Snapshot is valid so return it
                let context: Context<T> = snapshot.into();
                return Ok(context);
            }
        }

        let ctx = self
            .stream(id)
            .map_err(RepositoryError::Repository)
            .try_fold(None, |ctx: Option<Context<T>>, event| async move {
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

        for side_effect in side_effects_to_commit {
            //todo optimize this
            self.upsert_outbox_item(OutBoxMessage {
                id: side_effect.id,
                message: side_effect.message,
                retries: 0,
                requeue: true,
            })
            .await?;
        }

        Ok(())
    }

    async fn commit(self) -> Result<(), Self::DbError>;
}

/// A Repository is an object that creates a [`RepositoryTransaction`]
#[async_trait]
pub trait Repository<'a, T, Transaction>: Send + Sync
where
    T: Aggregate,
    T::AggregateId: Clone + Send + Sync,
    T::ApplyError: Debug,
    Transaction: RepositoryTransaction<'a, T, DbError = Self::DbError>,
    T::SideEffect: DeserializeOwned + Serialize,
{
    /// The error type returned by the Store during a [`RepositoryTransaction::stream`] and [`RepositoryTransaction::append`] call.
    type DbError: Send + Sync + Debug;

    async fn begin_transaction(&'a self) -> Result<Transaction, Self::DbError>;

    /// Start the outbox.
    /// This function will run forever, so should generally be spawned as a background task
    /// .
    /// Default implementation runs every 30 seconds and handles messages in batches
    async fn start_outbox<H>(
        &'a self,
        handler: H,
        poll_interval: std::time::Duration,
    ) -> Result<(), Self::DbError>
    where
        H: SideEffectHandler<T::SideEffectId, T::SideEffect, T::SideEffectError>,
    {
        loop {
            let deadline = std::time::Instant::now() + poll_interval;

            // Errors are ignored in the default implementation as they are added to the dead box.
            let res = self.run_outbox(&handler).await;
            println!("Got res {res:?}");
            futures_time::task::sleep_until(deadline.into()).await;
        }
    }

    /// Process
    /// This function will run forever, so should generally be spawned as a background task.
    #[doc(hidden)]
    async fn run_outbox<H>(&'a self, handler: &H) -> Result<(), Self::DbError>
    where
        H: SideEffectHandler<T::SideEffectId, T::SideEffect, T::SideEffectError>,
    {
        let mut tx = self.begin_transaction().await?;

        let outbox_items = tx.get_outbox_items().await?;

        for mut item in outbox_items {
            let item_id = item.id.clone();
            match handler.handle(&item).await {
                Ok(_) => {
                    tx.delete_outbox_item(item_id).await?;
                }
                Err(_) => {
                    item.retries += 1;
                    tx.upsert_outbox_item(item).await?;
                }
            };
        }

        tx.commit().await?;
        Ok(())
    }
}
