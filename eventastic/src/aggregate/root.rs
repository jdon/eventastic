use serde::Serialize;

use crate::repository::{RepositoryTransaction, Snapshot};
use crate::{
    aggregate::Aggregate,
    event::{Event, EventStoreEvent},
};
use std::fmt::Debug;

/// A context object that should be used by the Aggregate [Root] methods to
/// access the [Aggregate] state and to record new Domain Events.
#[derive(Debug, Clone)]
#[must_use]
pub struct Context<T>
where
    T: Aggregate,
{
    aggregate: T,
    version: u64,
    uncommitted_events: Vec<EventStoreEvent<T::DomainEventId, T::DomainEvent>>,
    uncommitted_side_effects: Vec<T::SideEffect>,
}

impl<T> Context<T>
where
    T: Aggregate,
{
    /// Returns the unique identifier for the Aggregate instance.
    pub fn aggregate_id(&self) -> &T::AggregateId {
        self.aggregate.aggregate_id()
    }

    /// Returns the current version for the [Aggregate].
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Returns the current snapshot of the [Aggregate].
    pub fn snapshot_version(&self) -> u64 {
        T::SNAPSHOT_VERSION
    }

    /// Returns the list of uncommitted, recorded Domain [Events] from the [Context]
    /// and resets the internal list to its default value.
    #[doc(hidden)]
    pub fn take_uncommitted_events(
        &mut self,
    ) -> Vec<EventStoreEvent<T::DomainEventId, T::DomainEvent>> {
        std::mem::take(&mut self.uncommitted_events)
    }

    /// Returns the list of uncommitted, recorded [`Aggregate::SideEffect`]s from the [Context]
    /// and resets the internal list to its default value.
    #[doc(hidden)]
    pub fn take_uncommitted_side_effects(&mut self) -> Vec<T::SideEffect> {
        std::mem::take(&mut self.uncommitted_side_effects)
    }

    /// Creates a new [Context] instance from a Domain [Event]
    /// while rehydrating an [Aggregate].
    ///
    /// # Errors
    ///
    /// The method can return an error if the event to apply is unexpected
    /// given the current state of the Aggregate.
    #[doc(hidden)]
    pub fn rehydrate_from(
        event: &EventStoreEvent<T::DomainEventId, T::DomainEvent>,
    ) -> Result<Context<T>, T::ApplyError> {
        Ok(Context {
            version: event.version,
            aggregate: T::apply_new(&event.event)?,
            uncommitted_events: Vec::default(),
            uncommitted_side_effects: Vec::default(),
        })
    }

    /// Applies a new Domain [Event] to the [Context] while rehydrating
    /// an [Aggregate].
    ///
    /// # Errors
    ///
    /// The method can return an error if the event to apply is unexpected
    /// given the current state of the Aggregate.
    #[doc(hidden)]
    pub fn apply_rehydrated_event(
        mut self,
        event: &EventStoreEvent<T::DomainEventId, T::DomainEvent>,
    ) -> Result<Context<T>, T::ApplyError> {
        self.version += 1;
        debug_assert!(self.version == event.version);
        self.aggregate.apply(&event.event)?;
        Ok(self)
    }

    pub(crate) fn record_new(event: T::DomainEvent) -> Result<Context<T>, T::ApplyError> {
        let aggregate = T::apply_new(&event)?;
        let mut uncommitted_side_effects = vec![];

        if let Some(mut side_effects) = aggregate.side_effects(&event) {
            uncommitted_side_effects.append(&mut side_effects);
        }

        let root = Context {
            version: 0,
            aggregate,
            uncommitted_events: vec![EventStoreEvent {
                id: event.id().clone(),
                version: 0,
                event,
            }],
            uncommitted_side_effects,
        };

        Ok(root)
    }
    /// Returns read access to the [Aggregate] state.
    pub fn state(&self) -> &T {
        &self.aggregate
    }

    /// Records a change to the [Aggregate] [Root], expressed by the specified
    /// Domain Event.
    /// # Errors
    ///
    /// The method can return an error if the event to apply is unexpected
    /// given the current state of the Aggregate.
    pub fn record_that(&mut self, event: T::DomainEvent) -> Result<(), T::ApplyError> {
        self.aggregate.apply(&event)?;
        self.version += 1;

        if let Some(mut side_effects) = self.aggregate.side_effects(&event) {
            self.uncommitted_side_effects.append(&mut side_effects);
        }

        self.uncommitted_events.push(EventStoreEvent {
            id: event.id().clone(),
            version: self.version,
            event,
        });

        Ok(())
    }

    pub async fn save<R>(
        &mut self,
        repository: &mut R,
    ) -> Result<(), SaveError<T, <R as RepositoryTransaction<T>>::DbError>>
    where
        T: Serialize,
        T::SideEffect: Serialize,
        R: RepositoryTransaction<T>,
    {
        let events_to_commit = self.take_uncommitted_events();

        let side_effects_to_commit = self.take_uncommitted_side_effects();

        if events_to_commit.is_empty() {
            return Ok(());
        }

        let aggregate_id = self.aggregate_id();

        let snapshot_version = self.snapshot_version();
        let snapshot_to_store = self.state();

        let snapshot = Snapshot {
            snapshot_version,
            aggregate: snapshot_to_store.clone(),
            version: self.version(),
        };

        // When we insert the events, it's possible that the events have already been inserted
        // If that's the case, we need to check if the previously inserted events are the same as the ones we have
        let inserted_event_ids = repository
            .append(aggregate_id, events_to_commit.clone())
            .await
            .map_err(SaveError::Repository)?;

        if inserted_event_ids.len() != events_to_commit.len() {
            // We failed to insert one or more of the events, it's possible that the events have already been inserted
            // If that's the case, we need to check if the previously inserted events are the same as the ones we have
            for event in events_to_commit {
                if !inserted_event_ids.contains(&event.id) {
                    if let Some(saved_event) = repository
                        .get_event(aggregate_id, event.id())
                        .await
                        .map_err(SaveError::Repository)?
                    {
                        if saved_event.event != event.event {
                            return Err(SaveError::IdempotencyError(
                                saved_event.event,
                                event.event,
                            ));
                        }
                    } else {
                        // The not inserted event was not found in the event store, this happens if a different event was inserted with the same version and aggregate id
                        // This is a fatal error, so return early
                        return Err(SaveError::OptimisticConcurrency(
                            aggregate_id.clone(),
                            event.version,
                        ));
                    }
                }
            }
        }

        repository
            .store_snapshot(snapshot)
            .await
            .map_err(SaveError::Repository)?;

        repository
            .insert_side_effects(side_effects_to_commit)
            .await?;

        Ok(())
    }
}

/// List of possible errors that can be returned by when recording events using [`Context::record_that`]
#[derive(Debug, thiserror::Error)]
pub enum SaveError<T, DE>
where
    T: Aggregate,
{
    /// This error is returned when the event in the repository with the same ID
    /// doesn't have the same content.
    #[error("Idempotency Error. Saved event {0:?} does not equal {1:?}")]
    IdempotencyError(T::DomainEvent, T::DomainEvent),

    /// This error is returned when the Repository returns
    /// an unexpected error while streaming back the Aggregate's Event Stream.
    #[error("Event store failed while streaming events: {0}")]
    Repository(#[from] DE),

    /// This error is returned when the Repository returns
    /// when it fails to insert the event because the version already exists
    #[error("Optimistic Concurrency Error Version {1} of aggregate {0:?} already exists")]
    OptimisticConcurrency(T::AggregateId, u64),
}

impl<T> From<Snapshot<T>> for Context<T>
where
    T: Aggregate,
{
    fn from(value: Snapshot<T>) -> Self {
        Self {
            aggregate: value.aggregate,
            version: value.version,
            uncommitted_events: Vec::new(),
            uncommitted_side_effects: Vec::new(),
        }
    }
}

pub trait Root<T>
where
    T: Aggregate,
{
    /// Creates a new [Aggregate] [Root] instance by applying the specified
    /// Domain Event.
    /// # Errors
    ///
    /// The method can return an error if the event to apply is unexpected
    /// given the current state of the Aggregate.
    fn record_new(event: T::DomainEvent) -> Result<Context<T>, T::ApplyError> {
        Context::record_new(event)
    }
}

impl<T> Root<T> for T where T: Aggregate {}
