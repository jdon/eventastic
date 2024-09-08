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

    /// Returns the list of uncommitted, recorded Domain [Event]s from the [Context]
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
    pub(crate) fn rehydrate_from(
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
    pub(crate) fn apply_rehydrated_event(
        mut self,
        event: &EventStoreEvent<T::DomainEventId, T::DomainEvent>,
    ) -> Result<Context<T>, T::ApplyError> {
        self.version += 1;
        debug_assert!(self.version == event.version);
        self.aggregate.apply(&event.event)?;
        Ok(self)
    }

    /// Checks if the event exists in the repository and that they are equal
    pub(crate) async fn check_idempotency<R>(
        &self,
        repository: &mut R,
        aggregate_id: &T::AggregateId,
        event: &T::DomainEvent,
    ) -> Result<bool, RecordError<T, <R as RepositoryTransaction<T>>::DbError>>
    where
        R: RepositoryTransaction<T>,
    {
        if let Some(saved_event) = repository
            .get_event(aggregate_id, event.id())
            .await
            .map_err(RecordError::Repository)?
        {
            if saved_event.event != *event {
                return Err(RecordError::IdempotencyError(
                    saved_event.event,
                    event.clone(),
                ));
            }
            return Ok(true);
        }

        if let Some(existing_event) = self.uncommitted_events.iter().find(|e| e.id == *event.id()) {
            if existing_event.event != *event {
                return Err(RecordError::IdempotencyError(
                    existing_event.event.clone(),
                    event.clone(),
                ));
            }
            return Ok(true);
        };

        Ok(false)
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
    pub async fn record_that<R>(
        &mut self,
        repository: &mut R,
        event: T::DomainEvent,
    ) -> Result<(), RecordError<T, <R as RepositoryTransaction<T>>::DbError>>
    where
        R: RepositoryTransaction<T>,
    {
        // Check if the event is has already been applied, if so let's ignore it.
        if self
            .check_idempotency(repository, self.aggregate_id(), &event)
            .await?
        {
            return Ok(());
        }

        self.aggregate.apply(&event).map_err(RecordError::Apply)?;
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
}

/// List of possible errors that can be returned by when recording events using [`Context::record_that`]
#[derive(Debug, thiserror::Error)]
pub enum RecordError<T, DE>
where
    T: Aggregate,
{
    /// The [Event] failed to be applied to the [Aggregate].
    /// This usually implies that the event is invalid for given state of the aggregate.
    #[error("Failed to rehydrate aggregate from event stream. {0:?}")]
    Apply(T::ApplyError),

    /// This error is returned when the event in the repository with the same ID
    /// doesn't have the same content.
    #[error("Idempotency Error. Saved event {0:?} does not equal {1:?}")]
    IdempotencyError(T::DomainEvent, T::DomainEvent),

    /// This error is returned when the Repository returns
    /// an unexpected error while streaming back the Aggregate's Event Stream.
    #[error("Event store failed while streaming events: {0}")]
    Repository(#[from] DE),
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
