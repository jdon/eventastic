use futures::TryStreamExt;

use crate::repository::{RepositoryError, RepositoryReader, RepositoryWriter, Snapshot};
use crate::{
    aggregate::Aggregate,
    event::{DomainEvent, EventStoreEvent},
};
use std::fmt::Debug;

/// A context object that should be used by the Aggregate [`Root`] methods to
/// access the [`Aggregate`] state and to record new Domain Events.
///
/// The Context wraps an aggregate and tracks:
/// - The current aggregate state
/// - The aggregate version for optimistic concurrency control
/// - Uncommitted events that haven't been persisted yet
/// - Uncommitted side effects to be processed
#[derive(Debug, Clone)]
#[must_use]
pub struct Context<T>
where
    T: Aggregate,
{
    aggregate: T,
    version: u64,
    uncommitted_events: Vec<EventStoreEvent<T::DomainEvent>>,
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

    /// Returns the current version for the [`Aggregate`].
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Returns the current snapshot version for the [`Aggregate`].
    pub fn snapshot_version(&self) -> u64 {
        T::SNAPSHOT_VERSION
    }

    /// Returns the list of uncommitted, recorded Domain Events from the [`Context`]
    /// and resets the internal list to its default value.
    #[doc(hidden)]
    pub fn take_uncommitted_events(&mut self) -> Vec<EventStoreEvent<T::DomainEvent>> {
        std::mem::take(&mut self.uncommitted_events)
    }

    /// Returns the list of uncommitted, recorded [`Aggregate::SideEffect`]s from the [`Context`]
    /// and resets the internal list to its default value.
    #[doc(hidden)]
    pub fn take_uncommitted_side_effects(&mut self) -> Vec<T::SideEffect> {
        std::mem::take(&mut self.uncommitted_side_effects)
    }

    /// Creates a new [`Context`] instance from a Domain Event
    /// while rehydrating an [`Aggregate`].
    #[doc(hidden)]
    pub fn rehydrate_from(
        event: &EventStoreEvent<T::DomainEvent>,
    ) -> Result<Context<T>, T::ApplyError> {
        Ok(Context {
            version: event.version,
            aggregate: T::apply_new(&event.event)?,
            uncommitted_events: Vec::default(),
            uncommitted_side_effects: Vec::default(),
        })
    }

    /// Applies a new Domain Event to the [`Context`] while rehydrating
    /// an [`Aggregate`].
    #[doc(hidden)]
    pub fn apply_rehydrated_event(
        mut self,
        event: &EventStoreEvent<T::DomainEvent>,
    ) -> Result<Context<T>, T::ApplyError> {
        self.version += 1;
        debug_assert!(self.version == event.version);
        self.aggregate.apply(&event.event)?;
        Ok(self)
    }

    pub(crate) fn record_new(event: T::DomainEvent) -> Result<Context<T>, T::ApplyError> {
        let aggregate = T::apply_new(&event)?;

        // Create the event store event first
        let event_store_event = EventStoreEvent {
            id: event.id().clone(),
            version: 0,
            event,
        };

        // Get side effects (if any)
        let uncommitted_side_effects = aggregate
            .side_effects(&event_store_event.event)
            .unwrap_or_default();

        // Create the context
        Ok(Context {
            version: 0,
            aggregate,
            uncommitted_events: vec![event_store_event],
            uncommitted_side_effects,
        })
    }
    /// Returns read access to the [`Aggregate`] state.
    pub fn state(&self) -> &T {
        &self.aggregate
    }

    /// Records a change to the [`Aggregate`] [`Root`], expressed by the specified
    /// Domain Event.
    pub fn record_that(&mut self, event: T::DomainEvent) -> Result<(), T::ApplyError> {
        self.aggregate.apply(&event)?;
        self.version += 1;

        if let Some(side_effects) = self.aggregate.side_effects(&event) {
            self.uncommitted_side_effects.extend(side_effects);
        }

        self.uncommitted_events.push(EventStoreEvent {
            id: event.id().clone(),
            version: self.version,
            event,
        });

        Ok(())
    }

    /// Saves the aggregate and its uncommitted events to the repository.
    /// This method handles concurrency control and idempotency checks.
    pub async fn save<R>(&mut self, transaction: &mut R) -> Result<(), SaveError<T, R::DbError>>
    where
        R: RepositoryWriter<T>,
    {
        let events_to_commit = self.take_uncommitted_events();

        if events_to_commit.is_empty() {
            return Ok(());
        }

        let side_effects_to_commit = self.take_uncommitted_side_effects();

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
        let inserted_event_ids = transaction
            .store_events(aggregate_id, events_to_commit.clone())
            .await
            .map_err(SaveError::Repository)?;

        if inserted_event_ids.len() != events_to_commit.len() {
            // We failed to insert one or more of the events, it's possible that the events have already been inserted
            // If that's the case, we need to check if the previously inserted events are the same as the ones we have
            for event in events_to_commit {
                if !inserted_event_ids.contains(&event.id) {
                    if let Some(saved_event) = transaction
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

        transaction
            .store_snapshot(snapshot)
            .await
            .map_err(SaveError::Repository)?;

        transaction
            .store_side_effects(side_effects_to_commit)
            .await?;

        Ok(())
    }

    /// Loads an aggregate from the repository by replaying its event stream.
    ///
    /// This method first attempts to load a snapshot if available, then replays
    /// any events that occurred after the snapshot to reconstruct the current state.
    pub async fn load<R>(
        reader: &mut R,
        aggregate_id: &T::AggregateId,
    ) -> Result<
        Context<T>,
        RepositoryError<
            T::ApplyError,
            <<T as Aggregate>::DomainEvent as DomainEvent>::EventId,
            R::DbError,
        >,
    >
    where
        R: RepositoryReader<T>,
    {
        let snapshot = reader.get_snapshot(aggregate_id).await?;

        let (context, version) = snapshot
            .map(|s| {
                (
                    Some(Context {
                        aggregate: s.aggregate,
                        version: s.version,
                        uncommitted_events: Vec::new(),
                        uncommitted_side_effects: Vec::new(),
                    }),
                    s.version + 1, // Start from next event
                )
            })
            .unwrap_or((None, 0));

        let ctx = reader
            .stream_from(aggregate_id, version)
            .map_err(RepositoryError::Repository)
            .try_fold(context, |ctx: Option<Context<T>>, event| async move {
                match ctx {
                    None => Context::rehydrate_from(&event).map(Some),
                    Some(ctx) => ctx.apply_rehydrated_event(&event).map(Some),
                }
                .map_err(|e| RepositoryError::Apply(event.id, e))
            })
            .await?;

        ctx.ok_or(RepositoryError::AggregateNotFound)
    }

    /// Regenerates side effects for a specific event by loading the aggregate
    /// up to and including that event.
    ///
    /// This method reconstructs the aggregate state at the point when the specified
    /// event was applied, then calls the aggregate's `side_effects` method to
    /// regenerate any side effects that should have been produced.
    ///
    /// This is useful for:
    /// - Recovering from side effect processing failures
    /// - Debugging side effect generation
    /// - Replaying side effects for specific events
    ///
    /// # Returns
    ///
    /// - `Ok(Some(Vec<T::SideEffect>))` if side effects were generated
    /// - `Ok(None)` if no side effects were generated or the event wasn't found
    /// - `Err` if there was an error loading the aggregate or applying events
    pub async fn regenerate_side_effects<R>(
        reader: &mut R,
        aggregate_id: &T::AggregateId,
        event_id: &<<T as Aggregate>::DomainEvent as DomainEvent>::EventId,
    ) -> Result<
        Option<Vec<T::SideEffect>>,
        RepositoryError<
            T::ApplyError,
            <<T as Aggregate>::DomainEvent as DomainEvent>::EventId,
            R::DbError,
        >,
    >
    where
        R: RepositoryReader<T>,
    {
        use futures::{StreamExt, TryStreamExt};

        // Get the specific event first to ensure it exists
        let event = reader.get_event(aggregate_id, event_id).await?;

        let Some(target_event) = event else {
            return Ok(None);
        };

        // Stream all events from the beginning up to and including the target version
        let context = reader
            .stream_from(aggregate_id, 0)
            .take_while(|result| {
                futures::future::ready(match result {
                    Ok(e) => e.version <= target_event.version,
                    Err(_) => true,
                })
            })
            .map_err(RepositoryError::Repository)
            .try_fold(None, |ctx: Option<Context<T>>, event| async move {
                match ctx {
                    None => Context::rehydrate_from(&event).map(Some),
                    Some(c) => c.apply_rehydrated_event(&event).map(Some),
                }
                .map_err(|e| RepositoryError::Apply(event.id, e))
            })
            .await?;

        let Some(aggregate) = context else {
            return Ok(None);
        };

        // Generate side effects with aggregate in the correct state
        Ok(aggregate.aggregate.side_effects(&target_event.event))
    }
}

/// List of possible errors that can be returned when recording events using [`Context::save`].
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

    /// This error is returned when the Repository fails to insert the event
    /// because the version already exists, indicating a concurrent modification.
    #[error("Optimistic Concurrency Error Version {1} of aggregate {0:?} already exists")]
    OptimisticConcurrency(T::AggregateId, u64),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::*;

    // Tests that Context::record_new creates a new context with proper initial state
    #[test]
    fn test_context_record_new() {
        let reset_event = create_reset_event("reset-1", "calc-1");
        let context = TestCounter::record_new(reset_event.clone()).unwrap();

        assert_eq!(context.state().id, "calc-1");
        assert_eq!(context.state().result, 0);
        assert_eq!(context.version(), 0);
        assert_eq!(context.aggregate_id(), "calc-1");
        assert_eq!(context.snapshot_version(), TestCounter::SNAPSHOT_VERSION);
    }

    // Tests that Context::record_that applies events and increments version correctly
    #[test]
    fn test_context_record_that() {
        let reset_event = create_reset_event("reset-1", "calc-1");
        let mut context = TestCounter::record_new(reset_event).unwrap();

        // Record addition event
        let add_event = create_add_event("add-1", 15);
        context.record_that(add_event).unwrap();

        assert_eq!(context.state().result, 15);
        assert_eq!(context.version(), 1);

        // Record subtraction event
        let subtract_event = create_subtract_event("sub-1", 5);
        context.record_that(subtract_event).unwrap();

        assert_eq!(context.state().result, 10);
        assert_eq!(context.version(), 2);
    }

    // Tests that Context::record_that properly handles and propagates aggregate errors
    #[test]
    fn test_context_record_that_error() {
        let reset_event = create_reset_event("reset-1", "calc-1");
        let mut context = TestCounter::record_new(reset_event).unwrap();

        // Try to record invalid event
        let multiply_zero_event = create_multiply_event("mul-zero", 0);
        let result = context.record_that(multiply_zero_event);
        assert!(matches!(result, Err(TestError::DivisionByZero)));

        // State should remain unchanged
        assert_eq!(context.state().result, 0);
        assert_eq!(context.version(), 0);
    }

    // Tests that Context correctly tracks version increments across multiple events
    #[test]
    fn test_context_version_tracking() {
        let reset_event = create_reset_event("reset-1", "calc-1");
        let mut context = TestCounter::record_new(reset_event).unwrap();
        assert_eq!(context.version(), 0);

        for i in 1..=5 {
            let add_event = create_add_event(&format!("add-{}", i), i);
            context.record_that(add_event).unwrap();
            assert_eq!(context.version(), i as u64);
        }
    }

    // Tests that Context::take_uncommitted_events returns events and empties the list
    #[test]
    fn test_context_uncommitted_events() {
        let reset_event = create_reset_event("reset-1", "calc-1");
        let mut context = TestCounter::record_new(reset_event).unwrap();

        let add_event = create_add_event("add-1", 10);
        context.record_that(add_event).unwrap();

        let events = context.take_uncommitted_events();
        assert_eq!(events.len(), 2); // Reset + Add

        // After taking, should be empty
        let empty_events = context.take_uncommitted_events();
        assert_eq!(empty_events.len(), 0);
    }

    // Tests that Context::take_uncommitted_side_effects returns side effects and empties the list
    #[test]
    fn test_context_side_effects() {
        let reset_event = create_reset_event("reset-1", "calc-1");
        let mut context = TestCounter::record_new(reset_event).unwrap();

        let add_event = create_add_event("add-1", 5);
        context.record_that(add_event).unwrap();

        let side_effects = context.take_uncommitted_side_effects();
        assert_eq!(side_effects.len(), 3); // 2 from reset + 1 from add

        // After taking, should be empty
        let empty_side_effects = context.take_uncommitted_side_effects();
        assert_eq!(empty_side_effects.len(), 0);
    }

    // Tests that Context::state provides read-only access to the aggregate state
    #[test]
    fn test_context_state_access() {
        let reset_event = create_reset_event("reset-1", "calc-1");
        let context = TestCounter::record_new(reset_event).unwrap();

        // Test state() method
        let state = context.state();
        assert_eq!(state.id, "calc-1");
        assert_eq!(state.result, 0);
        assert_eq!(state.operations_count, 0);
    }

    // Tests that Context::snapshot_version returns the aggregate's SNAPSHOT_VERSION
    #[test]
    fn test_context_snapshot_version() {
        let reset_event = create_reset_event("reset-1", "calc-1");
        let context = TestCounter::record_new(reset_event).unwrap();

        // Should return the aggregate's SNAPSHOT_VERSION constant
        assert_eq!(context.snapshot_version(), TestCounter::SNAPSHOT_VERSION);
        assert_eq!(context.snapshot_version(), 1);

        // Test that snapshot_version is correctly implemented
        let version = context.snapshot_version();
        assert_eq!(version, 1u64);
        assert_eq!(version, TestCounter::SNAPSHOT_VERSION);
    }

    // Tests Context::rehydrate_from function for creating context from events
    #[test]
    fn test_context_rehydrate_from() {
        use crate::event::EventStoreEvent;

        let reset_event = create_reset_event("reset-1", "calc-1");
        let store_event = EventStoreEvent::new("reset-1".to_string(), 0, reset_event);

        let context = Context::<TestCounter>::rehydrate_from(&store_event).unwrap();

        assert_eq!(context.state().id, "calc-1");
        assert_eq!(context.state().result, 0);
        assert_eq!(context.version(), 0);
        assert!(context.uncommitted_events.is_empty());
        assert!(context.uncommitted_side_effects.is_empty());
    }

    // Tests Context::apply_rehydrated_event for applying events during replay
    #[test]
    fn test_context_apply_rehydrated_event() {
        use crate::event::EventStoreEvent;

        let reset_event = create_reset_event("reset-1", "calc-1");
        let store_event = EventStoreEvent::new("reset-1".to_string(), 0, reset_event);
        let context = Context::<TestCounter>::rehydrate_from(&store_event).unwrap();

        let add_event = create_add_event("add-1", 10);
        let add_store_event = EventStoreEvent::new("add-1".to_string(), 1, add_event);

        let updated_context = context.apply_rehydrated_event(&add_store_event).unwrap();

        assert_eq!(updated_context.state().result, 10);
        assert_eq!(updated_context.version(), 1);
        assert!(updated_context.uncommitted_events.is_empty());
        assert!(updated_context.uncommitted_side_effects.is_empty());
    }

    // Tests Context::apply_rehydrated_event error handling
    #[test]
    fn test_context_apply_rehydrated_event_error() {
        use crate::event::EventStoreEvent;

        let reset_event = create_reset_event("reset-1", "calc-1");
        let store_event = EventStoreEvent::new("reset-1".to_string(), 0, reset_event);
        let context = Context::<TestCounter>::rehydrate_from(&store_event).unwrap();

        // Try to apply an invalid event (multiply by zero)
        let invalid_event = create_multiply_event("mul-zero", 0);
        let invalid_store_event = EventStoreEvent::new("mul-zero".to_string(), 1, invalid_event);

        let result = context.apply_rehydrated_event(&invalid_store_event);
        assert!(matches!(result, Err(TestError::DivisionByZero)));
    }

    // Tests Context::rehydrate_from error handling
    #[test]
    fn test_context_rehydrate_from_error() {
        use crate::event::EventStoreEvent;

        // Try to create context from non-creation event
        let add_event = create_add_event("add-1", 10);
        let store_event = EventStoreEvent::new("add-1".to_string(), 0, add_event);

        let result = Context::<TestCounter>::rehydrate_from(&store_event);
        assert!(matches!(result, Err(TestError::InvalidOperation)));
    }

    // Tests the record_new function that creates new contexts from events
    #[test]
    fn test_context_record_new_with_side_effects() {
        // Test that side effects are properly generated during record_new
        let reset_event = create_reset_event("reset-1", "calc-1");
        let context = Context::<TestCounter>::record_new(reset_event).unwrap();

        // Reset events generate 2 side effects
        assert_eq!(context.uncommitted_side_effects.len(), 2);

        // Check the side effects content
        let side_effects = &context.uncommitted_side_effects;
        assert!(side_effects.iter().any(|se| matches!(
            se,
            crate::test_fixtures::TestSideEffect::LogOperation { .. }
        )));
        assert!(
            side_effects
                .iter()
                .any(|se| matches!(se, crate::test_fixtures::TestSideEffect::NotifyUser { .. }))
        );
    }

    // Tests the case where no side effects are generated
    #[test]
    fn test_context_record_new_no_side_effects() {
        use crate::test_fixtures::*;

        // Create an aggregate that doesn't generate side effects from initial event
        #[derive(Clone, Debug, PartialEq, Eq)]
        struct SimpleCounts {
            id: String,
            count: i32,
        }

        #[derive(Clone, Debug, PartialEq, Eq, Hash)]
        enum SimpleEvent {
            Created { event_id: String, id: String },
        }

        impl crate::event::DomainEvent for SimpleEvent {
            type EventId = String;
            fn id(&self) -> &Self::EventId {
                match self {
                    SimpleEvent::Created { event_id, .. } => event_id,
                }
            }
        }

        impl crate::aggregate::Aggregate for SimpleCounts {
            const SNAPSHOT_VERSION: u64 = 1;
            type AggregateId = String;
            type DomainEvent = SimpleEvent;
            type ApplyError = String;
            type SideEffect = TestSideEffect;

            fn aggregate_id(&self) -> &Self::AggregateId {
                &self.id
            }

            fn apply_new(event: &Self::DomainEvent) -> Result<Self, Self::ApplyError> {
                match event {
                    SimpleEvent::Created { id, .. } => Ok(SimpleCounts {
                        id: id.clone(),
                        count: 0,
                    }),
                }
            }

            fn apply(&mut self, _event: &Self::DomainEvent) -> Result<(), Self::ApplyError> {
                Ok(())
            }

            fn side_effects(&self, _event: &Self::DomainEvent) -> Option<Vec<Self::SideEffect>> {
                None // No side effects
            }
        }

        let create_event = SimpleEvent::Created {
            event_id: "evt-1".to_string(),
            id: "simple-1".to_string(),
        };

        let context = Context::<SimpleCounts>::record_new(create_event).unwrap();

        // Should have no side effects
        assert_eq!(context.uncommitted_side_effects.len(), 0);
        assert_eq!(context.version(), 0);
        assert_eq!(context.state().id, "simple-1");

        // Test additional method calls
        assert_eq!(context.aggregate_id(), &"simple-1".to_string());
        let state_ref = context.state();
        assert_eq!(state_ref.count, 0);
    }
}
