//! In-memory repository implementation for testing and development.
//!
//! This module provides an in-memory implementation that can be used for testing,
//! development, and scenarios where you don't need persistent storage.

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::{Stream, stream};

use crate::{
    aggregate::{Aggregate, Context, SaveError},
    event::{DomainEvent, EventStoreEvent},
    repository::{Repository, RepositoryReader, RepositoryWriter, Snapshot},
};

/// Error type for in-memory repository operations.
#[derive(Debug, thiserror::Error)]
pub enum InMemoryError {
    #[error("Aggregate not found")]
    AggregateNotFound,
    #[error("Event not found")]
    EventNotFound,
    #[error("Version conflict: expected {expected}, got {actual}")]
    VersionConflict { expected: u64, actual: u64 },
    #[error("Event already exists with different content")]
    EventExists,
}

/// In-memory storage for events and snapshots.
#[derive(Debug, Clone)]
struct InMemoryStorage<T: Aggregate>
where
    T::AggregateId: Hash + Eq,
    <<T as Aggregate>::DomainEvent as DomainEvent>::EventId: Hash + Eq,
    T::SideEffect: Debug + Clone,
{
    /// Events stored by aggregate ID, then by version
    events: HashMap<T::AggregateId, HashMap<u64, EventStoreEvent<T::DomainEvent>>>,
    /// Events indexed by event ID for quick lookup
    events_by_id:
        HashMap<<<T as Aggregate>::DomainEvent as DomainEvent>::EventId, (T::AggregateId, u64)>,
    /// Snapshots stored by aggregate ID
    snapshots: HashMap<T::AggregateId, Snapshot<T>>,
    /// Side effects storage
    side_effects: Vec<T::SideEffect>,
}

impl<T: Aggregate> Default for InMemoryStorage<T>
where
    T::AggregateId: Hash + Eq,
    <<T as Aggregate>::DomainEvent as DomainEvent>::EventId: Hash + Eq,
    T::SideEffect: Debug + Clone,
{
    fn default() -> Self {
        Self {
            events: HashMap::new(),
            events_by_id: HashMap::new(),
            snapshots: HashMap::new(),
            side_effects: Vec::new(),
        }
    }
}

/// An in-memory repository implementation that stores events and snapshots in memory.
///
/// This repository is useful for testing, development, and scenarios where
/// persistent storage is not required. All data is lost when the repository
/// is dropped.
///
/// # Example
///
/// ```rust
/// use eventastic::memory::InMemoryRepository;
/// use eventastic::aggregate::{Aggregate, Root, SideEffect};
/// use eventastic::event::DomainEvent;
///
/// // Define a simple aggregate for demonstration
/// #[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// struct Counter {
///     id: String,
///     count: i32,
/// }
///
/// #[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// enum CounterEvent {
///     Created { event_id: String, initial_count: i32 },
///     Incremented { event_id: String, amount: i32 },
/// }
///
/// impl DomainEvent for CounterEvent {
///     type EventId = String;
///     fn id(&self) -> &Self::EventId {
///         match self {
///             CounterEvent::Created { event_id, .. } => event_id,
///             CounterEvent::Incremented { event_id, .. } => event_id,
///         }
///     }
/// }
///
/// #[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// struct CounterSideEffect {
///     id: String,
///     message: String,
/// }
///
/// impl SideEffect for CounterSideEffect {
///     type SideEffectId = String;
///     fn id(&self) -> &Self::SideEffectId {
///         &self.id
///     }
/// }
///
/// impl Aggregate for Counter {
///     const SNAPSHOT_VERSION: u64 = 1;
///     type AggregateId = String;
///     type DomainEvent = CounterEvent;
///     type ApplyError = String;
///     type SideEffect = CounterSideEffect;
///
///     fn aggregate_id(&self) -> &Self::AggregateId {
///         &self.id
///     }
///
///     fn apply_new(event: &Self::DomainEvent) -> Result<Self, Self::ApplyError> {
///         match event {
///             CounterEvent::Created { initial_count, .. } => Ok(Counter {
///                 id: "counter-1".to_string(),
///                 count: *initial_count,
///             }),
///             _ => Err("Counter must start with Created event".to_string()),
///         }
///     }
///
///     fn apply(&mut self, event: &Self::DomainEvent) -> Result<(), Self::ApplyError> {
///         match event {
///             CounterEvent::Created { .. } => Err("Counter already exists".to_string()),
///             CounterEvent::Incremented { amount, .. } => {
///                 self.count += amount;
///                 Ok(())
///             }
///         }
///     }
///
///     fn side_effects(&self, _event: &Self::DomainEvent) -> Option<Vec<Self::SideEffect>> {
///         None
///     }
/// }
///
/// // Create a repository for your aggregate type
/// # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// use eventastic::repository::Repository;
///
/// let repository: InMemoryRepository<Counter> = InMemoryRepository::new();
///
/// // Use transactions to store aggregates
/// let mut aggregate = Counter::record_new(CounterEvent::Created {
///     event_id: "event-1".to_string(),
///     initial_count: 0,
/// })?;
///
/// let mut transaction = repository.begin_transaction().await?;
/// transaction.store(&mut aggregate).await?;
/// transaction.commit()?;
///
/// // Load aggregates using the Repository trait
/// let loaded = repository.load(&"counter-1".to_string()).await?;
/// assert_eq!(loaded.state().count, 0);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct InMemoryRepository<T: Aggregate>
where
    T::AggregateId: Hash + Eq,
    <<T as Aggregate>::DomainEvent as DomainEvent>::EventId: Hash + Eq,
    T::SideEffect: Debug + Clone,
{
    storage: Arc<Mutex<InMemoryStorage<T>>>,
}

impl<T: Aggregate> InMemoryRepository<T>
where
    T::AggregateId: Hash + Eq,
    <<T as Aggregate>::DomainEvent as DomainEvent>::EventId: Hash + Eq,
    T::SideEffect: Debug + Clone,
{
    /// Creates a new empty in-memory repository.
    pub fn new() -> Self {
        Self {
            storage: Arc::new(Mutex::new(InMemoryStorage::default())),
        }
    }

    /// Gets a specific event by ID.
    pub fn get_event(
        &self,
        aggregate_id: &T::AggregateId,
        event_id: &<<T as Aggregate>::DomainEvent as DomainEvent>::EventId,
    ) -> Option<EventStoreEvent<T::DomainEvent>> {
        let storage = self.storage.lock().unwrap();

        if let Some((stored_aggregate_id, version)) = storage.events_by_id.get(event_id) {
            if stored_aggregate_id == aggregate_id {
                if let Some(aggregate_events) = storage.events.get(aggregate_id) {
                    return aggregate_events.get(version).cloned();
                }
            }
        }

        None
    }

    /// Gets all events for an aggregate starting from a specific version.
    pub fn get_events_from(
        &self,
        aggregate_id: &T::AggregateId,
        from_version: u64,
    ) -> Vec<EventStoreEvent<T::DomainEvent>> {
        let storage = self.storage.lock().unwrap();

        if let Some(aggregate_events) = storage.events.get(aggregate_id) {
            let mut events: Vec<_> = aggregate_events
                .iter()
                .filter(|(version, _)| **version >= from_version)
                .map(|(_, event)| event.clone())
                .collect();

            events.sort_by_key(|event| event.version);
            events
        } else {
            Vec::new()
        }
    }

    /// Returns the number of stored side effects.
    pub fn side_effects_count(&self) -> usize {
        let storage = self.storage.lock().unwrap();
        storage.side_effects.len()
    }

    /// Returns all stored side effects.
    pub fn get_all_side_effects(&self) -> Vec<T::SideEffect> {
        let storage = self.storage.lock().unwrap();
        storage.side_effects.clone()
    }
}

impl<T: Aggregate> Default for InMemoryRepository<T>
where
    T::AggregateId: Hash + Eq,
    <<T as Aggregate>::DomainEvent as DomainEvent>::EventId: Hash + Eq,
    T::SideEffect: Debug + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Transaction-like wrapper for in-memory repository.
///
/// This provides the same interface as a database transaction but operates
/// on the in-memory storage. All operations are immediately committed.
#[derive(Debug)]
pub struct InMemoryTransaction<T: Aggregate>
where
    T::AggregateId: Hash + Eq,
    <<T as Aggregate>::DomainEvent as DomainEvent>::EventId: Hash + Eq,
    T::SideEffect: Debug + Clone,
{
    repository: InMemoryRepository<T>,
}

impl<T: Aggregate> InMemoryTransaction<T>
where
    T::AggregateId: Hash + Eq + Send + Sync,
    <<T as Aggregate>::DomainEvent as DomainEvent>::EventId: Hash + Eq + Send + Sync,
    T::SideEffect: Debug + Clone + Send + Sync,
    T: Send + Sync,
    T::DomainEvent: Send + Sync,
    T::ApplyError: Send + Sync,
{
    /// Creates a new transaction wrapper around the repository.
    pub fn new(repository: InMemoryRepository<T>) -> Self {
        Self { repository }
    }

    /// Gets an aggregate by ID (equivalent to loading in a transaction).
    pub async fn get(
        &mut self,
        aggregate_id: &T::AggregateId,
    ) -> Result<Context<T>, InMemoryError> {
        Context::load(self, aggregate_id)
            .await
            .map_err(|e| match e {
                crate::repository::RepositoryError::AggregateNotFound => {
                    InMemoryError::AggregateNotFound
                }
                crate::repository::RepositoryError::Apply(_, _) => InMemoryError::AggregateNotFound,
                crate::repository::RepositoryError::Repository(db_err) => db_err,
            })
    }

    /// Stores an aggregate (equivalent to saving in a transaction).
    pub async fn store(&mut self, context: &mut Context<T>) -> Result<(), InMemoryError> {
        context.save(self).await.map_err(|e| match e {
            SaveError::IdempotencyError(_, _) => InMemoryError::EventExists,
            SaveError::OptimisticConcurrency(_, version) => InMemoryError::VersionConflict {
                expected: version,
                actual: version,
            },
            SaveError::Repository(db_err) => db_err,
        })
    }

    /// Commits the transaction (no-op for in-memory).
    pub fn commit(self) -> Result<(), InMemoryError> {
        Ok(())
    }

    /// Rolls back the transaction (no-op for in-memory).
    pub fn rollback(self) -> Result<(), InMemoryError> {
        Ok(())
    }
}

#[async_trait]
impl<T: Aggregate> RepositoryReader<T> for InMemoryTransaction<T>
where
    T::AggregateId: Hash + Eq + Send + Sync,
    <<T as Aggregate>::DomainEvent as DomainEvent>::EventId: Hash + Eq + Send + Sync,
    T::SideEffect: Debug + Clone + Send + Sync,
    T: Send + Sync,
    T::DomainEvent: Send + Sync,
{
    type DbError = InMemoryError;

    fn stream_from(
        &mut self,
        id: &T::AggregateId,
        version: u64,
    ) -> impl Stream<Item = Result<EventStoreEvent<T::DomainEvent>, Self::DbError>> {
        let events = self.repository.get_events_from(id, version);
        stream::iter(events.into_iter().map(Ok))
    }

    async fn get_event(
        &mut self,
        aggregate_id: &T::AggregateId,
        event_id: &<<T as Aggregate>::DomainEvent as DomainEvent>::EventId,
    ) -> Result<Option<EventStoreEvent<T::DomainEvent>>, Self::DbError> {
        Ok(self.repository.get_event(aggregate_id, event_id))
    }

    async fn get_snapshot(
        &mut self,
        id: &T::AggregateId,
    ) -> Result<Option<Snapshot<T>>, Self::DbError> {
        let storage = self.repository.storage.lock().unwrap();

        // This is the key: only return snapshots that match the current SNAPSHOT_VERSION
        if let Some(snapshot) = storage.snapshots.get(id) {
            if snapshot.snapshot_version == T::SNAPSHOT_VERSION {
                Ok(Some(snapshot.clone()))
            } else {
                // Snapshot version is incompatible, return None to force event replay
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl<T: Aggregate> RepositoryWriter<T> for InMemoryTransaction<T>
where
    T::AggregateId: Hash + Eq + Send + Sync,
    <<T as Aggregate>::DomainEvent as DomainEvent>::EventId: Hash + Eq + Send + Sync,
    T::SideEffect: Debug + Clone + Send + Sync,
    T: Send + Sync,
    T::DomainEvent: Send + Sync,
{
    async fn store_events(
        &mut self,
        id: &T::AggregateId,
        events: Vec<EventStoreEvent<T::DomainEvent>>,
    ) -> Result<Vec<<<T as Aggregate>::DomainEvent as DomainEvent>::EventId>, Self::DbError> {
        let mut storage = self.repository.storage.lock().unwrap();
        let mut stored_ids = Vec::new();

        for event in events {
            // Check for duplicate event IDs
            if let Some((existing_agg_id, _existing_version)) = storage.events_by_id.get(event.id())
            {
                if existing_agg_id == id {
                    // Same aggregate - event already exists, don't insert again
                    // Let Context::save handle idempotency checking
                    continue;
                } else {
                    // Different aggregate has the same event ID - this is an error
                    return Err(InMemoryError::EventExists);
                }
            }

            // Check for version conflicts (optimistic concurrency)
            if let Some(aggregate_events) = storage.events.get(id) {
                if aggregate_events.contains_key(&event.version) {
                    return Err(InMemoryError::VersionConflict {
                        expected: event.version,
                        actual: event.version,
                    });
                }
            }

            // Store the event
            storage
                .events
                .entry(id.clone())
                .or_default()
                .insert(event.version, event.clone());

            storage
                .events_by_id
                .insert(event.id().clone(), (id.clone(), event.version));

            stored_ids.push(event.id().clone());
        }

        Ok(stored_ids)
    }

    async fn store_snapshot(&mut self, snapshot: Snapshot<T>) -> Result<(), Self::DbError> {
        let mut storage = self.repository.storage.lock().unwrap();
        storage
            .snapshots
            .insert(snapshot.aggregate.aggregate_id().clone(), snapshot);
        Ok(())
    }

    async fn store_side_effects(
        &mut self,
        side_effects: Vec<T::SideEffect>,
    ) -> Result<(), Self::DbError> {
        let mut storage = self.repository.storage.lock().unwrap();
        storage.side_effects.extend(side_effects);
        Ok(())
    }
}

#[async_trait]
impl<T: Aggregate> Repository<T> for InMemoryRepository<T>
where
    T::AggregateId: Hash + Eq + Send + Sync,
    <<T as Aggregate>::DomainEvent as DomainEvent>::EventId: Hash + Eq + Send + Sync,
    T::SideEffect: Debug + Clone + Send + Sync,
    T: Send + Sync,
    T::DomainEvent: Send + Sync,
    T::ApplyError: Send + Sync,
{
    type Error = InMemoryError;

    async fn load(&self, aggregate_id: &T::AggregateId) -> Result<Context<T>, Self::Error> {
        let mut transaction = InMemoryTransaction::new(self.clone());
        Context::load(&mut transaction, aggregate_id)
            .await
            .map_err(|e| match e {
                crate::repository::RepositoryError::AggregateNotFound => {
                    InMemoryError::AggregateNotFound
                }
                crate::repository::RepositoryError::Apply(_, _) => InMemoryError::AggregateNotFound,
                crate::repository::RepositoryError::Repository(db_err) => db_err,
            })
    }
}

impl<T: Aggregate> InMemoryRepository<T>
where
    T::AggregateId: Hash + Eq + Send + Sync,
    <<T as Aggregate>::DomainEvent as DomainEvent>::EventId: Hash + Eq + Send + Sync,
    T::SideEffect: Debug + Clone + Send + Sync,
    T: Send + Sync,
    T::DomainEvent: Send + Sync,
    T::ApplyError: Send + Sync,
{
    /// Begins a new "transaction" (returns a transaction-like wrapper).
    pub async fn begin_transaction(&self) -> Result<InMemoryTransaction<T>, InMemoryError> {
        Ok(InMemoryTransaction::new(self.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        aggregate::{Context, Root, SaveError},
        repository::Repository,
        test_fixtures::*,
    };

    // Tests basic repository operations: create, store, and load aggregates
    #[tokio::test]
    async fn test_repository_load_and_save() {
        let repository: InMemoryRepository<TestCounter> = InMemoryRepository::new();

        // Create a new counter
        let reset_event = create_reset_event("reset-1", "calc-1");
        let mut context = TestCounter::record_new(reset_event).unwrap();

        // Add some operations
        context.record_that(create_add_event("add-1", 10)).unwrap();
        context
            .record_that(create_subtract_event("sub-1", 3))
            .unwrap();

        // Save using transaction
        let mut transaction = repository.begin_transaction().await.unwrap();
        transaction.store(&mut context).await.unwrap();
        transaction.commit().unwrap();

        // Load the counter
        let loaded_context = repository.load(&"calc-1".to_string()).await.unwrap();
        assert_eq!(loaded_context.state().result, 7);
        assert_eq!(loaded_context.state().operations_count, 2);
        assert_eq!(loaded_context.version(), 2);
    }

    // Tests that Repository::load returns an error for non-existent aggregates
    #[tokio::test]
    async fn test_repository_aggregate_not_found() {
        let repository: InMemoryRepository<TestCounter> = InMemoryRepository::new();
        let result = repository.load(&"non-existent".to_string()).await;
        assert!(result.is_err());
    }

    // Tests InMemoryTransaction operations: get, store, and commit within transactions
    #[tokio::test]
    async fn test_repository_transaction_operations() {
        let repository: InMemoryRepository<TestCounter> = InMemoryRepository::new();

        // Create counter using transaction
        let reset_event = create_reset_event("reset-1", "calc-1");
        let mut context = TestCounter::record_new(reset_event).unwrap();

        let mut transaction = repository.begin_transaction().await.unwrap();
        transaction.store(&mut context).await.unwrap();
        transaction.commit().unwrap();

        // Load using transaction
        let mut transaction = repository.begin_transaction().await.unwrap();
        let mut loaded_context = transaction.get(&"calc-1".to_string()).await.unwrap();

        // Modify and save
        loaded_context
            .record_that(create_add_event("add-1", 20))
            .unwrap();
        transaction.store(&mut loaded_context).await.unwrap();
        transaction.commit().unwrap();

        // Verify changes
        let final_context = repository.load(&"calc-1".to_string()).await.unwrap();
        assert_eq!(final_context.state().result, 20);
    }

    // Tests that storing the same event twice succeeds (idempotent behavior)
    #[tokio::test]
    async fn test_repository_idempotency() {
        let repository: InMemoryRepository<TestCounter> = InMemoryRepository::new();

        // Create counter
        let reset_event = create_reset_event("reset-1", "calc-1");
        let mut context1 = TestCounter::record_new(reset_event).unwrap();
        let mut transaction = repository.begin_transaction().await.unwrap();
        transaction.store(&mut context1).await.unwrap();
        transaction.commit().unwrap();

        // Try to apply the same event again
        let same_reset_event = create_reset_event("reset-1", "calc-1");
        let mut context2 = TestCounter::record_new(same_reset_event).unwrap();
        let mut transaction = repository.begin_transaction().await.unwrap();
        let result = transaction.store(&mut context2).await;

        // Should succeed since content is the same
        assert!(result.is_ok());
    }

    // Tests that cross-aggregate event ID conflicts are properly rejected
    #[tokio::test]
    async fn test_repository_idempotency_error() {
        let repository: InMemoryRepository<TestCounter> = InMemoryRepository::new();

        // Create counter
        let reset_event = create_reset_event("reset-1", "calc-1");
        let mut context1 = TestCounter::record_new(reset_event).unwrap();
        let mut transaction = repository.begin_transaction().await.unwrap();
        transaction.store(&mut context1).await.unwrap();
        transaction.commit().unwrap();

        // Try to apply different event with same ID
        let different_reset_event = create_reset_event("reset-1", "different-calc");
        let mut context2 = TestCounter::record_new(different_reset_event).unwrap();
        let mut transaction = repository.begin_transaction().await.unwrap();
        let result = transaction.store(&mut context2).await;

        // Should fail with idempotency error
        assert!(result.is_err());
    }

    // Tests that snapshot version checking works correctly (incompatible snapshots are ignored)
    #[tokio::test]
    async fn test_snapshot_version_checking() {
        let repository: InMemoryRepository<TestCounter> = InMemoryRepository::new();

        // Create and save counter
        let reset_event = create_reset_event("reset-1", "calc-1");
        let mut context = TestCounter::record_new(reset_event).unwrap();
        context.record_that(create_add_event("add-1", 100)).unwrap();

        let mut transaction = repository.begin_transaction().await.unwrap();
        transaction.store(&mut context).await.unwrap();
        transaction.commit().unwrap();

        // Verify snapshot was stored with correct version
        let mut transaction = repository.begin_transaction().await.unwrap();
        let snapshot = transaction
            .get_snapshot(&"calc-1".to_string())
            .await
            .unwrap();

        assert!(snapshot.is_some());
        let snap = snapshot.unwrap();
        assert_eq!(snap.snapshot_version, TestCounter::SNAPSHOT_VERSION);
        assert_eq!(snap.version, 1);
        assert_eq!(snap.aggregate.result, 100);
    }

    // Tests InMemoryRepository utility methods for side effects tracking
    #[tokio::test]
    async fn test_in_memory_repository_basic_operations() {
        let repository: InMemoryRepository<TestCounter> = InMemoryRepository::new();

        // Test empty repository
        assert_eq!(repository.side_effects_count(), 0);
        assert!(repository.get_all_side_effects().is_empty());

        // Create and save data
        let reset_event = create_reset_event("reset-1", "calc-1");
        let mut context = TestCounter::record_new(reset_event).unwrap();
        context.record_that(create_add_event("add-1", 50)).unwrap();

        let mut transaction = repository.begin_transaction().await.unwrap();
        transaction.store(&mut context).await.unwrap();
        transaction.commit().unwrap();

        // Verify side effects were stored
        assert!(repository.side_effects_count() > 0);
        assert!(!repository.get_all_side_effects().is_empty());
    }

    // Tests that Context::save handles empty event lists correctly (no-op behavior)
    #[tokio::test]
    async fn test_context_save_empty_events() {
        let repository: InMemoryRepository<TestCounter> = InMemoryRepository::new();

        // Create counter and save initially
        let reset_event = create_reset_event("reset-1", "calc-1");
        let mut context = TestCounter::record_new(reset_event).unwrap();
        let mut transaction = repository.begin_transaction().await.unwrap();
        transaction.store(&mut context).await.unwrap();
        transaction.commit().unwrap();

        // Load the counter and try to save without any changes
        let mut loaded_context = repository.load(&"calc-1".to_string()).await.unwrap();

        // No new events recorded
        assert_eq!(loaded_context.take_uncommitted_events().len(), 0);

        // Save should succeed with empty events
        let mut transaction = repository.begin_transaction().await.unwrap();
        let result = loaded_context.save(&mut transaction).await;
        assert!(result.is_ok());
        transaction.commit().unwrap();
    }

    // Tests that Context::save properly detects and reports idempotency violations
    #[tokio::test]
    async fn test_context_save_idempotency_error() {
        let repository: InMemoryRepository<TestCounter> = InMemoryRepository::new();

        // Create and save initial counter
        let reset_event = create_reset_event("reset-1", "calc-1");
        let mut context1 = TestCounter::record_new(reset_event).unwrap();
        let mut transaction = repository.begin_transaction().await.unwrap();
        context1.save(&mut transaction).await.unwrap();
        transaction.commit().unwrap();

        // Load the counter and add an event with a specific ID
        let mut loaded_context = repository.load(&"calc-1".to_string()).await.unwrap();
        loaded_context
            .record_that(create_add_event("add-1", 50))
            .unwrap();
        let mut transaction = repository.begin_transaction().await.unwrap();
        loaded_context.save(&mut transaction).await.unwrap();
        transaction.commit().unwrap();

        // Now try to add a DIFFERENT event with the same ID "add-1"
        let mut loaded_context2 = repository.load(&"calc-1".to_string()).await.unwrap();
        loaded_context2
            .record_that(create_add_event("add-1", 100))
            .unwrap(); // Different value!
        let mut transaction = repository.begin_transaction().await.unwrap();

        // Should fail with idempotency error (same event ID, different content)
        let result = loaded_context2.save(&mut transaction).await;
        assert!(matches!(result, Err(SaveError::IdempotencyError(_, _))));
    }

    // Tests that Context::save properly detects and reports optimistic concurrency violations
    #[tokio::test]
    async fn test_context_save_optimistic_concurrency() {
        let repository: InMemoryRepository<TestCounter> = InMemoryRepository::new();

        // Create and save initial counter
        let reset_event = create_reset_event("reset-1", "calc-1");
        let mut context = TestCounter::record_new(reset_event).unwrap();
        let mut transaction = repository.begin_transaction().await.unwrap();
        context.save(&mut transaction).await.unwrap();
        transaction.commit().unwrap();

        // Load the same counter in two different contexts (simulating concurrent access)
        let mut context1 = repository.load(&"calc-1".to_string()).await.unwrap();
        let mut context2 = repository.load(&"calc-1".to_string()).await.unwrap();

        // Modify both contexts - they should both try to add version 1 events
        context1.record_that(create_add_event("add-1", 10)).unwrap();
        context2.record_that(create_add_event("add-2", 20)).unwrap();

        // First save should succeed
        let mut transaction1 = repository.begin_transaction().await.unwrap();
        let result1 = context1.save(&mut transaction1).await;
        assert!(result1.is_ok());
        transaction1.commit().unwrap();

        // Second save should fail with repository error (version conflict)
        let mut transaction2 = repository.begin_transaction().await.unwrap();
        let result2 = context2.save(&mut transaction2).await;
        // The in-memory implementation returns a Repository error for version conflicts
        assert!(matches!(
            result2,
            Err(SaveError::Repository(InMemoryError::VersionConflict { .. }))
        ));
    }

    // Tests that Context::load correctly reconstructs aggregates from events and snapshots
    #[tokio::test]
    async fn test_context_load_with_snapshot() {
        let repository: InMemoryRepository<TestCounter> = InMemoryRepository::new();

        // Create and save counter with events
        let mut context = TestCounter::record_new(create_reset_event("reset-1", "calc-1")).unwrap();
        context.record_that(create_add_event("add-1", 100)).unwrap();

        let mut transaction = repository.begin_transaction().await.unwrap();
        context.save(&mut transaction).await.unwrap();
        transaction.commit().unwrap();

        // Load using Context::load method directly
        let mut transaction = repository.begin_transaction().await.unwrap();
        let mut loaded_context = Context::load(&mut transaction, &"calc-1".to_string())
            .await
            .unwrap();

        assert_eq!(loaded_context.state().result, 100);
        assert_eq!(loaded_context.version(), 1);
        assert_eq!(loaded_context.take_uncommitted_events().len(), 0);
    }

    // Tests that Context::regenerate_side_effects can regenerate side effects for specific events
    #[tokio::test]
    async fn test_regenerate_side_effects_specific_event() {
        let repository: InMemoryRepository<TestCounter> = InMemoryRepository::new();

        // Create counter with multiple events
        let mut context = TestCounter::record_new(create_reset_event("reset-1", "calc-1")).unwrap();
        context.record_that(create_add_event("add-1", 100)).unwrap();
        context
            .record_that(create_subtract_event("sub-1", 25))
            .unwrap();

        let mut transaction = repository.begin_transaction().await.unwrap();
        context.save(&mut transaction).await.unwrap();
        transaction.commit().unwrap();

        // Regenerate side effects for the Add event
        let mut transaction = repository.begin_transaction().await.unwrap();
        let side_effects = Context::<TestCounter>::regenerate_side_effects(
            &mut transaction,
            &"calc-1".to_string(),
            &"add-1".to_string(),
        )
        .await
        .unwrap();

        assert!(side_effects.is_some());
        let effects = side_effects.unwrap();
        assert_eq!(effects.len(), 1); // Add event generates 1 side effect

        assert!(matches!(
            effects[0],
            TestSideEffect::LogOperation { ref operation, .. } if operation == "Add 100"
        ));
    }

    // Tests that Context::regenerate_side_effects returns None for non-existent events
    #[tokio::test]
    async fn test_regenerate_side_effects_event_not_found() {
        let repository: InMemoryRepository<TestCounter> = InMemoryRepository::new();

        // Create counter
        let mut context = TestCounter::record_new(create_reset_event("reset-1", "calc-1")).unwrap();
        let mut transaction = repository.begin_transaction().await.unwrap();
        context.save(&mut transaction).await.unwrap();
        transaction.commit().unwrap();

        // Try to regenerate side effects for non-existent event
        let mut transaction = repository.begin_transaction().await.unwrap();
        let side_effects = Context::<TestCounter>::regenerate_side_effects(
            &mut transaction,
            &"calc-1".to_string(),
            &"non-existent-event".to_string(),
        )
        .await
        .unwrap();

        assert!(side_effects.is_none());
    }

    // Tests a complete end-to-end workflow with multiple operations and side effects
    #[tokio::test]
    async fn test_complete_counter_workflow() {
        let repository: InMemoryRepository<TestCounter> = InMemoryRepository::new();

        // Create counter
        let reset_event = create_reset_event("reset-1", "my-counter");
        let mut counter = TestCounter::record_new(reset_event).unwrap();

        // Perform calculations
        counter.record_that(create_add_event("add-1", 100)).unwrap();
        counter
            .record_that(create_subtract_event("sub-1", 25))
            .unwrap();
        counter
            .record_that(create_multiply_event("mul-1", 2))
            .unwrap();
        counter.record_that(create_add_event("add-2", 50)).unwrap();

        // Save to repository using transaction
        let mut transaction = repository.begin_transaction().await.unwrap();
        transaction.store(&mut counter).await.unwrap();
        transaction.commit().unwrap();

        // Load and verify final state
        let loaded_counter = repository.load(&"my-counter".to_string()).await.unwrap();
        assert_eq!(loaded_counter.state().result, 200); // ((100 - 25) * 2) + 50 = 200
        assert_eq!(loaded_counter.state().operations_count, 4);
        assert_eq!(loaded_counter.version(), 4);

        // Verify side effects were stored
        // Reset: 2 side effects, Add: 1, Subtract: 1, Multiply: 0, Add: 1 = 5 total
        let expected_side_effects = 2 + 1 + 1 + 0 + 1;
        assert_eq!(repository.side_effects_count(), expected_side_effects);

        let side_effects = repository.get_all_side_effects();
        assert_eq!(side_effects.len(), expected_side_effects);
    }

    // Tests error handling and recovery behavior when invalid operations are attempted
    #[tokio::test]
    async fn test_error_handling_and_recovery() {
        let repository: InMemoryRepository<TestCounter> = InMemoryRepository::new();

        // Create counter
        let mut counter = TestCounter::record_new(create_reset_event("reset-1", "calc-1")).unwrap();

        // Add some value
        counter.record_that(create_add_event("add-1", 10)).unwrap();

        // Try invalid operation (should fail)
        let invalid_result = counter.record_that(create_multiply_event("mul-zero", 0));
        assert!(matches!(invalid_result, Err(TestError::DivisionByZero)));

        // State should be unchanged after error
        assert_eq!(counter.state().result, 10);
        assert_eq!(counter.version(), 1);

        // Continue with valid operations
        counter
            .record_that(create_multiply_event("mul-valid", 3))
            .unwrap();
        assert_eq!(counter.state().result, 30);
        assert_eq!(counter.version(), 2);

        // Save and verify persistence
        let mut transaction = repository.begin_transaction().await.unwrap();
        transaction.store(&mut counter).await.unwrap();
        transaction.commit().unwrap();

        let loaded = repository.load(&"calc-1".to_string()).await.unwrap();
        assert_eq!(loaded.state().result, 30);
        assert_eq!(loaded.version(), 2);
    }

    // Tests that InMemoryRepository::default() works correctly
    #[tokio::test]
    async fn test_in_memory_repository_default() {
        let repository: InMemoryRepository<TestCounter> = InMemoryRepository::default();

        // Should behave exactly like new()
        assert_eq!(repository.side_effects_count(), 0);
        assert!(repository.get_all_side_effects().is_empty());

        // Should be able to store and retrieve data
        let reset_event = create_reset_event("reset-1", "calc-1");
        let mut context = TestCounter::record_new(reset_event).unwrap();

        let mut transaction = repository.begin_transaction().await.unwrap();
        transaction.store(&mut context).await.unwrap();
        transaction.commit().unwrap();

        let loaded = repository.load(&"calc-1".to_string()).await.unwrap();
        assert_eq!(loaded.state().id, "calc-1");
    }
}
