//! Domain events and event store abstractions for event sourcing.
//!
//! ## Overview
//!
//! This module provides the core abstractions for working with domain events in an event sourcing
//! system. Domain events represent significant business occurrences that have happened in your
//! domain and serve as the source of truth for aggregate state.
//!
//! ## Key Concepts
//!
//! ### Domain Events
//!
//! Domain events are immutable records of business-relevant facts that have occurred. They are:
//!
//! - **Immutable** - Once created, events never change
//! - **Uniquely identifiable** - Each event has a unique ID for idempotency
//! - **Business-focused** - Express what happened in domain terms
//! - **Complete** - Contain all necessary information about the occurrence
//!
//! ### Event Store Events
//!
//! [`EventStoreEvent`] wraps domain events with additional metadata needed for persistence:
//!
//! - **Version** - Enables optimistic concurrency control
//! - **ID** - Extracted from the domain event for quick access
//! - **Domain Event** - The actual business event
//!
//! ## Eventastic Workflow
//!
//! Once you have domain events (created by your business logic), they flow through
//! the eventastic system in a well-defined lifecycle:
//!
//! ### **1. Event Creation**
//! Your business logic creates domain events representing what happened:
//! ```rust,ignore
//! // Your application code creates events
//! let event = AccountEvent::MoneyWithdrawn {
//!     event_id: Uuid::new_v4(),
//!     amount: 100
//! };
//! ```
//!
//! ### **2. Event Recording**
//! Events are recorded in [`Context<T>`](crate::aggregate::Context), which applies them to
//! the aggregate state and queues them for persistence:
//! ```rust,ignore
//! context.record_that(event)?; // Applies event and adds to uncommitted events
//! ```
//!
//! ### **3. Event Persistence**
//! The repository wraps events in [`EventStoreEvent`] and stores them transactionally:
//! ```rust,ignore
//! transaction.store(&mut context).await?; // Persists all uncommitted events
//! transaction.commit().await?;
//! ```
//!
//! ### **4. Event Replay**
//! Stored events are replayed to reconstruct aggregate state during loading:
//! ```rust,ignore
//! let aggregate = repository.load(&aggregate_id).await?; // Replays all events
//! ```
//!
//! This workflow ensures **exactly-once processing**, **ACID compliance**, and **full auditability**
//! of all business operations.
//!
//! **Note**: Command processing (validating commands and deciding which events to create)
//! is handled by your application code, not by this library.
//!
//! ## Idempotency
//!
//! Event IDs are crucial for idempotency - attempting to store the same event ID twice will either
//! succeed (if content is identical) or fail with an idempotency error (if content differs).
//! This ensures exactly-once processing of business events.
//!
//! ## Examples
//!
//! For complete examples showing events in context, see:
//! - The [banking example](https://github.com/jdon/eventastic/tree/main/examples/bank) for production patterns
//! - [`crate::aggregate`] module documentation for how events are applied to aggregates
//! - [`crate::repository`] module documentation for event persistence patterns

use std::fmt::Debug;

/// A [`DomainEvent`] that has been wrapped with metadata for persistence.
///
/// This struct represents a domain event along with the metadata needed for storing
/// and retrieving it from the event store. It's used internally by the eventastic
/// framework to manage event persistence and concurrency control.
///
/// ## Fields
///
/// - **`id`** - The unique identifier extracted from the domain event, used for idempotency checking
/// - **`version`** - The version number of this event within the aggregate's event stream
/// - **`event`** - The actual domain event containing the business data
///
/// ## Versioning and Concurrency
///
/// The `version` field enables optimistic concurrency control. When multiple transactions
/// try to modify the same aggregate simultaneously, version conflicts are detected and
/// one transaction will fail with an [`OptimisticConcurrency`](crate::aggregate::SaveError::OptimisticConcurrency) error.
///
/// ## Usage
///
/// `EventStoreEvent` is typically created automatically when you call
/// [`Context::record_that()`](crate::aggregate::Context::record_that) or retrieved
/// when replaying events to reconstruct aggregate state.
///
/// ```rust
/// use eventastic::event::{DomainEvent, EventStoreEvent};
///
/// #[derive(Clone, PartialEq, Eq, Debug)]
/// enum MyEvent {
///     Created { event_id: String, value: i32 },
/// }
///
/// impl DomainEvent for MyEvent {
///     type EventId = String;
///     fn id(&self) -> &Self::EventId {
///         match self {
///             MyEvent::Created { event_id, .. } => event_id,
///         }
///     }
/// }
///
/// let domain_event = MyEvent::Created {
///     event_id: "evt-123".to_string(),
///     value: 42,
/// };
///
/// let store_event = EventStoreEvent::new(
///     "evt-123".to_string(),
///     1, // version
///     domain_event,
/// );
///
/// assert_eq!(store_event.id(), "evt-123");
/// assert_eq!(store_event.version(), 1);
/// ```
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EventStoreEvent<Evt>
where
    Evt: DomainEvent,
{
    /// The unique identifier of the event, extracted from the domain event.
    ///
    /// This ID is used for idempotency checking - attempting to store the same
    /// event ID twice will either succeed (if content is identical) or fail
    /// with an idempotency error (if content differs).
    pub id: Evt::EventId,

    /// The version number of this event within the aggregate's event stream.
    ///
    /// Versions start at 0 and increment for each new event. This enables
    /// optimistic concurrency control by detecting when two transactions
    /// try to modify the same aggregate simultaneously.
    pub version: u64,

    /// The actual domain event containing the business data.
    ///
    /// This is the event that was created by your business logic and contains
    /// all the information needed to apply the change to the aggregate state.
    pub event: Evt,
}

impl<Evt> EventStoreEvent<Evt>
where
    Evt: DomainEvent,
{
    /// Creates a new `EventStoreEvent`.
    pub fn new(id: Evt::EventId, version: u64, event: Evt) -> Self {
        Self { id, version, event }
    }

    /// Returns the id of the event.
    pub fn id(&self) -> &Evt::EventId {
        &self.id
    }

    /// Returns the version of the event.
    pub fn version(&self) -> u64 {
        self.version
    }
}

/// A domain event represents something significant that happened in your domain.
///
/// Domain events are the building blocks of event sourcing. They capture
/// business-relevant facts that have occurred and are used to reconstruct
/// aggregate state through event replay.
///
/// ## Design Principles
///
/// When designing domain events, follow these principles:
///
/// - **Past tense naming** - Events describe what happened ("OrderCreated", not "CreateOrder")
/// - **Immutable** - Events should never change after creation
/// - **Complete** - Include all data needed to apply the change
/// - **Unique IDs** - Each event instance must have a unique identifier
/// - **Business-focused** - Express domain concepts, not technical details
///
/// ## Relationship to Aggregates
///
/// Domain events are closely tied to [`Aggregate`](crate::aggregate::Aggregate)s:
///
/// - Created by your business logic methods
/// - Recorded in [`Context<T>`](crate::aggregate::Context) via `record_that()`
/// - Automatically applied to aggregate state during recording
/// - Used to reconstruct aggregate state through event replay
///
/// ## Idempotency and Event IDs
///
/// Event IDs are crucial for ensuring exactly-once processing. The eventastic framework
/// uses these IDs to detect duplicate events and maintain system consistency.
///
/// ## String ID Example
///
/// ```rust
/// use eventastic::event::DomainEvent;
///
/// #[derive(Clone, PartialEq, Eq, Debug)]
/// enum OrderEvent {
///     Created { event_id: String, order_id: String, customer_id: String },
///     ItemAdded { event_id: String, item_id: String, quantity: u32 },
///     Shipped { event_id: String, tracking_number: String },
/// }
///
/// impl DomainEvent for OrderEvent {
///     type EventId = String;
///     
///     fn id(&self) -> &Self::EventId {
///         match self {
///             OrderEvent::Created { event_id, .. } => event_id,
///             OrderEvent::ItemAdded { event_id, .. } => event_id,
///             OrderEvent::Shipped { event_id, .. } => event_id,
///         }
///     }
/// }
///
/// // Create some events
/// let created_event = OrderEvent::Created {
///     event_id: "evt-1".to_string(),
///     order_id: "order-123".to_string(),
///     customer_id: "customer-456".to_string(),
/// };
///
/// let item_added_event = OrderEvent::ItemAdded {
///     event_id: "evt-2".to_string(),
///     item_id: "item-789".to_string(),
///     quantity: 2,
/// };
///
/// // Access event IDs
/// assert_eq!(created_event.id(), "evt-1");
/// assert_eq!(item_added_event.id(), "evt-2");
///
/// // Events can be compared for equality
/// let created_event_copy = created_event.clone();
/// assert_eq!(created_event, created_event_copy);
/// ```
///
/// ## Using Different ID Types
///
/// Domain events can use different types for their IDs:
///
/// ```rust
/// use eventastic::event::DomainEvent;
///
/// // Using numeric IDs
/// #[derive(Clone, PartialEq, Eq, Debug)]
/// enum UserEvent {
///     Registered { event_id: u64, user_id: u64, email: String },
///     EmailChanged { event_id: u64, new_email: String },
/// }
///
/// impl DomainEvent for UserEvent {
///     type EventId = u64;
///     
///     fn id(&self) -> &Self::EventId {
///         match self {
///             UserEvent::Registered { event_id, .. } => event_id,
///             UserEvent::EmailChanged { event_id, .. } => event_id,
///         }
///     }
/// }
///
/// // Create an event with numeric ID
/// let user_event = UserEvent::Registered {
///     event_id: 12345,
///     user_id: 67890,
///     email: "user@example.com".to_string(),
/// };
///
/// assert_eq!(*user_event.id(), 12345);
/// ```
pub trait DomainEvent: Clone + Eq + PartialEq {
    /// The type used to uniquely identify this event.
    ///
    /// This is typically a UUID, String, or other unique identifier type
    /// that can distinguish this specific event instance from all others.
    ///
    /// ## Recommended Types
    ///
    /// - **`uuid::Uuid`** - Best for production systems, provides global uniqueness
    /// - **`String`** - Good for development and testing, human-readable
    /// - **`u64`** - Compact, good for high-performance scenarios with centralized ID generation
    type EventId: Debug + Clone + Eq + PartialEq;

    /// Returns the unique identifier for this event instance.
    ///
    /// Each event must have a unique ID that distinguishes it from all other
    /// events. This ID is used for idempotency checks and event deduplication.
    ///
    /// ## Idempotency
    ///
    /// The framework uses this ID to ensure exactly-once processing:
    /// - If an event with the same ID and identical content is stored again, it succeeds
    /// - If an event with the same ID but different content is stored, it fails with an idempotency error
    ///
    /// ## Usage in Aggregates
    ///
    /// The framework uses this ID internally for event deduplication and consistency.
    /// You simply provide unique IDs when creating events in your business logic.
    fn id(&self) -> &Self::EventId;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::*;

    // Tests that EventStoreEvent::new creates a proper wrapper around domain events
    #[test]
    fn test_event_store_event_creation() {
        let domain_event = create_add_event("add-1", 10);
        let store_event = EventStoreEvent::new("add-1".to_string(), 5, domain_event.clone());

        assert_eq!(store_event.id(), "add-1");
        assert_eq!(store_event.version(), 5);
        assert_eq!(store_event.event, domain_event);
    }

    // Tests that EventStoreEvent fields are accessible directly (struct-style access)
    #[test]
    fn test_event_store_event_fields() {
        let domain_event = create_reset_event("reset-1", "calc-1");
        let store_event = EventStoreEvent {
            id: "reset-1".to_string(),
            version: 0,
            event: domain_event.clone(),
        };

        assert_eq!(store_event.id, "reset-1");
        assert_eq!(store_event.version, 0);
        assert_eq!(store_event.event, domain_event);
    }

    // Tests that EventStoreEvent implements equality correctly (same ID, version, and event content)
    #[test]
    fn test_event_store_event_equality() {
        let domain_event = create_add_event("add-1", 15);
        let event1 = EventStoreEvent::new("add-1".to_string(), 3, domain_event.clone());
        let event2 = EventStoreEvent::new("add-1".to_string(), 3, domain_event);

        assert_eq!(event1, event2);
    }

    // Tests that EventStoreEvent accessor methods return the correct values
    #[test]
    fn test_event_store_event_accessor_methods() {
        let domain_event = create_subtract_event("sub-1", 25);
        let store_event = EventStoreEvent::new("sub-1".to_string(), 7, domain_event);

        // Test id() accessor
        assert_eq!(store_event.id(), "sub-1");

        // Test version() accessor
        assert_eq!(store_event.version(), 7);
    }

    // Tests that DomainEvent::id returns the correct identifier for all event types
    #[test]
    fn test_domain_event_id_access() {
        let reset_event = create_reset_event("reset-123", "calc-1");
        assert_eq!(reset_event.id(), "reset-123");

        let add_event = create_add_event("add-456", 100);
        assert_eq!(add_event.id(), "add-456");

        let subtract_event = create_subtract_event("sub-789", 50);
        assert_eq!(subtract_event.id(), "sub-789");

        let multiply_event = create_multiply_event("mul-999", 3);
        assert_eq!(multiply_event.id(), "mul-999");
    }

    // Tests that domain events with same content are equal, different content are not equal
    #[test]
    fn test_domain_event_equality() {
        let event1 = create_add_event("add-1", 100);
        let event2 = create_add_event("add-1", 100);
        let event3 = create_add_event("add-1", 200); // Different value
        let event4 = create_add_event("add-2", 100); // Different ID

        assert_eq!(event1, event2);
        assert_ne!(event1, event3);
        assert_ne!(event1, event4);
    }

    // Tests that domain events can be cloned and the clone equals the original
    #[test]
    fn test_domain_event_clone() {
        let original = create_multiply_event("mul-1", 5);
        let cloned = original.clone();

        assert_eq!(original, cloned);
        assert_eq!(original.id(), cloned.id());
    }

    // Tests that EventStoreEvent::id() returns a reference to the stored ID
    #[test]
    fn test_event_store_event_id_reference() {
        let domain_event = create_add_event("test-id", 42);
        let store_event = EventStoreEvent::new("test-id".to_string(), 1, domain_event);

        // Test that id() returns a reference to the actual stored ID
        let id_ref = store_event.id();
        assert_eq!(id_ref, "test-id");
        assert_eq!(id_ref, &store_event.id); // Reference to the actual field

        // Additional explicit call to ensure coverage
        assert_eq!(*store_event.id(), "test-id".to_string());

        // Test direct field access via id() method
        let field_value = &store_event.id;
        assert_eq!(store_event.id(), field_value);
    }
}
