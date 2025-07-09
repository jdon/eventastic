//! # Eventastic
//!
//! A type-safe event sourcing and CQRS library for Rust with PostgreSQL persistence.
//!
//! Eventastic provides strong consistency guarantees through mandatory transactions,
//! built-in idempotency checking, and reliable side effect processing via the
//! transactional outbox pattern.
//!
//! ## Quick Start
//!
//! ```rust
//! use eventastic::aggregate::{Aggregate, Context, Root, SideEffect};
//! use eventastic::event::DomainEvent;
//! use eventastic::memory::InMemoryRepository;
//! use eventastic::repository::Repository;
//!
//! // Define your domain aggregate
//! #[derive(Clone, Debug)]
//! struct Counter {
//!     id: String,
//!     value: i32,
//! }
//!
//! // Define your domain events
//! #[derive(Clone, Debug, PartialEq, Eq)]
//! enum CounterEvent {
//!     Created { event_id: String, initial_value: i32 },
//!     Incremented { event_id: String, amount: i32 },
//! }
//!
//! impl DomainEvent for CounterEvent {
//!     type EventId = String;
//!     fn id(&self) -> &Self::EventId {
//!         match self {
//!             CounterEvent::Created { event_id, .. } => event_id,
//!             CounterEvent::Incremented { event_id, .. } => event_id,
//!         }
//!     }
//! }
//!
//! // Define side effects (optional)
//! #[derive(Clone, Debug, PartialEq, Eq)]
//! struct NoSideEffect {
//!     id: String,
//! }
//!
//! impl SideEffect for NoSideEffect {
//!     type SideEffectId = String;
//!     fn id(&self) -> &Self::SideEffectId {
//!         &self.id
//!     }
//! }
//!
//! // Implement the Aggregate trait
//! impl Aggregate for Counter {
//!     const SNAPSHOT_VERSION: u64 = 1;
//!     type AggregateId = String;
//!     type DomainEvent = CounterEvent;
//!     type ApplyError = String;
//!     type SideEffect = NoSideEffect;
//!
//!     fn aggregate_id(&self) -> &Self::AggregateId {
//!         &self.id
//!     }
//!
//!     fn apply_new(event: &Self::DomainEvent) -> Result<Self, Self::ApplyError> {
//!         match event {
//!             CounterEvent::Created { initial_value, .. } => Ok(Counter {
//!                 id: "counter-1".to_string(),
//!                 value: *initial_value,
//!             }),
//!             _ => Err("Counter must be created first".to_string()),
//!         }
//!     }
//!
//!     fn apply(&mut self, event: &Self::DomainEvent) -> Result<(), Self::ApplyError> {
//!         match event {
//!             CounterEvent::Created { .. } => Err("Counter already created".to_string()),
//!             CounterEvent::Incremented { amount, .. } => {
//!                 self.value += amount;
//!                 Ok(())
//!             }
//!         }
//!     }
//!
//!     fn side_effects(&self, _event: &Self::DomainEvent) -> Option<Vec<Self::SideEffect>> {
//!         None
//!     }
//! }
//!
//! // Usage with in-memory repository
//! # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! let repository = InMemoryRepository::<Counter>::new();
//!
//! // Create and persist a new counter
//! let mut counter: Context<Counter> = Counter::record_new(
//!     CounterEvent::Created {
//!         event_id: "evt-1".to_string(),
//!         initial_value: 0,
//!     }
//! )?;
//!
//! counter.record_that(CounterEvent::Incremented {
//!     event_id: "evt-2".to_string(),
//!     amount: 5,
//! })?;
//!
//! // Save to repository
//! let mut transaction = repository.begin_transaction().await?;
//! transaction.store(&mut counter).await?;
//! transaction.commit()?;
//!
//! // Load from repository
//! let loaded_counter = repository.load(&"counter-1".to_string()).await?;
//! assert_eq!(loaded_counter.state().value, 5);
//! assert_eq!(loaded_counter.version(), 1);
//! # Ok(())
//! # }
//! ```
//!
//! ## Architecture
//!
//! Eventastic is built around four core modules:
//!
//! - **[`aggregate`]** - Domain aggregates that encapsulate business logic and generate events
//! - **[`event`]** - Domain events and event store abstractions for persistence
//! - **[`repository`]** - Transaction-based persistence layer with read/write operations
//! - **[`memory`]** - In-memory implementation for testing and development
//!
//! ### Transaction-First Design
//!
//! Unlike many event sourcing libraries, Eventastic requires transactions for all write
//! operations. This ensures:
//!
//! - **ACID compliance** - All changes are atomic and consistent
//! - **Idempotency** - Duplicate events are detected and handled gracefully
//! - **Side effect reliability** - External operations are processed via outbox pattern
//! - **Optimistic concurrency** - Concurrent modifications are detected and rejected
//!
//! ## Complete Example
//!
//! For an implementation demonstrating all concepts, see the
//! [banking example](https://github.com/jdon/eventastic/tree/main/examples/bank)
//! which shows.

pub mod aggregate;
pub mod event;
pub mod repository;

pub mod memory;

#[cfg(test)]
mod test_fixtures;
