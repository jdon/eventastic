//! Module containing support for the Aggregate pattern.
//!
//! ## What is an Aggregate?
//!
//! An [Aggregate] is the most important concept in your domain.
//!
//! It represents the entities your business domain is composed of,
//! and the business logic your domain is exposing.
//!
//! For example: in an Order Management bounded-context (e.g. a
//! microservice), the concepts of Order or Customer are two potential
//! [Aggregate]s.
//!
//! Aggregates expose mutations with the concept of **commands**:
//! from the previous example, an Order might expose some commands such as
//! _"Add Order Item"_, or _"Remove Order Item"_, or _"Place Order"_
//! to close the transaction.
//!
//! In Event Sourcing, the Aggregate state is modified by the usage of
//! **Domain Events**, which carry some or all the fields in the state
//! in a certain logical meaning.
//!
//! As such, commands in Event Sourcing will **produce** Domain Events.
//!
//! Aggregates should provide a way to **fold** Domain Events on the
//! current value of the state, to produce the next state.

use crate::event::Event;
use std::fmt::Debug;

mod repository;
mod root;

pub use repository::{RepositoryTransaction as EventSourcedRepository, *};
pub use root::*;

#[derive(Debug, Clone)]
pub struct SideEffect<Id, T> {
    pub id: Id,
    pub message: T,
}

/// An Aggregate represents a Domain Model that, through an Aggregate [Root],
/// acts as a _transactional boundary_.
///
/// Aggregates are also used to enforce Domain invariants
/// (i.e. certain constraints or rules that are unique to a specific Domain).
///
/// Since this is an Event-sourced version of the Aggregate pattern,
/// any change to the Aggregate state must be represented through
/// a Domain Event, which is then applied to the current state
/// using the [`Aggregate::apply`] method.
///
/// More on Aggregates can be found here: `<https://www.dddcommunity.org/library/vernon_2011/>`
pub trait Aggregate: Sized + Send + Sync + Clone {
    /// The current version of the snapshot to store.
    /// This should be number should be increased when a breaking change is made to the apply functions.
    const SNAPSHOT_VERSION: u32;

    /// The type used to uniquely identify the Aggregate.
    type AggregateId: Send + Sync + Clone + Debug + Eq + PartialEq;

    /// The type of Domain Events that interest this Aggregate.
    /// Usually, this type should be an `enum`.
    type DomainEvent: Send + Sync + Clone + Debug + Eq + PartialEq + Event<Self::DomainEventId>;

    /// The type used to uniquely identify the a given domain event.
    type DomainEventId: Send + Sync + Clone + Debug + Eq + PartialEq;

    /// The error type that can be returned by [`Aggregate::apply`] when
    /// mutating the Aggregate state.
    type ApplyError: Send + Sync + Debug;

    /// The type of side effect that this aggregate can produce.
    /// Usually, this type should be an `enum`.
    type SideEffectId: Send + Sync + Clone + Debug + Eq + PartialEq;

    /// The type of side effect that this aggregate can produce.
    /// Usually, this type should be an `enum`.
    type SideEffect: Send + Sync + Clone + Debug;

    /// The error type that can be returned when handling a [`Aggregate::SideEffect`]
    type SideEffectError: Send + Sync + Debug;

    /// Returns the unique identifier for the Aggregate instance.
    fn aggregate_id(&self) -> &Self::AggregateId;

    /// Create a new Aggregate through a Domain Event.
    ///
    /// # Errors
    ///
    /// The method can return an error if the event to apply is unexpected
    /// given the current state of the Aggregate.
    fn apply_new(event: &Self::DomainEvent) -> Result<Self, Self::ApplyError>;

    /// Mutates the state of an Aggregate through a Domain Event.
    ///
    /// # Errors
    ///
    /// The method can return an error if the event to apply is unexpected
    /// given the current state of the Aggregate.
    fn apply(&mut self, event: &Self::DomainEvent) -> Result<(), Self::ApplyError>;

    /// Generate a list of side effects for this given aggregate and domain event
    /// The domain event has already been applied to the aggregate
    fn side_effects(
        &self,
        event: &Self::DomainEvent,
    ) -> Option<Vec<SideEffect<Self::SideEffectId, Self::SideEffect>>>;
}
