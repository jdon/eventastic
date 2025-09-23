//! Module containing support for the Aggregate pattern.
//!
//! ## What is an Aggregate?
//!
//! An [`Aggregate`] is the most important concept in your domain.
//!
//! It represents the entities your business domain is composed of,
//! and the business logic your domain is exposing.
//!
//! For example: in an Order Management bounded-context (e.g. a
//! microservice), the concepts of Order or Customer are two potential
//! [`Aggregate`]s.
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
//!
//! ## Aggregates and Events
//!
//! Aggregates consume events to maintain their state:
//!
//! ### **Consuming Events**
//!
//! Aggregates apply events to change their state:
//! - [`Aggregate::apply_new()`] creates new aggregate instances from "creation" events
//! - [`Aggregate::apply()`] modifies existing aggregate state with subsequent events
//! - Both methods validate events against current state and business rules
//!
//! ### **Creating Events**
//!
//! Business logic methods (typically on the aggregate) create events that represent what happened:
//! - Commands are validated and translated into domain events  
//! - Events must implement the [`DomainEvent`] trait for unique identification
//! - Events are recorded via [`Context<T>::record_that()`](crate::aggregate::Context::record_that)
//! - Recorded events are held in [`Context<T>`](crate::aggregate::Context) until persistence
//!
//! For the complete event lifecycle and persistence patterns, see [`crate::event`] and [`crate::repository`].

use crate::event::DomainEvent;
use std::fmt::Debug;

mod root;

pub use root::*;

/// An Aggregate represents a Domain Model that, through an Aggregate [`Root`],
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
pub trait Aggregate: Sized + Clone {
    /// The current version of the aggregate's snapshot format.
    ///
    /// This version number tracks the compatibility of stored snapshots with the current
    /// aggregate implementation. When you make breaking changes to the aggregate structure
    /// or apply logic that would make existing snapshots incompatible, increment this number.
    ///
    /// ## When to increment:
    /// - Adding/removing/renaming fields in the aggregate struct
    /// - Changing field types or serialization format
    /// - Modifying apply logic in ways that change state calculation
    /// - Any change that would cause a stored snapshot to be invalid
    ///
    /// ## How it works:
    /// The repository compares this version against the version stored with each snapshot.
    /// If they don't match, the repository will ignore the incompatible snapshot and
    /// rebuild the aggregate state by replaying all events from the beginning.
    const SNAPSHOT_VERSION: u64;

    /// The type used to uniquely identify the Aggregate.
    type AggregateId: Clone + Debug + Eq + PartialEq;

    /// The type of Domain Events that interest this Aggregate.
    /// Usually, this type should be an `enum`.
    ///
    /// This type must implement the [`DomainEvent`] trait,
    /// which provides unique event identification for idempotency checking.
    ///
    /// See the [`crate::event`] module documentation for guidance on designing domain events.
    type DomainEvent: Clone + Debug + Eq + PartialEq + DomainEvent;

    /// The error type that can be returned by [`Aggregate::apply`] when
    /// mutating the Aggregate state.
    type ApplyError;

    /// The type of side effect that this aggregate can produce.
    /// Usually, this type should be an `enum`.
    type SideEffect: SideEffect;

    /// Returns the unique identifier for the Aggregate instance.
    fn aggregate_id(&self) -> &Self::AggregateId;

    /// Create a new Aggregate through a Domain Event.
    ///
    /// This method takes a [`DomainEvent`] and creates
    /// the initial state of the aggregate. Typically, this should only accept
    /// "creation" events that establish the aggregate's identity.
    ///
    /// # Errors
    ///
    /// The method can return an error if the event to apply is unexpected
    /// given the current state of the Aggregate.
    fn apply_new(event: &Self::DomainEvent) -> Result<Self, Self::ApplyError>;

    /// Mutates the state of an Aggregate through a Domain Event.
    ///
    /// This method takes a [`DomainEvent`] and applies
    /// the change to the aggregate's state. This is where your business logic
    /// validates the event and updates the aggregate accordingly.
    ///
    /// # Errors
    ///
    /// The method can return an error if the event to apply is unexpected
    /// given the current state of the Aggregate.
    fn apply(&mut self, event: &Self::DomainEvent) -> Result<(), Self::ApplyError>;

    /// Generates a list of side effects for this given aggregate and domain event
    ///
    /// The domain event has already been applied to the aggregate
    fn side_effects(&self, event: &Self::DomainEvent) -> Option<Vec<Self::SideEffect>>;
}

pub trait SideEffect {
    /// The type used to uniquely identify this side effect.
    type SideEffectId;

    /// Returns read access to the [`SideEffect::SideEffectId`]
    fn id(&self) -> &Self::SideEffectId;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::*;

    // Tests that Aggregate::apply_new creates a new aggregate from a creation event
    #[test]
    fn test_aggregate_creation() {
        let reset_event = create_reset_event("reset-1", "calc-1");
        let calc = TestCounter::apply_new(&reset_event).unwrap();

        assert_eq!(calc.id, "calc-1");
        assert_eq!(calc.result, 0);
        assert_eq!(calc.operations_count, 0);
    }

    // Tests that Aggregate::apply_new rejects non-creation events
    #[test]
    fn test_aggregate_creation_with_invalid_event() {
        let add_event = create_add_event("add-1", 10);
        let result = TestCounter::apply_new(&add_event);
        assert!(matches!(result, Err(TestError::InvalidOperation)));
    }

    // Tests that Aggregate::apply correctly processes multiple event types and updates state
    #[test]
    fn test_aggregate_apply_events() {
        let reset_event = create_reset_event("reset-1", "calc-1");
        let mut calc = TestCounter::apply_new(&reset_event).unwrap();

        // Apply addition
        let add_event = create_add_event("add-1", 15);
        calc.apply(&add_event).unwrap();
        assert_eq!(calc.result, 15);
        assert_eq!(calc.operations_count, 1);

        // Apply subtraction
        let subtract_event = create_subtract_event("sub-1", 5);
        calc.apply(&subtract_event).unwrap();
        assert_eq!(calc.result, 10);
        assert_eq!(calc.operations_count, 2);

        // Apply multiplication
        let multiply_event = create_multiply_event("mul-1", 3);
        calc.apply(&multiply_event).unwrap();
        assert_eq!(calc.result, 30);
        assert_eq!(calc.operations_count, 3);
    }

    // Tests that Aggregate::apply returns appropriate errors for invalid operations
    #[test]
    fn test_aggregate_apply_error() {
        let reset_event = create_reset_event("reset-1", "calc-1");
        let mut calc = TestCounter::apply_new(&reset_event).unwrap();

        // Try to apply reset to existing aggregate (invalid)
        let another_reset = create_reset_event("reset-2", "calc-2");
        let result = calc.apply(&another_reset);
        assert!(matches!(result, Err(TestError::InvalidOperation)));

        // Try to multiply by zero (invalid)
        let multiply_zero = create_multiply_event("mul-zero", 0);
        let result = calc.apply(&multiply_zero);
        assert!(matches!(result, Err(TestError::DivisionByZero)));
    }

    // Tests that Aggregate::side_effects generates the correct side effects for different events
    #[test]
    fn test_side_effects_generation() {
        let reset_event = create_reset_event("reset-1", "calc-1");
        let calc = TestCounter::apply_new(&reset_event).unwrap();

        // Reset event generates 2 side effects
        let side_effects = calc.side_effects(&reset_event);
        assert!(side_effects.is_some());
        let effects = side_effects.unwrap();
        assert_eq!(effects.len(), 2);

        // Add event generates 1 side effect
        let add_event = create_add_event("add-1", 10);
        let side_effects = calc.side_effects(&add_event);
        assert!(side_effects.is_some());
        let effects = side_effects.unwrap();
        assert_eq!(effects.len(), 1);

        // Multiply event generates no side effects
        let multiply_event = create_multiply_event("mul-1", 2);
        let side_effects = calc.side_effects(&multiply_event);
        assert!(side_effects.is_none());
    }

    // Tests that Aggregate::aggregate_id returns the correct identifier
    #[test]
    fn test_aggregate_id() {
        let reset_event = create_reset_event("reset-1", "my-counter");
        let calc = TestCounter::apply_new(&reset_event).unwrap();
        assert_eq!(calc.aggregate_id(), "my-counter");
    }

    // Tests that the aggregate has the expected SNAPSHOT_VERSION constant
    #[test]
    fn test_snapshot_version() {
        assert_eq!(TestCounter::SNAPSHOT_VERSION, 1);
    }
}
