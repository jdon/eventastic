//! Shared test fixtures for eventastic tests.
//!
//! This module provides common test aggregates, events, and side effects
//! that can be used across different test modules to ensure consistency
//! and reduce code duplication.

use crate::{
    aggregate::{Aggregate, SideEffect},
    event::DomainEvent,
};

/// A test counter aggregate for use in tests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TestCounter {
    pub id: String,
    pub result: i32,
    pub operations_count: i32,
}

/// Test events for the TestCounter aggregate.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TestEvent {
    Reset {
        event_id: String,
        counter_id: String,
    },
    Add {
        event_id: String,
        value: i32,
    },
    Subtract {
        event_id: String,
        value: i32,
    },
    Multiply {
        event_id: String,
        value: i32,
    },
}

/// Test errors for the TestCounter aggregate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TestError {
    InvalidOperation,
    DivisionByZero,
}

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestError::InvalidOperation => write!(f, "Invalid operation"),
            TestError::DivisionByZero => write!(f, "Division by zero"),
        }
    }
}

impl std::error::Error for TestError {}

/// Test side effects for the TestCounter aggregate.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TestSideEffect {
    LogOperation { id: String, operation: String },
    NotifyUser { id: String, message: String },
}

impl DomainEvent for TestEvent {
    type EventId = String;

    fn id(&self) -> &Self::EventId {
        match self {
            TestEvent::Reset { event_id, .. } => event_id,
            TestEvent::Add { event_id, .. } => event_id,
            TestEvent::Subtract { event_id, .. } => event_id,
            TestEvent::Multiply { event_id, .. } => event_id,
        }
    }
}

impl SideEffect for TestSideEffect {
    type SideEffectId = String;

    fn id(&self) -> &Self::SideEffectId {
        match self {
            TestSideEffect::LogOperation { id, .. } => id,
            TestSideEffect::NotifyUser { id, .. } => id,
        }
    }
}

impl Aggregate for TestCounter {
    const SNAPSHOT_VERSION: u64 = 1;
    type AggregateId = String;
    type DomainEvent = TestEvent;
    type ApplyError = TestError;
    type SideEffect = TestSideEffect;

    fn aggregate_id(&self) -> &Self::AggregateId {
        &self.id
    }

    fn apply_new(event: &Self::DomainEvent) -> Result<Self, Self::ApplyError> {
        match event {
            TestEvent::Reset { counter_id, .. } => Ok(TestCounter {
                id: counter_id.clone(),
                result: 0,
                operations_count: 0,
            }),
            _ => Err(TestError::InvalidOperation),
        }
    }

    fn apply(&mut self, event: &Self::DomainEvent) -> Result<(), Self::ApplyError> {
        match event {
            TestEvent::Reset { .. } => Err(TestError::InvalidOperation),
            TestEvent::Add { value, .. } => {
                self.result += value;
                self.operations_count += 1;
                Ok(())
            }
            TestEvent::Subtract { value, .. } => {
                self.result -= value;
                self.operations_count += 1;
                Ok(())
            }
            TestEvent::Multiply { value, .. } => {
                if *value == 0 {
                    return Err(TestError::DivisionByZero);
                }
                self.result *= value;
                self.operations_count += 1;
                Ok(())
            }
        }
    }

    fn side_effects(&self, event: &Self::DomainEvent) -> Option<Vec<Self::SideEffect>> {
        match event {
            TestEvent::Reset { event_id, .. } => Some(vec![
                TestSideEffect::LogOperation {
                    id: format!("{event_id}-log"),
                    operation: "Reset".to_string(),
                },
                TestSideEffect::NotifyUser {
                    id: format!("{event_id}-notify"),
                    message: "Counter has been reset".to_string(),
                },
            ]),
            TestEvent::Add { event_id, value } => Some(vec![TestSideEffect::LogOperation {
                id: format!("{event_id}-log"),
                operation: format!("Add {value}"),
            }]),
            TestEvent::Subtract { event_id, value } => Some(vec![TestSideEffect::LogOperation {
                id: format!("{event_id}-log"),
                operation: format!("Subtract {value}"),
            }]),
            TestEvent::Multiply { .. } => None, // No side effects for multiply
        }
    }
}

/// Helper function to create a basic reset event.
pub fn create_reset_event(event_id: &str, counter_id: &str) -> TestEvent {
    TestEvent::Reset {
        event_id: event_id.to_string(),
        counter_id: counter_id.to_string(),
    }
}

/// Helper function to create an add event.
pub fn create_add_event(event_id: &str, value: i32) -> TestEvent {
    TestEvent::Add {
        event_id: event_id.to_string(),
        value,
    }
}

/// Helper function to create a subtract event.
pub fn create_subtract_event(event_id: &str, value: i32) -> TestEvent {
    TestEvent::Subtract {
        event_id: event_id.to_string(),
        value,
    }
}

/// Helper function to create a multiply event.
pub fn create_multiply_event(event_id: &str, value: i32) -> TestEvent {
    TestEvent::Multiply {
        event_id: event_id.to_string(),
        value,
    }
}
