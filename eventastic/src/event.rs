//! Module `event` contains types and abstractions helpful for working
//! with Domain Events.

use std::fmt::Debug;

/// A [`DomainEvent`] that will be / has been persisted to the Event Store.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EventStoreEvent<Evt>
where
    Evt: DomainEvent,
{
    /// The id of the event
    pub id: Evt::EventId,

    // The version of the event
    pub version: u64,

    /// The actual Domain Event.
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

/// A domain event.
pub trait DomainEvent: Clone + Eq + PartialEq {
    type EventId: Debug + Clone + Eq + PartialEq;
    fn id(&self) -> &Self::EventId;
}
