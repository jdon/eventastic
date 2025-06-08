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

    /// The version of the event
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

/// A domain event represents something significant that happened in your domain.
///
/// Domain events are the building blocks of event sourcing. They capture
/// business-relevant facts that have occurred and are used to reconstruct
/// aggregate state through event replay.
///
/// # Example
///
/// ```rust,ignore
/// use eventastic::event::DomainEvent;
///
/// #[derive(Clone, PartialEq, Eq)]
/// enum OrderEvent {
///     Created { order_id: String, customer_id: String },
///     ItemAdded { item_id: String, quantity: u32 },
///     Shipped { tracking_number: String },
/// }
///
/// impl DomainEvent for OrderEvent {
///     type EventId = String;
///     
///     fn id(&self) -> &Self::EventId {
///         // Return unique identifier for this event instance
///         todo!()
///     }
/// }
/// ```
pub trait DomainEvent: Clone + Eq + PartialEq {
    /// The type used to uniquely identify this event.
    ///
    /// This is typically a UUID, String, or other unique identifier type
    /// that can distinguish this specific event instance from all others.
    type EventId: Debug + Clone + Eq + PartialEq;

    /// Returns the unique identifier for this event instance.
    ///
    /// Each event must have a unique ID that distinguishes it from all other
    /// events. This ID is used for idempotency checks and event deduplication.
    fn id(&self) -> &Self::EventId;
}
