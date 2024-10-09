//! Module `event` contains types and abstractions helpful for working
//! with Domain Events.

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// An [`Event`] that will be / has been persisted to the Event Store.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventStoreEvent<Id, Evt>
where
    Id: Debug,
    Evt: Clone + Eq + PartialEq,
{
    /// The id of the event
    pub id: Id,

    // The version of the event
    pub version: u64,

    /// The actual Domain Event.
    pub event: Evt,
}

/// A domain event.
pub trait Event<Id>
where
    Id: Debug,
{
    fn id(&self) -> &Id;
}

impl<Id, Evt> Event<Id> for EventStoreEvent<Id, Evt>
where
    Id: Debug,
    Evt: Clone + Eq + PartialEq,
{
    fn id(&self) -> &Id {
        &self.id
    }
}
