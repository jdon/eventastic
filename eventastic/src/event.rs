//! Module `event` contains types and abstractions helpful for working
//! with Domain Events.

use std::fmt::Debug;

use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};

use crate::Version;

/// An [`Event`] that will be / has been persisted to the Event Store.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventStoreEvent<Id, Evt>
where
    Id: Send + Debug,
    Self: Send + Sync,
    Evt: Send + Sync + Clone + Eq + PartialEq,
{
    /// The id of the event
    pub id: Id,

    // The version of the event
    pub version: Version,

    /// The actual Domain Event.
    pub event: Evt,
}

/// A domain event.
pub trait Event<Id>
where
    Id: Send + Debug,
{
    fn id(&self) -> &Id;
}

impl<Id, Evt> Event<Id> for EventStoreEvent<Id, Evt>
where
    Id: Send + Debug,
    Self: Send + Sync,
    Evt: Send + Sync + Clone + Eq + PartialEq,
{
    fn id(&self) -> &Id {
        &self.id
    }
}

/// Stream is a stream of [`EventStoreEvent`] Domain Events.
pub type Stream<'a, Id, Evt, Err> = BoxStream<'a, Result<EventStoreEvent<Id, Evt>, Err>>;
