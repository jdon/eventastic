use async_trait::async_trait;
use eventastic::aggregate::Aggregate;
use eventastic::aggregate::SideEffect;
use eventastic::aggregate::SideEffectHandler;
use eventastic::event::Event;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

// Define our aggregate
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Account {
    pub account_id: Uuid,
    balance: i64,
}

// Define our domain events
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub enum AccountEvent {
    Open {
        account_id: Uuid,
        event_id: Uuid,
        email: String,
        starting_balance: i64,
    },
    Add {
        event_id: Uuid,
        amount: i64,
    },
    Remove {
        event_id: Uuid,
        amount: i64,
    },
}

impl Event<Uuid> for AccountEvent {
    fn id(&self) -> &Uuid {
        match self {
            AccountEvent::Open { event_id, .. }
            | AccountEvent::Add { event_id, .. }
            | AccountEvent::Remove { event_id, .. } => event_id,
        }
    }
}

// Define our domain error
// Generally it's expected that applying an event is infallible as the business logic should be done in the command handlers
// But some events could cause an error and returning an error is probably better than panicking
#[derive(Error, Debug)]
pub enum DomainError {
    #[error("This event can't be applied given the current state of the aggregate")]
    InvalidState,
}

// Define our side effects
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub enum SideEffects {
    PublishMessage {
        id: Uuid,
        message: String,
    },
    SendEmail {
        id: Uuid,
        address: String,
        content: String,
    },
}

impl SideEffect for SideEffects {
    /// The type used to uniquely identify this side effect.
    type Id = Uuid;
    /// The error type that can be returned when calling a [`SideEffectHandler::handle`]
    type Error = SideEffectError;

    fn id(&self) -> &Self::Id {
        match self {
            SideEffects::PublishMessage { id, .. } | SideEffects::SendEmail { id, .. } => id,
        }
    }
}

// Define our side effect errors
#[derive(Error, Debug)]
pub enum SideEffectError {
    #[error("Failed to publish message")]
    PublishMessageError,
}

pub struct SideEffectContext {}

#[async_trait]
impl SideEffectHandler for SideEffectContext {
    type SideEffect = SideEffects;

    /// Handle a side effect
    /// If Ok(()) is returned, the side effect is complete and it will be deleted from the repository.
    /// If Err((true, Error)) is returned, the side effect be will requeued
    /// if Err((false, Error)) is returned, the side effect won't be requeued
    async fn handle(&self, msg: &SideEffects, retires: u16) -> Result<(), (bool, SideEffectError)> {
        println!("Got side effect message {msg:?} with retires {retires}");
        let requeue = retires < 3;

        Err((requeue, SideEffectError::PublishMessageError))
    }
}

// Implement the aggregate trait for our aggregate struct
impl Aggregate for Account {
    /// The current version of the snapshot to store.
    /// This should be number should be increased when a breaking change is made to the apply functions.
    const SNAPSHOT_VERSION: u64 = 1;

    /// The type used to uniquely identify the Aggregate.
    type AggregateId = Uuid;

    /// The type of Domain Events that interest this Aggregate.
    /// Usually, this type should be an `enum`.
    type DomainEvent = AccountEvent;

    /// The type used to uniquely identify the a given domain event.
    type DomainEventId = Uuid;

    /// The error type that can be returned by [`Aggregate::apply`] when
    /// mutating the Aggregate state.
    type ApplyError = DomainError;

    /// The type of side effect that this aggregate can produce.
    /// Usually, this type should be an `enum`.
    type SideEffect = SideEffects;

    /// Returns the unique identifier for the Aggregate instance.
    fn aggregate_id(&self) -> &Self::AggregateId {
        &self.account_id
    }

    /// Mutates the state of an Aggregate through a Domain Event.
    ///
    /// # Errors
    ///
    /// The method can return an error if the event to apply is unexpected
    /// given the current state of the Aggregate.
    fn apply(&mut self, event: &Self::DomainEvent) -> Result<(), Self::ApplyError> {
        match event {
            AccountEvent::Add { amount, .. } => {
                self.balance += amount;
            }
            AccountEvent::Remove { amount, .. } => {
                self.balance -= amount;
            }
            AccountEvent::Open { .. } => return Err(Self::ApplyError::InvalidState),
        }
        Ok(())
    }

    /// Create a new Aggregate through a Domain Event.
    ///
    /// # Errors
    ///
    /// The method can return an error if the event to apply is unexpected
    /// given the current state of the Aggregate.
    fn apply_new(event: &Self::DomainEvent) -> Result<Self, Self::ApplyError> {
        match event {
            AccountEvent::Open {
                account_id,
                starting_balance,
                ..
            } => Ok(Self {
                account_id: *account_id,
                balance: *starting_balance,
            }),
            AccountEvent::Add { .. } | AccountEvent::Remove { .. } => {
                Err(Self::ApplyError::InvalidState)
            }
        }
    }

    /// Generates a list of side effects for this given aggregate and domain event
    /// The domain event has already been applied to the aggregate
    fn side_effects(&self, event: &Self::DomainEvent) -> Option<Vec<Self::SideEffect>> {
        let side_effect = match event {
            AccountEvent::Open {
                account_id,
                event_id,
                email,
                starting_balance,
            } => Some(SideEffects::SendEmail {
                id: *event_id,
                address: email.clone(),
                content: format!(
                    "Account opened with id {account_id} and starting balance {starting_balance}"
                ),
            }),
            AccountEvent::Add { .. } | AccountEvent::Remove { .. } => None,
        };
        side_effect.map(|s| vec![s])
    }
}
