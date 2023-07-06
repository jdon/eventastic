use std::str::FromStr;

use eventastic::aggregate::Aggregate;
use eventastic::aggregate::Context;
use eventastic::aggregate::EventSourcedRepository;
use eventastic::aggregate::RecordError;
use eventastic::aggregate::Root;
use eventastic::event::Event;
use eventastic_postgres::PostgresRepository;

use serde::Deserialize;
use serde::Serialize;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use thiserror::Error;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Setup postgres repo

    let connection_options =
        PgConnectOptions::from_str("postgres://postgres:password@localhost/postgres")?;

    let pool_options = PoolOptions::default();

    let repository = PostgresRepository::new(connection_options, pool_options).await?;

    // Start transaction
    let mut transaction = repository.transaction().await?;

    let account_id = Uuid::new_v4();

    let event_id = Uuid::new_v4();

    let add_event_id = Uuid::new_v4();

    // Open bank account

    let event = AccountEvent::Open(event_id, account_id, 21);

    let mut account = Account::record_new(event)?;

    // Add funds to newly created event
    let add_event = AccountEvent::Add(add_event_id, 324);

    // Record takes in the transaction, as it does idempotency checks with the db.
    account
        .record_that(&mut transaction, add_event.clone())
        .await?;

    // Save uncommitted events in the db.
    transaction.store(&mut account).await?;

    // Since we have access to the transaction
    // We could use a transactional outbox to store our side effects

    // Commit the transaction
    transaction.commit().await?;

    // Get the aggregate from the db
    let mut transaction = repository.transaction().await?;

    let mut account: Context<Account> = transaction.get(&account_id).await?;

    assert_eq!(account.state().balance, 345);

    // Trying to apply the same event id but with different content gives us an IdempotencyError
    let changed_add_event = AccountEvent::Add(add_event_id, 123);

    let err = account
        .record_that(&mut transaction, changed_add_event)
        .await
        .expect_err("failed to get error");

    assert!(matches!(err, RecordError::IdempotencyError(_, _)));

    // Applying the already applied event, will be ignored and return Ok
    account.record_that(&mut transaction, add_event).await?;

    // Balance hasn't changed since the event wasn't actually applied
    assert_eq!(account.state().balance, 345);

    println!("Got account {account:?}");

    transaction.commit().await?;

    Ok(())
}

// Define our aggregate
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Account {
    pub account_id: Uuid,
    balance: u64,
}

// Define our domain events
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub enum AccountEvent {
    Open(Uuid, Uuid, u64),
    Add(Uuid, u64),
    Remove(Uuid, u64),
}

impl Event<Uuid> for AccountEvent {
    fn id(&self) -> &Uuid {
        match self {
            AccountEvent::Open(id, _, _)
            | AccountEvent::Add(id, _)
            | AccountEvent::Remove(id, _) => id,
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

// Implement the aggregate trait for our aggregate struct
impl Aggregate for Account {
    /// The current version of the snapshot to store.
    /// This should be number should be increased when a breaking change is made to the apply functions.
    const SNAPSHOT_VERSION: u32 = 1;

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
            AccountEvent::Add(_, amount) => {
                self.balance += amount;
            }
            AccountEvent::Remove(_, amount) => {
                self.balance -= amount;
            }
            AccountEvent::Open(_, _, _) => return Err(Self::ApplyError::InvalidState),
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
            AccountEvent::Open(_, aggregate_id, opening_balance) => Ok(Self {
                account_id: *aggregate_id,
                balance: *opening_balance,
            }),
            AccountEvent::Add(_, _) | AccountEvent::Remove(_, _) => {
                Err(Self::ApplyError::InvalidState)
            }
        }
    }
}
