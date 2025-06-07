use std::str::FromStr;

use eventastic::aggregate::Aggregate;
use eventastic::aggregate::Context;

use eventastic::aggregate::Root;
use eventastic::aggregate::SaveError;
use eventastic::aggregate::SideEffect;
use eventastic::event::Event;
use eventastic_outbox_postgres::{RepositoryOutboxExt, SideEffectHandler, TableOutbox};
use eventastic_postgres::PostgresRepository;
use eventastic_postgres::RootExt;
use serde::Deserialize;
use serde::Serialize;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use thiserror::Error;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Setup postgres repo
    let repository = get_repository().await;

    //Migrate the db

    repository.run_migrations().await?;

    // Run our side effect handler in the background
    tokio::spawn({
        let repo = repository.clone();
        async move {
            let _ = repo
                .start_outbox(SideEffectContext {}, std::time::Duration::from_secs(5))
                .await;
        }
    });

    // Start transaction
    let mut transaction = repository.begin_transaction().await?;

    let account_id = Uuid::new_v4();

    let event_id = Uuid::new_v4();

    let add_event_id = Uuid::new_v4();

    // Open a bank account
    let event = AccountEvent::Open {
        event_id,
        account_id,
        starting_balance: 21,
        email: "user@example.com".into(),
    };

    let mut account = Account::record_new(event)?;

    // Add funds to newly created account
    let add_event = AccountEvent::Add {
        event_id: add_event_id,
        amount: 324,
    };

    // Record add fund events.
    account.record_that(add_event.clone())?;

    // Save uncommitted events and side effects in the db.
    account.save(&mut transaction).await?;

    // Commit the transaction
    transaction.commit().await?;

    // Get the aggregate from the db
    let mut transaction = repository.begin_transaction().await?;

    let mut account = Account::load(&mut transaction, account_id).await?;

    // Check our balance is correct
    assert_eq!(account.state().balance, 345);

    // Trying to apply the same event id but with different content gives us an IdempotencyError
    let changed_add_event = AccountEvent::Add {
        event_id: add_event_id,
        amount: 123,
    };

    account.record_that(changed_add_event)?;

    // Applying the already applied event, will be ignored and return Ok
    let error = account
        .save(&mut transaction)
        .await
        .expect_err("Failed to get idempotency error");

    assert!(matches!(error, SaveError::IdempotencyError(_, _)));

    transaction.commit().await?;

    let mut transaction = repository.begin_transaction().await?;

    let mut transaction_2 = repository.begin_transaction().await?;

    let mut old_account_version: Context<Account> =
        Context::load(&mut transaction_2, &account_id).await?;

    let mut account: Context<Account> = Context::load(&mut transaction, &account_id).await?;

    // Balance hasn't changed since the event wasn't actually applied
    assert_eq!(account.state().balance, 345);

    println!("Got account {account:?}");

    // Apply a new add event and save to our db. This should have version number 2
    let add_event = AccountEvent::Add {
        event_id: Uuid::new_v4(),
        amount: 456,
    };

    account.record_that(add_event)?;
    account.save(&mut transaction).await?;
    transaction.commit().await?;

    // Attempt to apply another event to our aggregate, but with an out of date version number
    // This happens normally when two applies are executed concurrently
    // This will attempt to apply a different event with a version number 2
    // This should fail with an optimistic concurrency error
    let add_event = AccountEvent::Add {
        event_id: Uuid::new_v4(),
        amount: 789,
    };

    old_account_version.record_that(add_event)?;

    let err = old_account_version
        .save(&mut transaction_2)
        .await
        .expect_err("Failed to get optimistic concurrency error");

    assert!(matches!(
        err,
        SaveError::OptimisticConcurrency(id, version) if id == account_id && version == 2
    ));

    transaction_2.commit().await?;

    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    Ok(())
}

// Define our aggregate
#[derive(Clone, Serialize, Deserialize, Debug)]
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

    fn id(&self) -> &Self::Id {
        match self {
            SideEffects::PublishMessage { id, .. } | SideEffects::SendEmail { id, .. } => id,
        }
    }
}

pub struct SideEffectContext;

#[async_trait::async_trait]
impl SideEffectHandler for SideEffectContext {
    type SideEffect = SideEffects;
    type Error = ();

    async fn handle(&self, msg: &SideEffects, retries: u16) -> Result<(), (bool, Self::Error)> {
        println!("handling side effect {:?} retries {}", msg, retries);
        Ok(())
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

async fn get_repository() -> PostgresRepository<TableOutbox> {
    let connection_options =
        PgConnectOptions::from_str("postgres://postgres:password@localhost/postgres").unwrap();

    let pool_options = PoolOptions::default();

    PostgresRepository::new(connection_options, pool_options, TableOutbox)
        .await
        .unwrap()
}
