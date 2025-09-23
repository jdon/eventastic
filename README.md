# Eventastic

A type-safe event sourcing and CQRS library for Rust with PostgreSQL persistence.

## Features

- **Strongly-typed aggregates and events** - Define your domain model with Rust structs and enums
- **Mandatory transactions** for ACID guarantees
- **Built-in idempotency** prevents duplicate event processing
- **Optimistic concurrency control** detects conflicting modifications
- **Transactional outbox pattern** for reliable side effects
- **Snapshot optimization** for fast aggregate loading
- **In-memory repository** for testing and development

## Quick Start

Define your domain aggregate and events:

```rust
use eventastic::aggregate::{Aggregate, Context, Root, SideEffect};
use eventastic::event::DomainEvent;
use eventastic::memory::InMemoryRepository;
use eventastic::repository::Repository;

#[derive(Clone, Debug)]
struct BankAccount {
    id: String,
    balance: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AccountEvent {
    Opened { event_id: String, account_id: String, initial_balance: i64 },
    Deposited { event_id: String, amount: i64 },
    Withdrawn { event_id: String, amount: i64 },
}

impl DomainEvent for AccountEvent {
    type EventId = String;
    fn id(&self) -> &Self::EventId {
        match self {
            AccountEvent::Opened { event_id, .. } => event_id,
            AccountEvent::Deposited { event_id, .. } => event_id,
            AccountEvent::Withdrawn { event_id, .. } => event_id,
        }
    }
}

// Define a no-op side effect type
#[derive(Clone, Debug, PartialEq, Eq)]
struct NoSideEffect;

impl SideEffect for NoSideEffect {
    type SideEffectId = String;
    fn id(&self) -> &Self::SideEffectId {
        unreachable!("No side effects are produced")
    }
}

impl Aggregate for BankAccount {
    const SNAPSHOT_VERSION: u64 = 1;
    type AggregateId = String;
    type DomainEvent = AccountEvent;
    type ApplyError = String;
    type SideEffect = NoSideEffect;

    fn aggregate_id(&self) -> &Self::AggregateId {
        &self.id
    }

    fn apply_new(event: &Self::DomainEvent) -> Result<Self, Self::ApplyError> {
        match event {
            AccountEvent::Opened { account_id, initial_balance, .. } => {
                Ok(BankAccount {
                    id: account_id.clone(),
                    balance: *initial_balance,
                })
            }
            _ => Err("Account must be opened first".to_string()),
        }
    }

    fn apply(&mut self, event: &Self::DomainEvent) -> Result<(), Self::ApplyError> {
        match event {
            AccountEvent::Opened { .. } => Err("Account already exists".to_string()),
            AccountEvent::Deposited { amount, .. } => {
                self.balance += amount;
                Ok(())
            }
            AccountEvent::Withdrawn { amount, .. } => {
                if self.balance >= *amount {
                    self.balance -= amount;
                    Ok(())
                } else {
                    Err("Insufficient funds".to_string())
                }
            }
        }
    }

    fn side_effects(&self, _event: &Self::DomainEvent) -> Option<Vec<Self::SideEffect>> {
        None
    }
}
```

Use the aggregate with transactions:

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let repository = InMemoryRepository::<BankAccount>::new();

    // Create new account using the Root trait
    let mut account: Context<BankAccount> = BankAccount::record_new(
        AccountEvent::Opened {
            event_id: "evt-1".to_string(),
            account_id: "acc-123".to_string(),
            initial_balance: 1000,
        }
    )?;

    // Deposit money
    account.record_that(AccountEvent::Deposited {
        event_id: "evt-2".to_string(),
        amount: 500,
    })?;

    // Save with transaction
    let mut transaction = repository.begin_transaction().await?;
    transaction.store(&mut account).await?;
    transaction.commit()?;

    // Load account
    let loaded_account = repository.load(&"acc-123".to_string()).await?;
    assert_eq!(loaded_account.state().balance, 1500);

    Ok(())
}
```

## Architecture

Eventastic is built around four core concepts:

- **Aggregates** - Domain entities that apply events to update their state
- **Events** - Immutable records of what happened in your domain
- **Context** - Wrapper that tracks aggregate state and uncommitted events
- **Repository** - Persistence layer with transactional guarantees

## Why Eventastic?

### Transaction-First Design

Unlike many event sourcing libraries, Eventastic requires transactions for all write operations. This provides:

- **ACID compliance** - All changes are atomic and consistent
- **Idempotency** - Duplicate events are detected and handled gracefully
- **Concurrency safety** - Optimistic locking prevents data races
- **Side effect reliability** - External operations are processed via outbox pattern

### Rust Benefits

Using Rust provides compile-time guarantees:

- Events must implement required traits (DomainEvent, Clone, etc.)
- Aggregates must handle all event types in match statements
- Error handling is explicit with Result types
- No null pointer exceptions or runtime type errors

### Production Ready

Eventastic includes features needed for production systems:

- Automatic snapshot creation and loading
- Comprehensive error types with structured information
- Transaction-based consistency guarantees

## Persistence

The library provides multiple repository implementations:

- `eventastic::memory::InMemoryRepository` - For testing and development
- `eventastic_postgres::PostgresRepository` - For production PostgreSQL storage with:
  - Event and snapshot storage with versioning
  - Full transaction support with optimistic concurrency control
  - Optional encryption for sensitive data
  - Database migrations support
- `eventastic_outbox_postgres::TableOutbox` - Transactional outbox pattern for reliable side effect processing

## Examples

See the `examples/` directory for complete implementations:

- **Bank** - Full banking domain demonstrating:
  - Account creation and management
  - Transaction processing
  - Side effects via outbox pattern
  - Idempotency and concurrency handling