# Eventastic

A type-safe event sourcing and CQRS library for Rust with PostgreSQL persistence. 

Eventastic provides strong consistency guarantees through mandatory transactions, built-in idempotency checking, and reliable side effect processing via the transactional outbox pattern. Designed for building event-driven systems that require data integrity and audit trails.

## Examples
See full examples in [examples/bank](https://github.com/jdon/eventastic/blob/main/examples/bank/src/main.rs)

```rust
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Setup postgres repo
    let repository = get_repository().await;

    // Migrate the db
    repository.run_migrations().await?;

    // Run our side effects handler in a background task
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

    // Record add fund events (events are applied in-memory)
    account.record_that(add_event.clone())?;

    // Save uncommitted events and side effects in the db.
    transaction.store(&mut account).await?;

    // Commit the transaction
    transaction.commit().await?;

    // Get the aggregate from the db using a transaction (read-write access)
    let mut transaction = repository.begin_transaction().await?;

    let mut account = Account::load_with_transaction(&mut transaction, account_id).await?;

    // Check our balance is correct
    assert_eq!(account.state().balance, 345);

    // Demonstrate loading without a transaction (read-only access, more efficient)
    let account_readonly: Context<Account> = repository.load(&account_id).await?;
    assert_eq!(account_readonly.state().balance, 345);
    println!("Successfully loaded account with non-transactional method");

    // Trying to apply the same event id but with different content gives us an IdempotencyError
    let changed_add_event = AccountEvent::Add {
        event_id: add_event_id,
        amount: 123,
    };

    account.record_that(changed_add_event)?;

    // Idempotency errors occur when storing, not when recording events
    let err = transaction
        .store(&mut account)
        .await
        .expect_err("Failed to get idempotency error");

    assert!(matches!(err, SaveError::IdempotencyError(_, _)));

    transaction.commit().await?;

    let mut transaction = repository.begin_transaction().await?;

    let account: Context<Account> = transaction.get(&account_id).await?;

    // Balance hasn't changed since the event wasn't actually applied
    assert_eq!(account.state().balance, 345);

    println!("Got account {account:?}");
    Ok(())
}
