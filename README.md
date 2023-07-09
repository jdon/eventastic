# Eventastic

This is an opinionated fork of [Eventually-rs](https://github.com/get-eventually/eventually-rs).

Eventastic enforces the use of transactions, handles idempotency and removes command handling abstractions.

## Examples
See full examples in [examples/bank](https://github.com/jdon/eventastic/blob/main/examples/bank/src/main.rs)

```rust
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Setup postgres repo
    let repository = get_repository().await;

    // Run our side effects handler in a background task
    tokio::spawn(async {
        let repository = get_repository().await;

        let _ = repository
            .start_outbox(SideEffectContext {}, std::time::Duration::from_secs(5))
            .await;
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
    // Record takes in the transaction, as it does idempotency checks with the db.
    account
        .record_that(&mut transaction, add_event.clone())
        .await?;

    // Save uncommitted events and side effects in the db.
    transaction.store(&mut account).await?;

    // Commit the transaction
    transaction.commit().await?;

    // Get the aggregate from the db
    let mut transaction = repository.begin_transaction().await?;

    let mut account: Context<Account> = transaction.get(&account_id).await?;

    // Check our balance is correct
    assert_eq!(account.state().balance, 345);

    // Trying to apply the same event id but with different content gives us an IdempotencyError
    let changed_add_event = AccountEvent::Add {
        event_id: add_event_id,
        amount: 123,
    };

    let err = account
        .record_that(&mut transaction, changed_add_event)
        .await
        .expect_err("failed to get error");

    assert!(matches!(err, RecordError::IdempotencyError(_, _)));

    // Applying the already applied event, will be ignored and return Ok
    account.record_that(&mut transaction, add_event).await?;

    transaction.commit().await?;

    let mut transaction = repository.begin_transaction().await?;

    let account: Context<Account> = transaction.get(&account_id).await?;

    // Balance hasn't changed since the event wasn't actually applied
    assert_eq!(account.state().balance, 345);

    println!("Got account {account:?}");

    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    Ok(())
}
```
