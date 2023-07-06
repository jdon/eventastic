# Eventastic

This is an opinionated fork of [Eventually-rs](https://github.com/get-eventually/eventually-rs).

Eventastic enforces the use of transactions, handles idempotency and removes command handling abstractions.

## Examples
See full examples in [examples/bank](https://github.com/jdon/eventastic/blob/main/examples/bank/src/main.rs)

```rust
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
 Ok(())
```
