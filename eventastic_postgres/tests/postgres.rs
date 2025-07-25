mod common;

use common::helpers::{AccountBuilder, get_latest_event_timestamp, get_repository, load_account};
use common::test_aggregate::AccountEvent;
use eventastic::aggregate::Context;
use eventastic::repository::RepositoryReader;
use futures::StreamExt;
use uuid::Uuid;

use crate::common::helpers::create_account_with_many_events;
use crate::common::test_aggregate::Account;

#[tokio::test]
pub async fn aggregate_is_successfully_saved_and_loaded() {
    // Arrange
    let repository = get_repository().await;
    let mut account = AccountBuilder::new().build();
    let account_id = account.state().account_id;

    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Act
    account
        .save(&mut transaction)
        .await
        .expect("Failed to save account");

    let created_account = account.state();
    transaction
        .commit()
        .await
        .expect("Failed to commit transaction");

    // Assert

    let loaded_account = load_account(account_id).await;
    let loaded_account = loaded_account.state();

    assert_eq!(created_account, loaded_account);
}

#[tokio::test]
pub async fn aggregate_is_not_saved_if_no_events_are_applied() {
    // Arrange
    let repository = get_repository().await;
    let mut account = AccountBuilder::new().save().await;
    let account_id = account.state().account_id;

    let event_time_stamp = get_latest_event_timestamp(account_id).await;

    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Act
    account
        .save(&mut transaction)
        .await
        .expect("Failed to save account");

    transaction
        .commit()
        .await
        .expect("Failed to commit transaction");

    // Assert

    assert_eq!(
        event_time_stamp,
        get_latest_event_timestamp(account_id).await
    );
}

#[tokio::test]
pub async fn transaction_rollback_discards_changes() {
    // Arrange
    let repository = get_repository().await;
    let mut account = AccountBuilder::new().build();
    let account_id = account.state().account_id;

    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Apply event and save to the transaction but don't commit
    account
        .record_that(AccountEvent::Add {
            event_id: Uuid::new_v4(),
            amount: 100,
        })
        .expect("Failed to apply event");

    account
        .save(&mut transaction)
        .await
        .expect("Failed to save account");

    // Roll back the transaction instead of committing
    transaction
        .rollback()
        .await
        .expect("Failed to rollback transaction");

    // Act - Try to load the account from the database
    // Start a new transaction
    let mut load_transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Assert
    // The account should not exist in the database since we rolled back
    let load_result =
        Context::<common::test_aggregate::Account>::load(&mut load_transaction, &account_id).await;
    assert!(
        load_result.is_err(),
        "Account should not exist after rollback"
    );

    load_transaction
        .commit()
        .await
        .expect("Failed to commit transaction");
}

#[tokio::test]
pub async fn transaction_isolates_changes_until_commit() {
    // Arrange
    let repository = get_repository().await;

    // Create an account in one transaction
    let mut account = AccountBuilder::new().build();
    let account_id = account.state().account_id;

    // Record an event that adds funds
    account
        .record_that(AccountEvent::Add {
            event_id: Uuid::new_v4(),
            amount: 50,
        })
        .expect("Failed to apply event");

    // Begin a transaction to save the account with the event
    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Save but don't commit yet
    account
        .save(&mut transaction)
        .await
        .expect("Failed to save account");

    // Act - Try to read the account from a different transaction before committing
    let mut concurrent_transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // The account should not be visible in the other transaction yet
    let load_result =
        Context::<common::test_aggregate::Account>::load(&mut concurrent_transaction, &account_id)
            .await;

    // Assert
    assert!(
        load_result.is_err(),
        "Account should not be visible in another transaction before commit"
    );

    concurrent_transaction
        .commit()
        .await
        .expect("Failed to commit concurrent transaction");

    // Now commit the original transaction
    transaction
        .commit()
        .await
        .expect("Failed to commit transaction");

    // Verify the account is now visible
    let mut verification_transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    let loaded_account = Context::<common::test_aggregate::Account>::load(
        &mut verification_transaction,
        &account_id,
    )
    .await
    .expect("Failed to load account after commit");

    assert_eq!(loaded_account.state().balance, 50);

    verification_transaction
        .commit()
        .await
        .expect("Failed to commit verification transaction");
}

#[tokio::test]
pub async fn transaction_handles_invalid_data_gracefully() {
    // Arrange
    let repository = get_repository().await;

    // Create a valid account first
    let mut account = AccountBuilder::new().save().await;
    let account_id = account.state().account_id;

    // Begin a transaction
    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Try to apply an invalid event (Open event on an existing account)
    // This should be rejected by the apply() method in the Account aggregate
    account
        .record_that(AccountEvent::Open {
            account_id,
            event_id: Uuid::new_v4(),
            email: "test@example.com".to_string(),
            starting_balance: 50,
        })
        .expect_err("Should fail to apply Open event on existing account");

    // The account should remain unchanged and still be savable
    account
        .save(&mut transaction)
        .await
        .expect("Failed to save account");

    transaction
        .commit()
        .await
        .expect("Failed to commit transaction");

    // Verify the account is still in its original state
    let loaded_account = load_account(account_id).await;
    assert_eq!(account.state().balance, loaded_account.state().balance);
}

#[tokio::test]
pub async fn transaction_handles_multiple_operations_correctly() {
    // Arrange
    let repository = get_repository().await;

    // Create an account with initial events
    let mut account = AccountBuilder::new().with_add_event(100).save().await;
    let account_id = account.state().account_id;
    let initial_balance = account.state().balance;

    // Start a transaction that will perform multiple operations
    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // First operation: Add funds
    account
        .record_that(AccountEvent::Add {
            event_id: Uuid::new_v4(),
            amount: 50,
        })
        .expect("Failed to apply Add event");

    // Save the changes but keep the transaction open
    account
        .save(&mut transaction)
        .await
        .expect("Failed to save account after first operation");

    // Second operation: Remove funds
    account
        .record_that(AccountEvent::Remove {
            event_id: Uuid::new_v4(),
            amount: 25,
        })
        .expect("Failed to apply Remove event");

    // Save again in the same transaction
    account
        .save(&mut transaction)
        .await
        .expect("Failed to save account after second operation");

    // Finally commit the transaction
    transaction
        .commit()
        .await
        .expect("Failed to commit transaction");

    // Verify that both operations were applied correctly
    let loaded_account = load_account(account_id).await;
    assert_eq!(
        initial_balance + 50 - 25,
        loaded_account.state().balance,
        "Account balance should reflect all operations in the transaction"
    );
}

#[tokio::test]
pub async fn transaction_handles_multiple_aggregates_correctly() {
    // Arrange
    let repository = get_repository().await;

    // Create two separate accounts
    let mut account1 = AccountBuilder::new().save().await;
    let account_id1 = account1.state().account_id;

    let mut account2 = AccountBuilder::new().save().await;
    let account_id2 = account2.state().account_id;

    // Apply changes to both accounts in the same transaction
    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Update first account
    account1
        .record_that(AccountEvent::Add {
            event_id: Uuid::new_v4(),
            amount: 100,
        })
        .expect("Failed to apply event to first account");

    account1
        .save(&mut transaction)
        .await
        .expect("Failed to save first account");

    // Update second account in the same transaction
    account2
        .record_that(AccountEvent::Add {
            event_id: Uuid::new_v4(),
            amount: 200,
        })
        .expect("Failed to apply event to second account");

    account2
        .save(&mut transaction)
        .await
        .expect("Failed to save second account");

    // Either both accounts should be updated or neither (atomic transaction)
    transaction
        .commit()
        .await
        .expect("Failed to commit transaction");

    // Verify both accounts were updated correctly
    let loaded_account1 = load_account(account_id1).await;
    let loaded_account2 = load_account(account_id2).await;

    assert_eq!(loaded_account1.state().balance, account1.state().balance);
    assert_eq!(loaded_account2.state().balance, account2.state().balance);
}

#[tokio::test]
pub async fn repository_error_handling_and_recovery() {
    // Arrange
    let repository = get_repository().await;

    // Create an account
    let mut account = AccountBuilder::new().save().await;
    let account_id = account.state().account_id;
    let initial_balance = account.state().balance;

    // Start a transaction
    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Apply an event
    account
        .record_that(AccountEvent::Add {
            event_id: Uuid::new_v4(),
            amount: 50,
        })
        .expect("Failed to apply Add event");

    // Save changes
    account
        .save(&mut transaction)
        .await
        .expect("Failed to save account");

    // Simulate an error by rolling back instead of committing
    transaction
        .rollback()
        .await
        .expect("Failed to rollback transaction");

    // Recovery: create a new transaction and retry the operation
    let mut recovery_transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin recovery transaction");

    // Reload the account (should be in original state)
    let mut reloaded_account = load_account(account_id).await;
    assert_eq!(
        reloaded_account.state().balance,
        initial_balance,
        "Account should be in original state after rollback"
    );

    // Retry the operation
    reloaded_account
        .record_that(AccountEvent::Add {
            event_id: Uuid::new_v4(),
            amount: 75, // Different amount this time
        })
        .expect("Failed to apply event during recovery");

    // Save and commit
    reloaded_account
        .save(&mut recovery_transaction)
        .await
        .expect("Failed to save account during recovery");

    recovery_transaction
        .commit()
        .await
        .expect("Failed to commit recovery transaction");

    // Verify the recovery operation was successful
    let final_account = load_account(account_id).await;
    assert_eq!(
        final_account.state().balance,
        initial_balance + 75,
        "Account balance should reflect recovery operation"
    );
}

#[tokio::test]
pub async fn repository_load_works_without_transaction() {
    use common::test_aggregate::Account;
    use eventastic::repository::Repository;

    // Arrange
    let repository = get_repository().await;
    let account = AccountBuilder::new()
        .with_add_event(100)
        .with_remove_event(20)
        .save()
        .await;
    let account_id = account.state().account_id;
    let expected_balance = account.state().balance;

    // Act - Load using the new Repository::load method (no transaction needed)
    let loaded_account: Context<Account> = repository
        .load(&account_id)
        .await
        .expect("Failed to load account using Repository::load");

    // Assert
    assert_eq!(loaded_account.state().account_id, account_id);
    assert_eq!(loaded_account.state().balance, expected_balance);

    // Verify it loads the same data as the transaction-based approach
    let transaction_loaded_account = load_account(account_id).await;
    assert_eq!(loaded_account.state(), transaction_loaded_account.state());
}

#[tokio::test]
async fn streaming_returns_events_in_version_order() {
    let repository = get_repository().await;
    let account_id = Uuid::new_v4();

    // Create an account with many events
    let mut account = create_account_with_many_events(account_id, 150_000).await;

    // Save the account
    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");
    transaction
        .store(&mut account)
        .await
        .expect("Failed to save account");
    transaction
        .commit()
        .await
        .expect("Failed to commit transaction");

    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    let mut event_count = 0;

    let mut events_stream =
        RepositoryReader::<Account>::stream_from(&mut transaction, &account_id, 0);

    // Process events one by one to verify streaming behavior
    while let Some(event_result) = events_stream.next().await {
        let event = event_result.expect("Failed to get event from stream");
        event_count += 1;

        // Verify we're getting events in order (events are 0-indexed)
        assert_eq!(
            event.version as usize,
            event_count - 1,
            "Events should be in order"
        );
    }
}
