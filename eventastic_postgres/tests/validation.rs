mod common;

use common::helpers::{AccountBuilder, get_repository};
use common::test_aggregate::AccountEvent;
use eventastic::aggregate::SaveError;
use uuid::Uuid;

#[tokio::test]
pub async fn idempotency_error_if_event_with_different_content_is_saved() {
    // Arrange
    // Create and save a new account to the repository
    let mut account = AccountBuilder::new().save().await;

    // Get a repository instance to interact with the database
    let repository = get_repository().await;

    // Generate a UUID that will be reused for two different events
    // This will create an idempotency conflict since the event ID should uniquely identify the event content
    let event_id = Uuid::new_v4();

    // Create first event with the generated ID and amount 10
    let add_event = AccountEvent::Add {
        event_id,
        amount: 10, // First event adds 10 to the account
    };

    // Record the event in the aggregate's context, which applies it to the account state
    account
        .record_that(add_event)
        .expect("Failed to apply event");

    // Begin a transaction to save the event to the repository
    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Save the account with the new event
    account
        .save(&mut transaction)
        .await
        .expect("Failed to save account");

    // Commit the transaction to persist the changes
    transaction
        .commit()
        .await
        .expect("Failed to commit transaction");

    // Act
    // Create a second event with the SAME ID but DIFFERENT content (amount 20 instead of 10)
    // This should violate idempotency since events with the same ID should have the same content
    let add_event = AccountEvent::Add {
        event_id,   // Same event ID as before
        amount: 20, // Different amount (20 instead of 10)
    };

    // Record the conflicting event
    account
        .record_that(add_event)
        .expect("Failed to apply event");

    // Begin a new transaction to try to save the conflicting event
    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Try to save the account with the conflicting event
    // This should fail with an IdempotencyError since an event with this ID
    // but different content already exists in the repository
    let err = account
        .save(&mut transaction)
        .await
        .expect_err("Failed get error");

    // Assert
    // Verify the error is an IdempotencyError and contains the expected events
    // First parameter is the saved event (amount 10), second parameter is the conflicting event (amount 20)
    assert!(matches!(err,
        SaveError::IdempotencyError(
            AccountEvent::Add { amount: 10, event_id: id1 },
            AccountEvent::Add { amount: 20, event_id: id2 }
        ) if id1 == event_id && id2 == event_id
    ));
}

#[tokio::test]
pub async fn no_idempotency_error_if_event_with_same_content_is_saved() {
    // Arrange
    // Create and save a new account to the repository
    let mut account = AccountBuilder::new().save().await;

    // Get a repository instance to interact with the database
    let repository = get_repository().await;

    // Generate a UUID that will be reused for two different events
    // This will create an idempotency conflict since the event ID should uniquely identify the event content
    let event_id = Uuid::new_v4();

    // Create first event with the generated ID and amount 10
    let add_event = AccountEvent::Add {
        event_id,
        amount: 10, // First event adds 10 to the account
    };

    // Record the event in the aggregate's context, which applies it to the account state
    account
        .record_that(add_event.clone())
        .expect("Failed to apply event");

    // Begin a transaction to save the event to the repository
    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Save the account with the new event
    account
        .save(&mut transaction)
        .await
        .expect("Failed to save account");

    // Commit the transaction to persist the changes
    transaction
        .commit()
        .await
        .expect("Failed to commit transaction");

    // Act

    // Record the conflicting event
    account
        .record_that(add_event)
        .expect("Failed to apply event");

    // Begin a new transaction to try to save the conflicting event
    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Try to save the account with the conflicting event
    account
        .save(&mut transaction)
        .await
        .expect("Failed to save account");

    transaction
        .commit()
        .await
        .expect("Failed to commit transaction");
}

#[tokio::test]
pub async fn optimistic_concurrency_error_if_aggregate_was_updated_by_another() {
    // Arrange
    // Create a new account and save it to the repository
    let mut account = AccountBuilder::new().save().await;

    // Create a clone of the account to simulate a concurrent access scenario
    // Both instances represent the same aggregate but will diverge as changes are made
    let mut outdated_account = account.clone();
    let account_id = account.state().account_id;

    // Create an event to add 10 to the account
    let add_event = AccountEvent::Add {
        event_id: Uuid::new_v4(),
        amount: 10,
    };

    // Apply the event to the first account instance
    account
        .record_that(add_event)
        .expect("Failed to apply event");

    // Get a repository instance
    let repository = get_repository().await;

    // Begin a transaction to save the updated account
    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Save the account with its new event
    account
        .save(&mut transaction)
        .await
        .expect("Failed to save account");

    // Commit the transaction - at this point, the account in the repository has version 1
    transaction
        .commit()
        .await
        .expect("Failed to commit transaction");

    // Act
    // Attempt to update the account with an outdated version
    // The outdated_account is still at version 0, but the repository has version 1

    // Apply a different event to the outdated account instance
    outdated_account
        .record_that(AccountEvent::Add {
            event_id: Uuid::new_v4(),
            amount: 20,
        })
        .expect("Failed to apply event");

    // Begin a new transaction to try to save the outdated account
    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Try to save the outdated account - this should fail with an OptimisticConcurrency error
    // because the repository version is ahead of what the outdated_account expects
    let err = outdated_account
        .save(&mut transaction)
        .await
        .expect_err("Failed to get error while saving account");

    // Assert
    // Verify we got an OptimisticConcurrency error with the correct aggregate ID and version
    assert!(
        matches!(err, SaveError::OptimisticConcurrency(id, version) if id == account_id && version == 1)
    );
}
