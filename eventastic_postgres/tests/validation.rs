mod common;

use common::helpers::{get_repository, AccountBuilder};
use common::test_aggregate::AccountEvent;
use eventastic::aggregate::SaveError;
use uuid::Uuid;

#[tokio::test]
pub async fn idempotency_error_if_event_with_different_content_is_saved() {
    // Arrange
    let mut account = AccountBuilder::new().save().await;

    let repository = get_repository().await;

    let event_id = Uuid::now_v7();

    let add_event = AccountEvent::Add {
        event_id,
        amount: 10,
    };

    account
        .record_that(add_event)
        .expect("Failed to apply event");

    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    account
        .save(&mut transaction)
        .await
        .expect("Failed to save account");

    transaction
        .commit()
        .await
        .expect("Failed to commit transaction");

    // Act

    let add_event = AccountEvent::Add {
        event_id,
        amount: 20,
    };

    account
        .record_that(add_event)
        .expect("Failed to apply event");

    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    let err = account
        .save(&mut transaction)
        .await
        .expect_err("Failed get error");

    // Assert

    assert!(matches!(err, SaveError::IdempotencyError(_, _)));
}

#[tokio::test]
pub async fn optimistic_concurrency_error_if_aggregate_was_updated_by_another() {
    // Arrange

    let mut account = AccountBuilder::new().save().await;
    let mut outdated_account = account.clone();
    let account_id = account.state().account_id;

    let add_event = AccountEvent::Add {
        event_id: Uuid::now_v7(),
        amount: 10,
    };

    account
        .record_that(add_event)
        .expect("Failed to apply event");

    let repository = get_repository().await;

    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    account
        .save(&mut transaction)
        .await
        .expect("Failed to save account");

    transaction
        .commit()
        .await
        .expect("Failed to commit transaction");

    // Act
    // Attempt to update the account with an outdated version

    outdated_account
        .record_that(AccountEvent::Add {
            event_id: Uuid::now_v7(),
            amount: 20,
        })
        .expect("Failed to apply event");

    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    let err = outdated_account
        .save(&mut transaction)
        .await
        .expect_err("Failed to get error while saving account");

    assert!(
        matches!(err, SaveError::OptimisticConcurrency(id, version) if id == account_id && version == 1)
    );
}
