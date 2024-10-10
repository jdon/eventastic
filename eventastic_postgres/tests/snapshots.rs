mod common;

use common::helpers::{
    delete_snapshot, get_account_snapshot, get_repository, load_account, replace_account_snapshot,
    AccountBuilder,
};
use common::test_aggregate::AccountEvent;
use uuid::Uuid;

#[tokio::test]
pub async fn snapshots_are_saved_automatically() {
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
    let snapshot = get_account_snapshot(account_id)
        .await
        .expect("Failed to get snapshot");

    let state = snapshot.aggregate;

    assert_eq!(created_account.clone(), state);
}

#[tokio::test]
pub async fn aggregate_are_rebuilt_if_snapshots_are_missing() {
    // Arrange
    let account = AccountBuilder::new()
        .with_add_event(100)
        .with_remove_event(10)
        .with_add_event(10)
        .save()
        .await;

    let account_id = account.state().account_id;

    delete_snapshot(account_id).await;

    assert!(get_account_snapshot(account_id).await.is_none());

    // Act

    let rebuilt_account = load_account(account_id).await;

    // Assert

    assert_eq!(rebuilt_account.state(), account.state());
    // Snapshot is not saved again on load, it's only stored on save
    assert!(get_account_snapshot(account_id).await.is_none());
}

#[tokio::test]
pub async fn snapshots_are_successfully_saved_when_new_event_is_applied() {
    // Arrange
    let repository = get_repository().await;
    let mut account = AccountBuilder::new()
        .with_add_event(100)
        .with_remove_event(10)
        .with_add_event(10)
        .save()
        .await;

    let account_id = account.state().account_id;

    delete_snapshot(account_id).await;

    assert!(get_account_snapshot(account_id).await.is_none());

    // Act

    account
        .record_that(AccountEvent::Add {
            event_id: Uuid::now_v7(),
            amount: 10,
        })
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

    // Assert

    let saved_snapshot = get_account_snapshot(account_id)
        .await
        .expect("Failed to get snapshot");

    let saved_state = saved_snapshot.aggregate;

    assert_eq!(&saved_state, account.state());
}

#[tokio::test]
pub async fn snapshots_are_rebuilt_if_snapshot_version_is_different() {
    // Arrange
    let account = AccountBuilder::new()
        .with_add_event(100)
        .with_remove_event(10)
        .with_add_event(10)
        .save()
        .await;

    let account_id = account.state().account_id;

    let snapshot = get_account_snapshot(account_id)
        .await
        .expect("Failed to get snapshot");

    let mut snapshot = snapshot.clone();
    snapshot.snapshot_version = 0;

    snapshot.aggregate.balance = 0;

    // Insert our modified snapshot with a different version and balance
    replace_account_snapshot(account_id, snapshot).await;

    // Act

    // Account should be rebuilt and not use the snapshot with the wrong version
    let rebuilt_account = load_account(account_id).await;

    // Assert

    assert_eq!(rebuilt_account.state(), account.state());
}
