mod common;

use common::helpers::{
    AccountBuilder, count_account_snapshots, delete_snapshot, get_account_snapshot,
    get_account_snapshot_with_version, get_repository, insert_snapshot_with_version, load_account,
    replace_account_snapshot,
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
pub async fn aggregate_is_rebuilt_if_snapshots_are_missing() {
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
            event_id: Uuid::new_v4(),
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

#[tokio::test]
pub async fn multiple_snapshot_versions_can_coexist() {
    // Arrange
    let account = AccountBuilder::new()
        .with_add_event(100)
        .with_remove_event(10)
        .save()
        .await;

    let account_id = account.state().account_id;

    // Get the current snapshot (version 2)
    let current_snapshot = get_account_snapshot(account_id)
        .await
        .expect("Failed to get current snapshot");

    // Create a snapshot with version 1 (different from current version 2)
    let mut old_snapshot = current_snapshot.clone();
    old_snapshot.aggregate.balance = 500; // Different balance to verify they coexist

    // Act - Insert snapshot with version 1
    insert_snapshot_with_version(account_id, old_snapshot.clone(), 1).await;

    // Assert - Both snapshots should exist
    assert_eq!(count_account_snapshots(account_id).await, 2);

    // Verify we can retrieve each snapshot by version
    let version_1_snapshot = get_account_snapshot_with_version(account_id, 1)
        .await
        .expect("Failed to get version 1 snapshot");
    let version_2_snapshot = get_account_snapshot_with_version(account_id, 2)
        .await
        .expect("Failed to get version 2 snapshot");

    assert_eq!(version_1_snapshot.aggregate.balance, 500);
    assert_eq!(version_2_snapshot.aggregate.balance, 90);
    assert_eq!(version_1_snapshot.snapshot_version, 1);
    assert_eq!(version_2_snapshot.snapshot_version, 2);

    // Verify loading the aggregate uses the correct current version (2)
    let loaded_account = load_account(account_id).await;
    assert_eq!(loaded_account.state().balance, 90);
}

#[tokio::test]
pub async fn same_snapshot_version_updates_existing_snapshot() {
    // Arrange
    let repository = get_repository().await;
    let mut account = AccountBuilder::new().with_add_event(100).build();

    let account_id = account.state().account_id;

    // Save the initial account (creates snapshot with version 2)
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

    // Verify initial state
    assert_eq!(count_account_snapshots(account_id).await, 1);
    let initial_snapshot = get_account_snapshot(account_id)
        .await
        .expect("Failed to get initial snapshot");
    assert_eq!(initial_snapshot.aggregate.balance, 100);

    // Act - Modify the account and save again (same SNAPSHOT_VERSION)
    account
        .record_that(AccountEvent::Add {
            event_id: Uuid::new_v4(),
            amount: 50,
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

    // Assert - Still only one snapshot, but updated
    assert_eq!(count_account_snapshots(account_id).await, 1);
    let updated_snapshot = get_account_snapshot(account_id)
        .await
        .expect("Failed to get updated snapshot");
    assert_eq!(updated_snapshot.aggregate.balance, 150);
    assert_eq!(updated_snapshot.snapshot_version, 2);
}

#[tokio::test]
pub async fn old_snapshots_are_ignored_when_loading() {
    // Arrange
    let account = AccountBuilder::new()
        .with_add_event(100)
        .with_remove_event(10)
        .save()
        .await;

    let account_id = account.state().account_id;

    // Get the current snapshot (version 2) and verify balance
    let current_snapshot = get_account_snapshot(account_id)
        .await
        .expect("Failed to get current snapshot");
    assert_eq!(current_snapshot.aggregate.balance, 90);

    // Create snapshots with old versions (0 and 1) with different balances
    let mut old_snapshot_v0 = current_snapshot.clone();
    old_snapshot_v0.aggregate.balance = 999; // Wrong balance

    let mut old_snapshot_v1 = current_snapshot.clone();
    old_snapshot_v1.aggregate.balance = 888; // Wrong balance

    // Act - Insert old snapshots
    insert_snapshot_with_version(account_id, old_snapshot_v0, 0).await;
    insert_snapshot_with_version(account_id, old_snapshot_v1, 1).await;

    // Assert - All snapshots exist in database
    assert_eq!(count_account_snapshots(account_id).await, 3);

    // Verify we can retrieve each snapshot by version
    let version_0_snapshot = get_account_snapshot_with_version(account_id, 0)
        .await
        .expect("Failed to get version 0 snapshot");
    let version_1_snapshot = get_account_snapshot_with_version(account_id, 1)
        .await
        .expect("Failed to get version 1 snapshot");
    let version_2_snapshot = get_account_snapshot_with_version(account_id, 2)
        .await
        .expect("Failed to get version 2 snapshot");

    assert_eq!(version_0_snapshot.aggregate.balance, 999);
    assert_eq!(version_1_snapshot.aggregate.balance, 888);
    assert_eq!(version_2_snapshot.aggregate.balance, 90);

    // Most important: Loading the aggregate should use the correct current version (2)
    // and ignore the old snapshots with incorrect balances
    let loaded_account = load_account(account_id).await;
    assert_eq!(loaded_account.state().balance, 90);
}
