mod common;

use common::helpers::{get_repository, load_account, AccountBuilder};

#[tokio::test]
pub async fn aggregate_are_successfully_saved_and_loaded() {
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
