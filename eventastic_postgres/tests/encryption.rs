use common::{
    encryption::TestEncryptionProvider,
    helpers::{AccountBuilder, get_encrypted_repository, get_repository, get_side_effect},
    test_aggregate::{Account, AccountEvent, SideEffects},
};
use eventastic::aggregate::{Context, Root};
use eventastic::repository::RepositoryReader;
use eventastic_outbox_postgres::TableOutbox;
use eventastic_postgres::{DbError, NoEncryption, PostgresRepository};
use futures::StreamExt;
use uuid::Uuid;

mod common;

#[tokio::test]
async fn when_encryption_is_enabled_aggregate_can_be_saved_and_loaded() {
    // Arrange
    let repository = get_encrypted_repository().await;
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
        .expect("Failed to commit transaction");
    let created_account = account.state();
    transaction
        .commit()
        .await
        .expect("Failed to commit transaction");

    // Assert
    let loaded_account = load_encrypted_account(account_id)
        .await
        .expect("Failed to load account");
    let loaded_account = loaded_account.state();

    assert_eq!(created_account, loaded_account);
}

#[tokio::test]
async fn when_encryption_is_enabled_events_can_be_saved_and_loaded_by_id() {
    // Arrange
    let repository = get_encrypted_repository().await;

    let account_id = Uuid::new_v4();
    let event_id = Uuid::new_v4();
    let email = "test@example.com".to_string();
    let starting_balance = 100;
    let open_event = AccountEvent::Open {
        account_id,
        event_id,
        email: email.clone(),
        starting_balance,
    };

    let mut account = Account::record_new(open_event.clone()).unwrap();

    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Act
    account
        .save(&mut transaction)
        .await
        .expect("Failed to commit transaction");
    transaction
        .commit()
        .await
        .expect("Failed to commit transaction");

    // Assert
    let mut repository = get_encrypted_repository().await;
    let result = <PostgresRepository<
        TableOutbox<TestEncryptionProvider>,
        TestEncryptionProvider,
    > as RepositoryReader<Account>>::get_event(
        &mut repository, &account_id, &event_id
    ).await;
    assert!(matches!(result, Ok(Some(e)) if e.event == open_event));
}

#[tokio::test]
async fn when_encryption_is_enabled_events_cannot_be_loaded_by_id_without_encryption() {
    // Arrange
    let repository = get_encrypted_repository().await;

    let account_id = Uuid::new_v4();
    let event_id = Uuid::new_v4();
    let email = "test@example.com".to_string();
    let starting_balance = 100;
    let open_event = AccountEvent::Open {
        account_id,
        event_id,
        email: email.clone(),
        starting_balance,
    };

    let mut account = Account::record_new(open_event).unwrap();

    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Act
    account
        .save(&mut transaction)
        .await
        .expect("Failed to commit transaction");
    transaction
        .commit()
        .await
        .expect("Failed to commit transaction");

    // Assert
    let mut repository = get_repository().await;
    let result =
        <PostgresRepository<TableOutbox<NoEncryption>, NoEncryption> as RepositoryReader<
            Account,
        >>::get_event(&mut repository, &account_id, &event_id)
        .await;
    assert!(matches!(result, Err(DbError::PicklingError(_))));
}

#[tokio::test]
async fn when_encryption_is_enabled_events_can_be_saved_and_loaded() {
    // Arrange
    let repository = get_encrypted_repository().await;
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
        .expect("Failed to commit transaction");
    transaction
        .commit()
        .await
        .expect("Failed to commit transaction");

    // Assert
    let mut repository = get_encrypted_repository().await;
    let mut events = <PostgresRepository<
        TableOutbox<TestEncryptionProvider>,
        TestEncryptionProvider,
    > as RepositoryReader<Account>>::stream_from(
        &mut repository, &account_id, 0
    );
    while let Some(event) = events.next().await {
        assert!(matches!(event, Ok(_)));
    }
}

#[tokio::test]
async fn when_encryption_is_enabled_events_cannot_be_loaded_without_encryption() {
    // Arrange
    let repository = get_encrypted_repository().await;
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
        .expect("Failed to commit transaction");
    transaction
        .commit()
        .await
        .expect("Failed to commit transaction");

    // Assert
    let mut repository = get_repository().await;
    let mut events =
        <PostgresRepository<TableOutbox<NoEncryption>, NoEncryption> as RepositoryReader<
            Account,
        >>::stream_from(&mut repository, &account_id, 0);
    while let Some(event) = events.next().await {
        assert!(matches!(
            event,
            Err(eventastic_postgres::DbError::PicklingError(_))
        ));
    }
}

#[tokio::test]
async fn when_encryption_is_enabled_aggregate_cannot_be_loaded_without_encryption() {
    // Arrange
    let repository = get_encrypted_repository().await;
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
        .expect("Failed to commit transaction");
    transaction
        .commit()
        .await
        .expect("Failed to commit transaction");

    // Assert
    let repository = get_repository().await;
    let mut transaction = repository.begin_transaction().await.unwrap();
    assert!(matches!(
        transaction.get::<Account>(&account_id).await,
        Err(eventastic::repository::RepositoryError::Repository(
            eventastic_postgres::DbError::PicklingError(_)
        )),
    ));
}

#[tokio::test]
async fn when_encryption_is_enabled_side_effect_can_be_saved_and_loaded() {
    // Arrange
    let repository = get_encrypted_repository().await;

    let account_id = Uuid::new_v4();
    let event_id = Uuid::new_v4();
    let email = "test@example.com".to_string();
    let starting_balance = 100;
    let open_event = AccountEvent::Open {
        account_id,
        event_id,
        email: email.clone(),
        starting_balance,
    };

    let mut account = Account::record_new(open_event).unwrap();

    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Act
    account
        .save(&mut transaction)
        .await
        .expect("Failed to commit transaction");
    transaction
        .commit()
        .await
        .expect("Failed to commit transaction");

    // Assert
    let (side_effect, _retries, _requeue) = get_side_effect(event_id, TestEncryptionProvider)
        .await
        .expect("Side effect should be stored in outbox table");
    match side_effect {
        SideEffects::SendEmail {
            id: side_effect_id,
            address,
            content,
        } => {
            assert_eq!(side_effect_id, event_id);
            assert_eq!(address, email);
            assert!(content.contains(&account_id.to_string()));
            assert!(content.contains(&starting_balance.to_string()));
        }
        _ => panic!("Expected SendEmail side effect"),
    }
}

async fn load_encrypted_account(account_id: Uuid) -> anyhow::Result<Context<Account>> {
    let repository = get_encrypted_repository().await;

    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    let context: Context<Account> = transaction.get(&account_id).await?;

    Ok(context)
}
