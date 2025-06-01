mod common;

use common::helpers::{get_repository, get_side_effect};
use common::test_aggregate::{Account, AccountEvent, SideEffects};
use eventastic::aggregate::Root;
use uuid::Uuid;

#[tokio::test]
async fn side_effect_is_correctly_stored() {
    // Arrange
    let repository = get_repository().await;

    // Create an Open event with a known event ID so we can query for it later
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

    // Create an aggregate from the event
    let mut account = Account::record_new(open_event).expect("Failed to create account");

    // Act - Save the account which should trigger storing the side effect
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

    // Assert - Verify the side effect was stored in the outbox table
    let (side_effect, retries, requeue) = get_side_effect(event_id)
        .await
        .expect("Side effect should be stored in outbox table");

    // Assert the side effect contains the expected data
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

    // Assert the retry settings
    assert_eq!(retries, 0, "Initial retries should be 0");
    assert!(requeue, "Side effect should be requeued by default");
}

#[tokio::test]
async fn multiple_side_effects_are_stored_correctly() {
    // Arrange
    let repository = get_repository().await;

    // Create an account with known event IDs
    let account_id = Uuid::new_v4();
    let open_event_id = Uuid::new_v4();
    let add_event_id = Uuid::new_v4();

    // Create an Open event
    let open_event = AccountEvent::Open {
        account_id,
        event_id: open_event_id,
        email: "test@example.com".to_string(),
        starting_balance: 200,
    };

    // Create an aggregate from the event
    let mut account = Account::record_new(open_event).expect("Failed to create account");

    // Add another event that will generate a different side effect
    let add_amount = 50;
    let add_event = AccountEvent::Add {
        event_id: add_event_id,
        amount: add_amount,
    };

    account
        .record_that(add_event)
        .expect("Failed to apply Add event");

    // Act - Save the account which should store both side effects
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

    // Assert - Verify both side effects were stored

    // Check for the Open event's side effect
    get_side_effect(open_event_id)
        .await
        .expect("Open event side effect should be stored");

    // Check for the Add event's side effect
    let (side_effect, _, _) = get_side_effect(add_event_id)
        .await
        .expect("Add event side effect should be stored");

    match side_effect {
        SideEffects::PublishMessage { id, message } => {
            assert_eq!(id, add_event_id);
            assert_eq!(message, add_amount.to_string());
        }
        _ => panic!("Expected PublishMessage side effect"),
    }
}
