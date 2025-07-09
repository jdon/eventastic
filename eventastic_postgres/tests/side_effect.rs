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

#[tokio::test]
async fn side_effect_regeneration_works_correctly() {
    use eventastic::aggregate::Context;

    // Arrange
    let repository = get_repository().await;

    // Create an account with multiple events
    let account_id = Uuid::new_v4();
    let open_event_id = Uuid::new_v4();
    let add_event_id = Uuid::new_v4();
    let remove_event_id = Uuid::new_v4();

    // Create events
    let open_event = AccountEvent::Open {
        account_id,
        event_id: open_event_id,
        email: "test@example.com".to_string(),
        starting_balance: 100,
    };

    let add_event = AccountEvent::Add {
        event_id: add_event_id,
        amount: 50,
    };

    let remove_event = AccountEvent::Remove {
        event_id: remove_event_id,
        amount: 25,
    };

    // Create and save the aggregate with all events
    let mut account = Account::record_new(open_event.clone()).expect("Failed to create account");
    account
        .record_that(add_event.clone())
        .expect("Failed to apply Add event");
    account
        .record_that(remove_event.clone())
        .expect("Failed to apply Remove event");

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

    // Act & Assert - Test regenerating side effects for Open event (should generate SendEmail)
    let mut repo_clone = repository.clone();
    let regenerated_open_effects =
        Context::<Account>::regenerate_side_effects(&mut repo_clone, &account_id, &open_event_id)
            .await
            .expect("Failed to regenerate side effects for Open event");

    assert!(
        regenerated_open_effects.is_some(),
        "Open event should generate side effects"
    );
    let open_effects = regenerated_open_effects.unwrap();
    assert_eq!(
        open_effects.len(),
        1,
        "Open event should generate exactly one side effect"
    );

    match &open_effects[0] {
        SideEffects::SendEmail {
            id,
            address,
            content,
        } => {
            assert_eq!(*id, open_event_id, "Side effect ID should match event ID");
            assert_eq!(address, "test@example.com", "Email address should match");
            assert!(
                content.contains(&account_id.to_string()),
                "Content should contain account ID"
            );
            assert!(
                content.contains("100"),
                "Content should contain starting balance"
            );
        }
        _ => panic!("Expected SendEmail side effect for Open event"),
    }

    // Act & Assert - Test regenerating side effects for Add event (should generate PublishMessage)
    let mut repo_clone = repository.clone();
    let regenerated_add_effects =
        Context::<Account>::regenerate_side_effects(&mut repo_clone, &account_id, &add_event_id)
            .await
            .expect("Failed to regenerate side effects for Add event");

    assert!(
        regenerated_add_effects.is_some(),
        "Add event should generate side effects"
    );
    let add_effects = regenerated_add_effects.unwrap();
    assert_eq!(
        add_effects.len(),
        1,
        "Add event should generate exactly one side effect"
    );

    match &add_effects[0] {
        SideEffects::PublishMessage { id, message } => {
            assert_eq!(*id, add_event_id, "Side effect ID should match event ID");
            assert_eq!(message, "50", "Message should contain the amount");
        }
        _ => panic!("Expected PublishMessage side effect for Add event"),
    }

    // Act & Assert - Test regenerating side effects for Remove event (should generate no side effects)
    let mut repo_clone = repository.clone();
    let regenerated_remove_effects =
        Context::<Account>::regenerate_side_effects(&mut repo_clone, &account_id, &remove_event_id)
            .await
            .expect("Failed to regenerate side effects for Remove event");

    assert!(
        regenerated_remove_effects.is_none(),
        "Remove event should not generate side effects"
    );

    // Act & Assert - Test error case: non-existent event ID
    let non_existent_event_id = Uuid::new_v4();
    let mut repo_clone = repository.clone();
    let non_existent_result = Context::<Account>::regenerate_side_effects(
        &mut repo_clone,
        &account_id,
        &non_existent_event_id,
    )
    .await
    .expect("Should not error for non-existent event ID");

    assert!(
        non_existent_result.is_none(),
        "Non-existent event should return None"
    );

    // Act & Assert - Test error case: non-existent aggregate ID
    let non_existent_aggregate_id = Uuid::new_v4();
    let mut repo_clone = repository.clone();
    let non_existent_aggregate_result = Context::<Account>::regenerate_side_effects(
        &mut repo_clone,
        &non_existent_aggregate_id,
        &open_event_id,
    )
    .await
    .expect("Should not error for non-existent aggregate ID");

    assert!(
        non_existent_aggregate_result.is_none(),
        "Non-existent aggregate should return None"
    );
}
