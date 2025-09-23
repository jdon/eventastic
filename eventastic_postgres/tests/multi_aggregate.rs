mod common;

use common::helpers::get_repository;
use common::test_aggregate::{Account, AccountEvent};
use common::test_order_aggregate::{Order, OrderEvent, OrderStatus};
use eventastic::aggregate::Root;
use eventastic_outbox_postgres::TableOutbox;
use eventastic_postgres::PostgresRepository;
use eventastic_postgres::{NoEncryption, TableConfig};
use sqlx::pool::PoolOptions;
use sqlx::postgres::PgConnectOptions;
use std::str::FromStr;
use uuid::Uuid;

// Helper function to get an order repository using the same pool
async fn get_order_repository() -> PostgresRepository<Order, TableOutbox<NoEncryption>, NoEncryption>
{
    let host = std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string());
    let connection_string = format!("postgres://postgres:password@{host}/postgres");
    let connection_options =
        PgConnectOptions::from_str(&connection_string).expect("Failed to parse connection options");

    let pool_options = PoolOptions::default();

    PostgresRepository::new(
        connection_options,
        pool_options,
        TableConfig::new("events", "snapshots"),
        TableOutbox::new(NoEncryption),
        NoEncryption,
    )
    .await
    .expect("Failed to connect to postgres")
}

#[tokio::test]
pub async fn multi_aggregate_transaction_commit_test() {
    // Arrange
    let account_repo = get_repository().await;
    let order_repo = get_order_repository().await;

    let account_id = Uuid::new_v4();
    let order_id = Uuid::new_v4();
    let customer_id = Uuid::new_v4();

    // Start with account repository transaction
    let mut account_tx = account_repo
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Create and store account
    let account_event = AccountEvent::Open {
        event_id: Uuid::new_v4(),
        account_id,
        email: "test@example.com".to_string(),
        starting_balance: 1000,
    };
    let mut account = Account::record_new(account_event).expect("Failed to create account");
    account_tx
        .store(&mut account)
        .await
        .expect("Failed to store account");

    // Get the raw transaction and pass it to order repository
    let raw_tx = account_tx.into_inner();
    let mut order_tx = order_repo.transaction_from(raw_tx);

    // Create and store order
    let order_event = OrderEvent::Created {
        event_id: Uuid::new_v4(),
        order_id,
        customer_id,
        total_amount: 500,
    };
    let mut order = Order::record_new(order_event).expect("Failed to create order");
    order_tx
        .store(&mut order)
        .await
        .expect("Failed to store order");

    // Commit the transaction
    order_tx
        .commit()
        .await
        .expect("Failed to commit transaction");

    // Assert - verify both aggregates were saved
    let mut account_load_tx = account_repo
        .begin_transaction()
        .await
        .expect("Failed to begin load transaction");
    let loaded_account = account_load_tx
        .get(&account_id)
        .await
        .expect("Failed to load account");
    assert_eq!(loaded_account.state().account_id, account_id);
    assert_eq!(loaded_account.state().balance, 1000);
    account_load_tx
        .commit()
        .await
        .expect("Failed to commit load transaction");

    let mut order_load_tx = order_repo
        .begin_transaction()
        .await
        .expect("Failed to begin order load transaction");
    let loaded_order = order_load_tx
        .get(&order_id)
        .await
        .expect("Failed to load order");
    assert_eq!(loaded_order.state().order_id, order_id);
    assert_eq!(loaded_order.state().total_amount, 500);
    assert_eq!(loaded_order.state().status, OrderStatus::Pending);
    order_load_tx
        .commit()
        .await
        .expect("Failed to commit order load transaction");
}

#[tokio::test]
pub async fn multi_aggregate_transaction_rollback_test() {
    // Arrange
    let account_repo = get_repository().await;
    let order_repo = get_order_repository().await;

    let account_id = Uuid::new_v4();
    let order_id = Uuid::new_v4();
    let customer_id = Uuid::new_v4();

    // Start with account repository transaction
    let mut account_tx = account_repo
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Create and store account
    let account_event = AccountEvent::Open {
        event_id: Uuid::new_v4(),
        account_id,
        email: "test@example.com".to_string(),
        starting_balance: 1000,
    };
    let mut account = Account::record_new(account_event).expect("Failed to create account");
    account_tx
        .store(&mut account)
        .await
        .expect("Failed to store account");

    // Get the raw transaction and pass it to order repository
    let raw_tx = account_tx.into_inner();
    let mut order_tx = order_repo.transaction_from(raw_tx);

    // Create and store order
    let order_event = OrderEvent::Created {
        event_id: Uuid::new_v4(),
        order_id,
        customer_id,
        total_amount: 500,
    };
    let mut order = Order::record_new(order_event).expect("Failed to create order");
    order_tx
        .store(&mut order)
        .await
        .expect("Failed to store order");

    // Rollback the transaction instead of committing
    order_tx
        .rollback()
        .await
        .expect("Failed to rollback transaction");

    // Assert - verify neither aggregate was saved
    let mut account_load_tx = account_repo
        .begin_transaction()
        .await
        .expect("Failed to begin load transaction");
    let account_result = account_load_tx.get(&account_id).await;
    assert!(
        account_result.is_err(),
        "Account should not exist after rollback"
    );
    account_load_tx
        .rollback()
        .await
        .expect("Failed to rollback load transaction");

    let mut order_load_tx = order_repo
        .begin_transaction()
        .await
        .expect("Failed to begin order load transaction");
    let order_result = order_load_tx.get(&order_id).await;
    assert!(
        order_result.is_err(),
        "Order should not exist after rollback"
    );
    order_load_tx
        .rollback()
        .await
        .expect("Failed to rollback order load transaction");
}

#[tokio::test]
pub async fn multi_aggregate_transaction_with_mixed_side_effects() {
    // Arrange
    let account_repo = get_repository().await;
    let order_repo = get_order_repository().await;

    let account_id = Uuid::new_v4();
    let order_id = Uuid::new_v4();
    let customer_id = Uuid::new_v4();

    // Start with account repository transaction
    let mut account_tx = account_repo
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Create account and add money (generates side effects)
    let account_open_event = AccountEvent::Open {
        event_id: Uuid::new_v4(),
        account_id,
        email: "test@example.com".to_string(),
        starting_balance: 1000,
    };
    let mut account = Account::record_new(account_open_event).expect("Failed to create account");

    let add_event = AccountEvent::Add {
        event_id: Uuid::new_v4(),
        amount: 500,
    };
    account
        .record_that(add_event)
        .expect("Failed to add money to account");

    account_tx
        .store(&mut account)
        .await
        .expect("Failed to store account");

    // Get the raw transaction and pass it to order repository
    let raw_tx = account_tx.into_inner();
    let mut order_tx = order_repo.transaction_from(raw_tx);

    // Create and confirm order (generates different side effects)
    let order_event = OrderEvent::Created {
        event_id: Uuid::new_v4(),
        order_id,
        customer_id,
        total_amount: 500,
    };
    let mut order = Order::record_new(order_event).expect("Failed to create order");

    let confirm_event = OrderEvent::Confirmed {
        event_id: Uuid::new_v4(),
    };
    order
        .record_that(confirm_event)
        .expect("Failed to confirm order");

    order_tx
        .store(&mut order)
        .await
        .expect("Failed to store order");

    // Commit the transaction
    order_tx
        .commit()
        .await
        .expect("Failed to commit transaction");

    // Assert - verify both aggregates were saved with correct states
    let mut account_load_tx = account_repo
        .begin_transaction()
        .await
        .expect("Failed to begin load transaction");
    let loaded_account = account_load_tx
        .get(&account_id)
        .await
        .expect("Failed to load account");
    assert_eq!(loaded_account.state().balance, 1500); // 1000 + 500
    account_load_tx
        .commit()
        .await
        .expect("Failed to commit load transaction");

    let mut order_load_tx = order_repo
        .begin_transaction()
        .await
        .expect("Failed to begin order load transaction");
    let loaded_order = order_load_tx
        .get(&order_id)
        .await
        .expect("Failed to load order");
    assert_eq!(loaded_order.state().status, OrderStatus::Confirmed);
    order_load_tx
        .commit()
        .await
        .expect("Failed to commit order load transaction");
}
