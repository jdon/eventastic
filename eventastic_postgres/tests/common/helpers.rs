use super::test_aggregate::{Account, AccountEvent};
use chrono::{DateTime, Utc};
use eventastic::aggregate::{Context, Root};
use eventastic_postgres::PostgresRepository;
use sqlx::Row;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::str::FromStr;
use uuid::Uuid;

pub async fn get_repository() -> PostgresRepository {
    let host = std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string());
    let connection_string = format!("postgres://postgres:password@{host}/postgres");
    let connection_options = PgConnectOptions::from_str(connection_string.as_str())
        .expect("Failed to parse connection options");

    let pool_options = PoolOptions::default();

    let repo = PostgresRepository::new(connection_options, pool_options)
        .await
        .expect("Failed to connect to postgres");
    repo.run_migrations()
        .await
        .expect("Failed to run migrations");
    repo
}

#[derive(serde::Deserialize, Debug, Clone, serde::Serialize)]
pub struct SavedSnapshot {
    pub version: i32,
    pub aggregate: Account,
    pub snapshot_version: i32,
}

pub async fn get_account_snapshot(account_id: Uuid) -> Option<SavedSnapshot> {
    let repository = get_repository().await;

    let transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    let row = sqlx::query("SELECT snapshot FROM snapshots where aggregate_id = $1")
        .bind(account_id)
        .fetch_optional(&mut *transaction.into_inner())
        .await
        .expect("Failed to fetch snapshot");

    row.map(|row| {
        let snapshot: Result<serde_json::Value, _> = row.try_get("snapshot");
        snapshot
    })
    .transpose()
    .expect("Failed to deserialize snapshot")
    .map(|snapshot| serde_json::from_value(snapshot).expect("Failed to deserialize snapshot"))
}

pub async fn replace_account_snapshot(account_id: Uuid, snapshot: SavedSnapshot) {
    let repository = get_repository().await;

    let transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    let mut pg_transaction = transaction.into_inner();

    let row = sqlx::query("UPDATE snapshots set snapshot = $1 where aggregate_id = $2")
        .bind(serde_json::to_value(&snapshot).expect("Failed to serialize snapshot"))
        .bind(account_id)
        .execute(&mut *pg_transaction)
        .await
        .expect("Failed to update snapshot");

    assert!(row.rows_affected() == 1, "Failed to update snapshot");
    pg_transaction
        .commit()
        .await
        .expect("Failed to commit transaction");
}

pub async fn delete_snapshot(account_id: Uuid) {
    let repository = get_repository().await;

    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction")
        .into_inner();

    sqlx::query("DELETE FROM snapshots WHERE aggregate_id = $1")
        .bind(account_id)
        .execute(&mut *transaction)
        .await
        .expect("Failed to delete snapshot");

    transaction
        .commit()
        .await
        .expect("Failed to commit transaction");
}

pub async fn load_account(account_id: Uuid) -> Context<Account> {
    let repository = get_repository().await;

    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    let context: Context<Account> = Context::load(&mut transaction, &account_id)
        .await
        .expect("Failed to load account");

    context
}

pub async fn get_latest_event_timestamp(account_id: Uuid) -> DateTime<Utc> {
    let repository = get_repository().await;

    let transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    let row =
        sqlx::query("SELECT MAX(created_at) as created_at FROM events where aggregate_id = $1")
            .bind(account_id)
            .fetch_one(&mut *transaction.into_inner())
            .await
            .expect("Failed to fetch timestamp");

    row.get("created_at")
}

pub struct AccountBuilder {
    account_id: Uuid,
    open_event: AccountEvent,
    events: Vec<AccountEvent>,
}

impl AccountBuilder {
    pub fn new() -> Self {
        let account_id = Uuid::now_v7();
        Self {
            account_id,
            events: Vec::new(),
            open_event: AccountEvent::Open {
                event_id: Uuid::now_v7(),
                account_id,
                starting_balance: 21,
                email: "user@example.com".into(),
            },
        }
    }

    pub fn with_open_event(mut self, event: AccountEvent) -> Self {
        self.open_event = event;
        self
    }

    pub fn with_add_event(mut self, amount: i64) -> Self {
        let add_event = AccountEvent::Add {
            event_id: Uuid::now_v7(),
            amount,
        };
        self.events.push(add_event);
        self
    }

    pub fn with_remove_event(mut self, amount: i64) -> Self {
        let remove_event = AccountEvent::Remove {
            event_id: Uuid::now_v7(),
            amount,
        };
        self.events.push(remove_event);
        self
    }

    pub fn with_event(mut self, event: AccountEvent) -> Self {
        self.events.push(event);
        self
    }

    pub fn build(self) -> Context<Account> {
        let mut account =
            Account::record_new(self.open_event).expect("Failed to record new account");

        for event in self.events {
            account.record_that(event).expect("Failed to apply event");
        }

        account
    }

    pub async fn save(self) -> Context<Account> {
        let repository = get_repository().await;
        let mut account = self.build();

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

        account
    }
}
