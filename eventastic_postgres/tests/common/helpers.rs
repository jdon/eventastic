use super::test_aggregate::{Account, AccountEvent};
use chrono::{DateTime, Utc};
use eventastic::aggregate::{Context, Root};
use eventastic_outbox_postgres::TableOutbox;
use eventastic_postgres::{Pickle, PostgresRepository, TableRegistryBuilder};
use sqlx::Row;
use sqlx::{pool::PoolOptions, postgres::PgConnectOptions};
use std::str::FromStr;
use uuid::Uuid;

pub async fn get_repository() -> PostgresRepository<TableOutbox> {
    let host = std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string());
    let connection_string = format!("postgres://postgres:password@{host}/postgres");
    let connection_options = PgConnectOptions::from_str(connection_string.as_str())
        .expect("Failed to parse connection options");

    let pool_options = PoolOptions::default();

    let tables = TableRegistryBuilder::new()
        .register_with_tables::<Account>("events", "snapshots")
        .build();

    let repo = PostgresRepository::new(connection_options, pool_options, TableOutbox, tables)
        .await
        .expect("Failed to connect to postgres");
    repo.run_migrations()
        .await
        .expect("Failed to run migrations");
    repo
}

#[derive(serde::Deserialize, Debug, Clone, serde::Serialize)]
pub struct SavedSnapshot {
    pub version: i64,
    pub aggregate: Account,
    pub snapshot_version: i64,
}

pub async fn get_account_snapshot(account_id: Uuid) -> Option<SavedSnapshot> {
    let repository = get_repository().await;

    let transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    let row = sqlx::query(
        "SELECT aggregate, version, snapshot_version FROM snapshots where aggregate_id = $1",
    )
    .bind(account_id)
    .fetch_optional(&mut *transaction.into_inner())
    .await
    .expect("Failed to fetch snapshot");

    row.map(|row| {
        let aggregate_bytes: Result<Vec<u8>, _> = row.try_get("aggregate");
        let version: Result<i64, _> = row.try_get("version");
        let snapshot_version: Result<i64, _> = row.try_get("snapshot_version");

        SavedSnapshot {
            aggregate: Account::unpickle(&aggregate_bytes.unwrap()).unwrap(),
            version: version.unwrap(),
            snapshot_version: snapshot_version.unwrap(),
        }
    })
}

pub async fn get_account_snapshot_with_version(
    account_id: Uuid,
    snapshot_version: i64,
) -> Option<SavedSnapshot> {
    let repository = get_repository().await;

    let transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    let row = sqlx::query(
        "SELECT aggregate, version, snapshot_version FROM snapshots WHERE aggregate_id = $1 AND snapshot_version = $2",
    )
    .bind(account_id)
    .bind(snapshot_version)
    .fetch_optional(&mut *transaction.into_inner())
    .await
    .expect("Failed to fetch snapshot");

    row.map(|row| {
        let aggregate_bytes: Result<Vec<u8>, _> = row.try_get("aggregate");
        let version: Result<i64, _> = row.try_get("version");
        let snapshot_version: Result<i64, _> = row.try_get("snapshot_version");

        SavedSnapshot {
            aggregate: Account::unpickle(&aggregate_bytes.unwrap()).unwrap(),
            version: version.unwrap(),
            snapshot_version: snapshot_version.unwrap(),
        }
    })
}

pub async fn get_all_account_snapshots(account_id: Uuid) -> Vec<SavedSnapshot> {
    let repository = get_repository().await;

    let transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    let rows = sqlx::query(
        "SELECT aggregate, version, snapshot_version FROM snapshots WHERE aggregate_id = $1 ORDER BY snapshot_version ASC",
    )
    .bind(account_id)
    .fetch_all(&mut *transaction.into_inner())
    .await
    .expect("Failed to fetch snapshots");

    rows.into_iter()
        .map(|row| {
            let aggregate_bytes: Result<Vec<u8>, _> = row.try_get("aggregate");
            let version: Result<i64, _> = row.try_get("version");
            let snapshot_version: Result<i64, _> = row.try_get("snapshot_version");

            SavedSnapshot {
                aggregate: Account::unpickle(&aggregate_bytes.unwrap()).unwrap(),
                version: version.unwrap(),
                snapshot_version: snapshot_version.unwrap(),
            }
        })
        .collect()
}

pub async fn count_account_snapshots(account_id: Uuid) -> usize {
    let repository = get_repository().await;

    let transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    let row = sqlx::query("SELECT COUNT(*) as count FROM snapshots WHERE aggregate_id = $1")
        .bind(account_id)
        .fetch_one(&mut *transaction.into_inner())
        .await
        .expect("Failed to count snapshots");

    let count: i64 = row.try_get("count").expect("Failed to get count");
    count as usize
}

pub async fn replace_account_snapshot(account_id: Uuid, snapshot: SavedSnapshot) {
    let repository = get_repository().await;

    let transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    let mut pg_transaction = transaction.into_inner();

    let row = sqlx::query("UPDATE snapshots set aggregate = $1, snapshot_version = $2, version = $3 where aggregate_id = $4")
        .bind(snapshot.aggregate.pickle().expect("Failed to serialize snapshot"))
        .bind(snapshot.snapshot_version)
        .bind(snapshot.version)
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

pub async fn insert_snapshot_with_version(account_id: Uuid, snapshot: SavedSnapshot, version: i64) {
    let repository = get_repository().await;

    let mut transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction")
        .into_inner();

    sqlx::query("INSERT INTO snapshots (aggregate_id, aggregate, version, snapshot_version, created_at) VALUES ($1, $2, $3, $4, NOW())")
        .bind(account_id)
        .bind(snapshot.aggregate.pickle().expect("Failed to serialize snapshot"))
        .bind(snapshot.version)
        .bind(version)
        .execute(&mut *transaction)
        .await
        .expect("Failed to insert snapshot");

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

    let context: Context<Account> = transaction
        .get(&account_id)
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
        let account_id = Uuid::new_v4();
        Self {
            account_id,
            events: Vec::new(),
            open_event: AccountEvent::Open {
                event_id: Uuid::new_v4(),
                account_id,
                starting_balance: 0,
                email: "user@example.com".into(),
            },
        }
    }

    pub fn with_email(mut self, new_email: String) -> Self {
        if let AccountEvent::Open { ref mut email, .. } = self.open_event {
            *email = new_email;
        }
        self
    }

    pub fn with_balance(mut self, balance: i64) -> Self {
        if let AccountEvent::Open {
            ref mut starting_balance,
            ..
        } = self.open_event
        {
            *starting_balance = balance;
        }
        self
    }

    pub fn with_open_event(mut self, event: AccountEvent) -> Self {
        self.open_event = event;
        self
    }

    pub fn with_add_event(mut self, amount: i64) -> Self {
        let add_event = AccountEvent::Add {
            event_id: Uuid::new_v4(),
            amount,
        };
        self.events.push(add_event);
        self
    }

    pub fn with_remove_event(mut self, amount: i64) -> Self {
        let remove_event = AccountEvent::Remove {
            event_id: Uuid::new_v4(),
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

        transaction
            .store(&mut account)
            .await
            .expect("Failed to save account");

        transaction
            .commit()
            .await
            .expect("Failed to commit transaction");

        account
    }
}

pub async fn get_side_effect(
    id: uuid::Uuid,
) -> Option<(super::test_aggregate::SideEffects, i32, bool)> {
    let repository = get_repository().await;
    let transaction = repository
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    let row = sqlx::query("SELECT id, message, retries, requeue FROM outbox WHERE id = $1")
        .bind(id)
        .fetch_optional(&mut *transaction.into_inner())
        .await
        .expect("Failed to query outbox table");

    if let Some(row) = row {
        let message_bytes: Vec<u8> = row
            .try_get("message")
            .expect("Failed to get message from row");
        let retries: i32 = row
            .try_get("retries")
            .expect("Failed to get retries from row");
        let requeue: bool = row
            .try_get("requeue")
            .expect("Failed to get requeue from row");

        let side_effect: super::test_aggregate::SideEffects =
            super::test_aggregate::SideEffects::unpickle(&message_bytes)
                .expect("Failed to deserialize side effect");

        Some((side_effect, retries, requeue))
    } else {
        None
    }
}
