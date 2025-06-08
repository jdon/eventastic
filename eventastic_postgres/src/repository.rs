use crate::{PostgresTransaction, SideEffectStorage};
use sqlx::{
    Pool, Postgres,
    postgres::{PgConnectOptions, PgPoolOptions},
};

/// PostgreSQL-based repository implementation for event sourcing.
///
/// This repository provides persistent storage for aggregates, events, and snapshots
/// using PostgreSQL as the backing store. It integrates with a configurable side effect
/// storage mechanism for handling the outbox pattern.
#[derive(Clone)]
pub struct PostgresRepository<O>
where
    O: SideEffectStorage + Clone,
{
    pub(crate) inner: Pool<Postgres>,
    pub(crate) outbox: O,
}

impl<O> PostgresRepository<O>
where
    O: SideEffectStorage + Clone,
{
    /// Creates a new PostgreSQL repository with the specified connection and pool options.
    ///
    /// # Parameters
    ///
    /// - `connect_options` - PostgreSQL connection configuration
    /// - `pool_options` - Connection pool configuration  
    /// - `outbox` - Side effect storage implementation for the outbox pattern
    pub async fn new(
        connect_options: PgConnectOptions,
        pool_options: PgPoolOptions,
        outbox: O,
    ) -> Result<Self, sqlx::Error> {
        let pool = pool_options.connect_with(connect_options).await?;

        Ok(Self {
            inner: pool,
            outbox,
        })
    }

    /// Start a new database transaction using the default isolation level.
    ///
    /// The returned transaction can be used to perform multiple operations
    /// atomically and provides access to the repository methods.
    pub async fn begin_transaction(&self) -> Result<PostgresTransaction<'_, O>, sqlx::Error> {
        Ok(PostgresTransaction {
            inner: self.inner.begin().await?,
            outbox: &self.outbox,
        })
    }

    /// Run database migrations to set up the required tables and schema.
    ///
    /// This method should be called once during application startup to ensure
    /// the database schema is up to date with the required tables for events,
    /// snapshots, and outbox storage.
    pub async fn run_migrations(&self) -> Result<(), sqlx::Error> {
        sqlx::migrate!("./migrations").run(&self.inner).await?;

        Ok(())
    }
}
